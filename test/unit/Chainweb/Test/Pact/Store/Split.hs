{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.Test.Pact.Store.Split
--
-- Tests for split routing (storage redesign P3, spec 4.3 "Reads" and 5).
--
-- The fixture is the freezer's: one history committed into a SQLite file that
-- the freezer then freezes and trims (depth 2, run to 'NothingToDo', so the
-- verified frontier is height 8, end txid 90), and into an untouched legacy
-- twin. Every routed read of the split store must equal the twin's.
--
module Chainweb.Test.Pact.Store.Split
( tests
) where

import Control.Exception
import Control.Lens ((&), (.~))
import Control.Monad

import qualified Data.ByteString as B
import Data.IORef
import Data.List (sort, sortOn)
import Data.Ord (Down(..))
import Data.Text (Text)
import qualified Data.Text as T

import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)

import Test.Tasty
import Test.Tasty.HUnit

import qualified Pact.Types.Persistence as Pact4

-- internal modules

import Chainweb.BlockHeader.Internal
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Freezer
import Chainweb.Pact.Backend.Store.Layout (PactLayoutError(..), sqliteDirName)
import Chainweb.Pact.Backend.Store.Config (PactHistoryStore(..))
import Chainweb.Pact.Backend.Store.SQLite
import Chainweb.Pact.Backend.Store.Split
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils (chainDbFileName, chainwebPragmas, withSQLiteConnection, withTempSQLiteConnection)
import qualified Pact.Types.SQLite as P4SQL
import Chainweb.Storage.Table.RocksDB (writeBatchSync)
import Chainweb.Test.Pact.Store.Freezer
    ( cid, mkHash, block, commitAll, dataTables, withFixture, withHistoryFixture, history, Node(..), Crash(..)
    , testConfig, withNode, withNodeW, withPlainNode, runToEnd, finalFrontier, frontierAt
    , dumpTables, bookkeeping, masterOf
    )
import Chainweb.Test.TestVersions (slowForkingCpmTestVersion)
import Chainweb.Test.Utils (petersen)

type TxId = Pact4.TxId

tests :: TestTree
tests = testGroup "Split store"
    [ testCase "legacy query plans" legacyPlansTest
    , testCase "every read op equals the legacy twin: all tables, all keys (plus absent), bounds {none, 0, every txid, F-1, F, F+1, max+1}" everyReadTest
    , testCase "distinctKeys KeysAsStored and KeysOrderedByKey equal the legacy twin, order included" keysTest
    , testCase "a table named in another ASCII case reads the same rows below F" caseTest
    , testCase "missing table and a table created above F: Left identical to legacy; no cold read counted" missingTableTest
    , testCase "rewind to a parent above the written frontier: reads equal the rewound legacy twin" rewindAboveTest
    , testCase "rewind below the written frontier, also after a crash between write and publish: RewindBelowFrontier, SQLite unchanged" rewindBelowTest
    , testCase "SYS:Pacts: a continuation read whose row is only in cold returns its bytes; counters move" sysPactsTest
    , testCase "a publish between the split's frontier read and the cold read: no duplicate rows, order as legacy" straddleTest
    , testCase "a freezer step and its trim between the hot statement and the frontier read: every read op equals the legacy twin" trimRaceTest
    , testCase "a trim cut short at one frontier resumes, and the next step's trim removes stale anchors, inside a read's hot statement: every read op equals the legacy twin (n2)" trimResumeRaceTest
    , testCase "backup is refused" backupTest
    , testCase "a frozen-name table missing from the cold catalog with rows below F: every read op equals the legacy twin" uncataloguedTest
    , testCase "a compacted base whose table is first changed after the first step: every read op below F equals the legacy twin" compactedLateTableTest
    ]

-- -------------------------------------------------------------------------- --
-- Fixture

data Split = Split
    { _sStore :: PactStore
    , _sLegacy :: PactStore
    , _sCounters :: IORef SplitCounters
    , _sNode :: Node
    , _sTwin :: SQLiteEnv
    }

-- | The frozen fixture: a full freezer run, then the split store over the
-- trimmed SQLite file and the twin.
withSplit :: (Split -> IO a) -> IO a
withSplit k = withFixture $ \root twin -> withPlainNode root $ \n -> do
    void $ runToEnd (_nEnv n)
    _ccVerified (_nCold n) >>= (@?= Just finalFrontier)
    splitOf n twin k

splitOf :: Node -> SQLiteEnv -> (Split -> IO a) -> IO a
splitOf n twin k = do
    ref <- newIORef emptySplitCounters
    let env = SplitEnv
            { _seChain = cid
            , _seHot = sqlitePactStore (_nPact n)
            , _seCold = _nCold n
            , _seCounters = ref
            }
    k (Split (splitPactStore env) (sqlitePactStore twin) ref n twin)

-- | F of the fixture.
f0 :: TxId
f0 = _frEndTxId finalFrontier

-- | Every txid of the history, and one past the end: 0 to 130 holds
-- F - 1, F and F + 1. The tip block 12 ends at 130.
bounds :: [TxId]
bounds = [0 .. 131]

missingTable :: Text
missingTable = "no_such_table"

counters :: Split -> IO SplitCounters
counters = readIORef . _sCounters

coldReads :: Split -> IO Word
coldReads s = fromIntegral . _scColdReads <$> counters s

-- | The keys of a table in the twin, plus an absent key.
keysOf :: Split -> Text -> IO [B.ByteString]
keysOf s t = _psDistinctKeys (_sLegacy s) (KeysAsStored Nothing) t >>= \case
    Left _ -> return ["absent"]
    Right ks -> return ("absent" : ks)

same :: (Eq a, Show a) => String -> IO a -> IO a -> Assertion
same what split legacy = do
    a <- split
    b <- legacy
    assertEqual what b a

-- | Within one txid the cold part of a @SYS:*@ table comes in key order, not
-- rowid order (no by-txid lists); see 'rowsInTxIdRange' in
-- "Chainweb.Pact.Backend.Store.Split".
normaliseSysRange :: Text -> Either e [(TxId, B.ByteString, B.ByteString)] -> Either e [(TxId, B.ByteString, B.ByteString)]
normaliseSysRange t
    | isSysTable t = fmap (sortOn (\(x, k, _) -> (Down x, k)))
    | otherwise = id

rangePoints :: [TxId]
rangePoints = [0, 1, 5, 10, 20, 45, 50, 80, 89, 90, 91, 100, 110, 129, 130, 131]

-- | Every routed read of a table, split against legacy, with the table
-- named @q@ in the split store and @t@ in the twin.
compareTable :: Split -> [TxId] -> Text -> Text -> Assertion
compareTable s bs q t = do
    let sp = _sStore s
        lg = _sLegacy s
        at what = T.unpack q <> " " <> what
    ks <- keysOf s t
    forM_ ks $ \k -> do
        same (at ("readNewest " <> show k <> " none")) (_psReadNewest sp q k Nothing) (_psReadNewest lg t k Nothing)
        forM_ bs $ \b -> do
            let w = show k <> " " <> show b
            same (at ("readNewest " <> w)) (_psReadNewest sp q k (Just b)) (_psReadNewest lg t k (Just b))
            same (at ("readPrev " <> w)) (_psReadPrev sp q k b) (_psReadPrev lg t k b)
            same (at ("readHistorical " <> w)) (_psReadHistorical sp q k b) (_psReadHistorical lg t k b)
    compareKeys s bs q t
    unless (isSysTable t) $ forM_ bs $ \x -> do
        same (at ("txIdsAfter " <> show x)) (_psTxIdsAfter sp q x) (_psTxIdsAfter lg t x)
        same (at ("rowsAtTxId " <> show x)) (_psRowsAtTxId sp q x) (_psRowsAtTxId lg t x)
    forM_ rangePoints $ \from -> forM_ rangePoints $ \to ->
        same (at ("rowsInTxIdRange " <> show (from, to)))
            (normaliseSysRange t <$> _psRowsInTxIdRange sp q from to)
            (normaliseSysRange t <$> _psRowsInTxIdRange lg t from to)

compareKeys :: Split -> [TxId] -> Text -> Text -> Assertion
compareKeys s bs q t = do
    let sp = _sStore s
        lg = _sLegacy s
        at what = T.unpack q <> " " <> what
    same (at "keys as stored, none") (_psDistinctKeys sp (KeysAsStored Nothing) q) (_psDistinctKeys lg (KeysAsStored Nothing) t)
    forM_ bs $ \b -> do
        same (at ("keys as stored " <> show b)) (_psDistinctKeys sp (KeysAsStored (Just b)) q) (_psDistinctKeys lg (KeysAsStored (Just b)) t)
        same (at ("keys by key " <> show b)) (_psDistinctKeys sp (KeysOrderedByKey b) q) (_psDistinctKeys lg (KeysOrderedByKey b) t)

-- -------------------------------------------------------------------------- --
-- Tests

-- | Step 1: the plans of the legacy statements whose order the split answer
-- reproduces, on a table made by '_psCommitBlock' with 3.2.1's schema.
legacyPlansTest :: Assertion
legacyPlansTest = withFixture $ \_ twin -> do
    let plan r = sqliteExplainLegacyRead twin r "coin_table"
        autoindex = "SCAN coin_table USING COVERING INDEX sqlite_autoindex_coin_table_1"
    unbounded <- plan (LegacyKeys (KeysAsStored Nothing))
    bounded <- plan (LegacyKeys (KeysAsStored (Just 5)))
    forM_ (unbounded ++ bounded) $ \d -> do
        assertBool ("KeysAsStored uses the txid index: " <> T.unpack d) (not ("coin_table_ix" `T.isInfixOf` d))
        assertBool ("KeysAsStored uses a temporary B-tree: " <> T.unpack d) (not ("TEMP B-TREE" `T.isInfixOf` d))
    unbounded @?= [autoindex]
    bounded @?= [autoindex]
    plan (LegacyKeys (KeysOrderedByKey 5)) >>= (@?= [autoindex])
    plan LegacyTxIdsAfter >>= (@?= ["SEARCH coin_table USING COVERING INDEX coin_table_ix (txid>?)"])
    plan LegacyRowsAtTxId >>= (@?= ["SEARCH coin_table USING INDEX coin_table_ix (txid=?)"])
    -- the orders these plans give
    let lg = sqlitePactStore twin
    Right ks <- _psDistinctKeys lg (KeysAsStored Nothing) "coin_table"
    ks @?= sort ks
    Right xs <- _psTxIdsAfter lg "coin_table" 0
    xs @?= sortOn Down xs

everyReadTest :: Assertion
everyReadTest = withSplit $ \s -> do
    forM_ (dataTables ++ [missingTable]) $ \t -> compareTable s bounds t t
    n <- coldReads s
    assertBool "the cold store was read" (n > 0)

keysTest :: Assertion
keysTest = withSplit $ \s -> do
    forM_ dataTables $ \t -> compareKeys s bounds t t
    -- the trim left the hot store without the keys below F that only cold
    -- has, so equality needs the cold part
    Right hot <- _psDistinctKeys (sqlitePactStore (_nPact (_sNode s))) (KeysOrderedByKey 30) "SYS:Pacts"
    Right lg <- _psDistinctKeys (_sLegacy s) (KeysOrderedByKey 30) "SYS:Pacts"
    assertBool "SYS:Pacts keys below F are only in cold" (hot /= lg)

caseTest :: Assertion
caseTest = withSplit $ \s -> do
    let below = [0 .. f0 - 1]
    forM_ [("late_table", "Late_Table"), ("LATE_TABLE", "Late_Table"), ("COIN_TABLE", "coin_table")] $ \(q, t) -> do
        n0 <- coldReads s
        compareTable s below q t
        n1 <- coldReads s
        assertBool ("cold was read for " <> T.unpack q) (n1 > n0)

missingTableTest :: Assertion
missingTableTest = withSplit $ \s -> do
    let hotStore = sqlitePactStore (_nPact (_sNode s))
    -- a table created above F, in both stores
    let high = (block 13)
            { _bcNewTables = ["High_Table"]
            , _bcRows = [("High_Table", [("h", 131, "{\"h\":1}"), ("h", 133, "{\"h\":3}"), ("i", 133, "{\"i\":3}")])]
            , _bcEndTxId = 140
            }
    _psCommitBlock hotStore high >>= either (assertFailure . show) return
    commitAll (_sTwin s) [high]
    r <- _psReadNewest (_sStore s) missingTable "k" (Just 5)
    case r of
        Left _ -> return ()
        Right v -> assertFailure $ "expected the missing table error, got " <> show v
    compareTable s (bounds ++ [132 .. 141]) missingTable missingTable
    compareTable s (bounds ++ [132 .. 141]) "High_Table" "High_Table"
    coldReads s >>= (@?= 0)

-- | A header with the fixture's height and hash, for '_psRewindDbTo'.
parentAt :: Int -> ParentHeader
parentAt h = ParentHeader $ genesisBlockHeader (slowForkingCpmTestVersion petersen) cid
    & blockHash .~ mkHash h 0
    & blockHeight .~ fromIntegral h

rewindAboveTest :: Assertion
rewindAboveTest = withSplit $ \s -> do
    forM_ [10, 8] $ \h -> do
        a <- _psRewindDbTo (_sStore s) (Just (parentAt h))
        b <- _psRewindDbTo (_sLegacy s) (Just (parentAt h))
        a @?= b
        forM_ (dataTables ++ [missingTable]) $ \t -> compareTable s bounds t t

-- | The SQLite state of the node: every data table, the bookkeeping tables
-- and the schema.
sqliteState :: SQLiteEnv -> IO ([(Text, [[Text]])], [[Text]])
sqliteState db = (,) <$> dumpTables db (dataTables ++ bookkeeping) <*> masterOf db

refused :: Split -> Maybe ParentHeader -> TxId -> Frontier -> Assertion
refused s p target f = do
    s0 <- sqliteState (_nPact (_sNode s))
    try (_psRewindDbTo (_sStore s) p) >>= \case
        Left e -> do
            e @?= RewindBelowFrontier cid target f
            let msg = displayException e
            forM_ ["chain 0", "end txid " <> show target, "height " <> show (_frHeight f), "whole snapshot", "replay", "storage-export"] $ \w ->
                assertBool ("message names " <> w <> ": " <> msg) (T.pack w `T.isInfixOf` T.pack msg)
        Right x -> assertFailure $ "expected RewindBelowFrontier, got " <> show x
    s1 <- sqliteState (_nPact (_sNode s))
    assertBool "SQLite unchanged" (s0 == s1)

rewindBelowTest :: Assertion
rewindBelowTest = do
    withSplit $ \s -> do
        refused s (Just (parentAt 5)) 60 finalFrontier
        refused s (Just (parentAt 7)) 80 finalFrontier
        refused s Nothing 0 finalFrontier
    -- a crash after the first step's write, before its publish: written
    -- frontier at height 2, nothing verified
    withFixture $ \root twin -> do
        let hooks _ = noHooks { _fhAfterWrite = throwIO (Crash 1) }
        r <- try $ withNode root testConfig writeBatchSync hooks (const (return ())) $ runToEnd . _nEnv
        case r of
            Left (Crash _) -> return ()
            Right o -> assertFailure $ "expected the crash, got " <> show o
        withPlainNode root $ \n -> do
            _ccVerified (_nCold n) >>= (@?= Nothing)
            _ccWritten (_nCold n) >>= (@?= Just (frontierAt 2))
            splitOf n twin $ \s -> do
                refused s (Just (parentAt 1)) 20 (frontierAt 2)
                refused s Nothing 0 (frontierAt 2)

sysPactsTest :: Assertion
sysPactsTest = withSplit $ \s -> do
    let hotStore = sqlitePactStore (_nPact (_sNode s))
        pacts = "SYS:Pacts"
    -- p3 starts at height 3 and continues at height 4: both below F
    _psReadNewest hotStore pacts "p3" Nothing >>= (@?= Right Nothing)
    _psReadNewest (_sStore s) pacts "p3" Nothing >>= (@?= Right (Just "{\"cont\":4}"))
    c1 <- counters s
    (_scColdReads c1, _scSysPactsColdReads c1, _scSysPactsColdHits c1, _scSysPactsHotHits c1) @?= (1, 1, 1, 0)
    assertBool "max <= total" (_scSysPactsColdMaxMicros c1 <= _scSysPactsColdMicros c1)
    -- the Pact 5 read, bounded
    _psReadNewest (_sStore s) pacts "p3" (Just 44) >>= (@?= Right (Just "{\"start\":3}"))
    -- p11 is hot
    _psReadNewest (_sStore s) pacts "p11" Nothing >>= (@?= Right (Just "{\"cont\":12}"))
    -- a pact id that never existed: a cold read, no hit
    _psReadNewest (_sStore s) pacts "p-none" Nothing >>= (@?= Right Nothing)
    c2 <- counters s
    (_scColdReads c2, _scSysPactsColdReads c2, _scSysPactsColdHits c2, _scSysPactsHotHits c2) @?= (3, 3, 2, 1)
    -- the keys of SYS:Pacts below F are a cold read of SYS:Pacts too
    Right ks <- _psDistinctKeys (_sStore s) (KeysOrderedByKey 30) pacts
    assertBool "keys below F come from cold" (not (null ks))
    c3 <- counters s
    (_scColdReads c3, _scSysPactsColdReads c3, _scSysPactsColdHits c3) @?= (4, 4, 3)

-- | A freezer publish lands between the split's read of F and the cold
-- read: the split store sees the older frontier (height 4, end txid 50) while
-- the cold readers already see the newer one (end txid 90). The hot store is
-- the untrimmed twin, as it is before the trim (which waits for the lock the
-- request holds). Every read must still equal the twin, in particular
-- 'rowsInTxIdRange': no row of [50, 90) twice, and txid descending.
straddleTest :: Assertion
straddleTest = withSplit $ \s0 -> do
    let older = frontierAt 4
        cold' = (_nCold (_sNode s0)) { _ccVerified = return (Just older) }
    ref <- newIORef emptySplitCounters
    let env = SplitEnv
            { _seChain = cid
            , _seHot = _sLegacy s0
            , _seCold = cold'
            , _seCounters = ref
            }
        s = s0 { _sStore = splitPactStore env, _sCounters = ref }
    _ccVerified (_nCold (_sNode s0)) >>= (@?= Just finalFrontier)
    forM_ (dataTables ++ [missingTable]) $ \t -> compareTable s bounds t t
    -- the straddling window explicitly: [50, 90) is in both parts
    forM_ dataTables $ \t -> do
        Right rs <- _psRowsInTxIdRange (_sStore s) t 0 131
        let xs = [ x | (x, _, _) <- rs ]
        assertBool ("txid descending, " <> T.unpack t) (xs == sortOn Down xs)
        Right lg <- _psRowsInTxIdRange (_sLegacy s) t 0 131
        length rs @?= length lg
    n <- coldReads s
    assertBool "the cold store was read" (n > 0)

-- | Where the probes' freezer step runs, inside the hot statement wrapper.
--
-- * 'StepAfterHot' is the race of the current order: the hot rows come from
--   the state before the step, and the frontier read after the hot statement
--   is the one after it.
-- * 'SteadyStepBeforeHot' is a steady-state check for the current order: the
--   step has finished before the hot statement and the frontier read. It is
--   also the case that catches a frontier read before the hot statement (the
--   order before 0e6549c2a): that read sees the old frontier, the step then
--   trims, and the hot statement misses the trimmed rows. A mutation check
--   (2026-11-25, Task 27 fix round 1) put that order back in @keyRead@: n2
--   failed in 'SteadyStepBeforeHot' (readNewest, readPrev, readHistorical of
--   bob and SYS:Pacts at 65 answered Nothing) and passed in 'StepAfterHot'.
data StepAt = SteadyStepBeforeHot | StepAfterHot
    deriving Show

-- | Reads outside @cpModuleCacheVar@ (@lookupHistorical@, @getBlockHistory@)
-- interleave with the trim. Each probe runs on a fresh fixture frozen to
-- end txid 30 (height 2). A whole freezer step (write, verify, publish and
-- the trim of that step, to end txid 60) then runs inside the probe's hot
-- statement wrapper: just before the statement, or just after it and so
-- before the split store's routing decision. The trim deletes rows in
-- @[30, 60)@ which the probes ask about. Every probe must equal the twin.
trimRaceTest :: Assertion
trimRaceTest = do
    bad <- fmap concat $ forM [(w, p) | w <- [SteadyStepBeforeHot, StepAfterHot], p <- probes] $ \(when', (what0, op)) -> withFixture $ \root twin -> withPlainNode root $ \n -> do
        let what = show when' <> ": " <> what0
            first = frontierAt 2
            next = frontierAt 5
            expectFroze w o = case o of
                Froze f' -> assertEqual (what <> ": step end txid") (_frEndTxId w) (_frEndTxId f')
                _ -> assertFailure (what <> ": expected a step to " <> show w)
        freezeOnce (_nEnv n) >>= expectFroze first
        armed <- newIORef True
        let fire = do
                a <- atomicModifyIORef' armed (\x -> (False, x))
                when a $ freezeOnce (_nEnv n) >>= expectFroze next
            thenStep act = case when' of
                SteadyStepBeforeHot -> fire *> act
                StepAfterHot -> act <* fire
            hot = sqlitePactStore (_nPact n)
            hot' = hot
                { _psReadNewest = \t k mb -> thenStep (_psReadNewest hot t k mb)
                , _psReadPrev = \t k b -> thenStep (_psReadPrev hot t k b)
                , _psReadHistorical = \t k b -> thenStep (_psReadHistorical hot t k b)
                , _psDistinctKeys = \q t -> thenStep (_psDistinctKeys hot q t)
                , _psTxIdsAfter = \t x -> thenStep (_psTxIdsAfter hot t x)
                , _psRowsAtTxId = \t x -> thenStep (_psRowsAtTxId hot t x)
                , _psRowsInTxIdRange = \t a b -> thenStep (_psRowsInTxIdRange hot t a b)
                }
        ref <- newIORef emptySplitCounters
        let sp = splitPactStore SplitEnv
                { _seChain = cid
                , _seHot = hot'
                , _seCold = _nCold n
                , _seCounters = ref
                }
        got <- op sp
        want <- op (sqlitePactStore twin)
        readIORef armed >>= assertBool (what <> ": the step ran inside the hot statement") . not
        _ccVerified (_nCold n) >>= assertEqual (what <> ": verified frontier") (Just (_frEndTxId next)) . fmap _frEndTxId
        -- the trim did delete rows of the window from the hot store
        _psRowsAtTxId hot "coin_table" 43 >>= assertEqual (what <> ": alice@43 trimmed") (Right [])
        return [ what <> "\n  expected: " <> want <> "\n   but got: " <> got | got /= want ]
    assertBool (unlines bad) (null bad)
  where
    coin = "coin_table"
    pacts = "SYS:Pacts"
    sh :: Show a => IO a -> IO String
    sh = fmap show
    probes :: [(String, PactStore -> IO String)]
    probes =
        -- bob is written at 21 and 41: the step trims 21, keeps the anchor 41
        [ ("readHistorical bob 35", \p -> sh (_psReadHistorical p coin "bob" 35))
        , ("readNewest bob 35", \p -> sh (_psReadNewest p coin "bob" (Just 35)))
        , ("readPrev bob 35", \p -> sh (_psReadPrev p coin "bob" 35))
        -- p2 starts at 24 and continues at 34: nothing of it stays hot
        , ("readNewest SYS:Pacts p2", \p -> sh (_psReadNewest p pacts "p2" Nothing))
        , ("readNewest SYS:Pacts p3 45", \p -> sh (_psReadNewest p pacts "p3" (Just 45)))
        , ("keys SYS:Pacts as stored", \p -> sh (_psDistinctKeys p (KeysAsStored Nothing) pacts))
        , ("keys SYS:Pacts by key 45", \p -> sh (_psDistinctKeys p (KeysOrderedByKey 45) pacts))
        , ("keys coin_table by key 35", \p -> sh (_psDistinctKeys p (KeysOrderedByKey 35) coin))
        , ("txIdsAfter coin_table 35", \p -> sh (_psTxIdsAfter p coin 35))
        , ("txIdsAfter coin_table 0", \p -> sh (_psTxIdsAfter p coin 0))
        , ("rowsAtTxId coin_table 43", \p -> sh (_psRowsAtTxId p coin 43))
        , ("rowsInTxIdRange coin_table 0 131", \p -> sh (_psRowsInTxIdRange p coin 0 131))
        , ("rowsInTxIdRange coin_table 35 55", \p -> sh (_psRowsInTxIdRange p coin 35 55))
        , ("rowsInTxIdRange SYS:Pacts 0 131", \p -> sh (normaliseSysRange pacts <$> _psRowsInTxIdRange p pacts 0 131))
        ]

-- | n2, the two-step race: the step to height 5 publishes and its trim stops
-- after one chunk (a crash). Inside a probe's hot statement (just before or
-- just after it), the freezer then resumes that trim, steps to height 8 and
-- trims again, which removes anchors that were current at height 5 (bob at
-- 41, replaced by 81 below the new frontier). Every probe must equal the
-- twin.
trimResumeRaceTest :: Assertion
trimResumeRaceTest = do
    bad <- fmap concat $ forM [(w, p) | w <- [SteadyStepBeforeHot, StepAfterHot], p <- probes] $ \(when', (what0, op)) -> withFixture $ \root twin -> do
        crash <- newIORef False
        let what = show when' <> ": " <> what0
            hooks _ = noHooks
                { _fhAfterTrimChunk = do
                    c <- atomicModifyIORef' crash (\x -> (False, x))
                    when c $ throwIO (Crash 0)
                }
        withNodeW (TrimWindow 1 1 1) root testConfig writeBatchSync hooks (const (return ())) $ \n -> do
            freezeOnce (_nEnv n) >>= \case
                Froze f -> assertEqual (what <> ": first step") (frontierAt 2) f
                o -> assertFailure (what <> ": expected the first step, got " <> show o)
            writeIORef crash True
            try (freezeOnce (_nEnv n)) >>= \case
                Left (Crash _) -> return ()
                Right o -> assertFailure (what <> ": expected the trim to stop, got " <> show o)
            _ccVerified (_nCold n) >>= assertEqual (what <> ": the second step published") (Just (frontierAt 5))
            -- the anchor bob@41 is still hot before the resumed trims
            _psRowsAtTxId (sqlitePactStore (_nPact n)) coin 41 >>= assertBool (what <> ": bob@41 hot") . (/= Right [])
            armed <- newIORef True
            let fire = do
                    a <- atomicModifyIORef' armed (\x -> (False, x))
                    when a $ void $ runToEnd (_nEnv n)
                thenStep act = case when' of
                    SteadyStepBeforeHot -> fire *> act
                    StepAfterHot -> act <* fire
                hot = sqlitePactStore (_nPact n)
                hot' = hot
                    { _psReadNewest = \t k mb -> thenStep (_psReadNewest hot t k mb)
                    , _psReadPrev = \t k b -> thenStep (_psReadPrev hot t k b)
                    , _psReadHistorical = \t k b -> thenStep (_psReadHistorical hot t k b)
                    , _psDistinctKeys = \q t -> thenStep (_psDistinctKeys hot q t)
                    , _psTxIdsAfter = \t x -> thenStep (_psTxIdsAfter hot t x)
                    , _psRowsAtTxId = \t x -> thenStep (_psRowsAtTxId hot t x)
                    , _psRowsInTxIdRange = \t a b -> thenStep (_psRowsInTxIdRange hot t a b)
                    }
            ref <- newIORef emptySplitCounters
            let sp = splitPactStore SplitEnv
                    { _seChain = cid
                    , _seHot = hot'
                    , _seCold = _nCold n
                    , _seCounters = ref
                    }
            got <- op sp
            want <- op (sqlitePactStore twin)
            readIORef armed >>= assertBool (what <> ": the steps ran inside the hot statement") . not
            _ccVerified (_nCold n) >>= assertEqual (what <> ": verified frontier") (Just finalFrontier)
            -- the stale anchor bob@41 and the rows of 61 left the hot store
            _psRowsAtTxId hot coin 41 >>= assertEqual (what <> ": txid 41 trimmed") (Right [])
            _psRowsAtTxId hot coin 61 >>= assertEqual (what <> ": txid 61 trimmed") (Right [])
            return [ what <> "\n  expected: " <> want <> "\n   but got: " <> got | got /= want ]
    assertBool (unlines bad) (null bad)
  where
    coin = "coin_table"
    pacts = "SYS:Pacts"
    sh :: Show a => IO a -> IO String
    sh = fmap show
    probes :: [(String, PactStore -> IO String)]
    probes =
        [ ("readHistorical bob 45", \p -> sh (_psReadHistorical p coin "bob" 45))
        , ("readHistorical bob 65", \p -> sh (_psReadHistorical p coin "bob" 65))
        , ("readNewest bob 45", \p -> sh (_psReadNewest p coin "bob" (Just 45)))
        , ("readNewest bob 65", \p -> sh (_psReadNewest p coin "bob" (Just 65)))
        , ("readPrev bob 65", \p -> sh (_psReadPrev p coin "bob" 65))
        , ("readNewest SYS:Pacts p5 65", \p -> sh (_psReadNewest p pacts "p5" (Just 65)))
        , ("keys coin_table by key 45", \p -> sh (_psDistinctKeys p (KeysOrderedByKey 45) coin))
        , ("txIdsAfter coin_table 35", \p -> sh (_psTxIdsAfter p coin 35))
        , ("rowsAtTxId coin_table 61", \p -> sh (_psRowsAtTxId p coin 61))
        , ("rowsInTxIdRange coin_table 35 75", \p -> sh (_psRowsInTxIdRange p coin 35 75))
        , ("rowsInTxIdRange coin_table 0 131", \p -> sh (_psRowsInTxIdRange p coin 0 131))
        ]

backupTest :: Assertion
backupTest = withSplit $ \s -> withSystemTempDirectory "split-backup" $ \tmp ->
    try (_psBackupTo (_sStore s) (tmp </> "backup.sqlite")) >>= \case
        Left e -> e @?= BackupRefused HistorySplit
        Right () -> assertFailure "expected BackupRefused"

-- | A table with rows below F that no @VersionedTableMutation@ row names is
-- never frozen or trimmed, so it is not in the cold catalog and SQLite holds
-- all its rows: every read must return SQLite's answer, below F too.
uncataloguedTest :: Assertion
uncataloguedTest = withSplit $ \s -> do
    let orphan = (block 13)
            { _bcNewTables = ["Orphan_Table"]
            , _bcRows = [("Orphan_Table", [("a", 15, "{\"a\":1}"), ("b", 15, "{\"b\":1}"), ("a", 42, "{\"a\":2}"), ("c", 135, "{\"c\":3}")])]
            , _bcEndTxId = 140
            }
    _psCommitBlock (sqlitePactStore (_nPact (_sNode s))) orphan >>= either (assertFailure . show) return
    commitAll (_sTwin s) [orphan]
    forM_ [_nPact (_sNode s), _sTwin s] $ \db ->
        P4SQL.exec' db "DELETE FROM VersionedTableMutation WHERE tablename = ?" [P4SQL.SText "Orphan_Table"]
    _ccTableId (_nCold (_sNode s)) "Orphan_Table" >>= (@?= Nothing)
    compareTable s (bounds ++ [132 .. 141]) "Orphan_Table" "Orphan_Table"
    compareKeys s (bounds ++ [132 .. 141]) "Orphan_Table" "Orphan_Table"

-- | A compacted base (P3a concern 1): the history starts at height 2, and
-- the compaction kept no @VersionedTableCreation@ row below it, so only the
-- SQLite schema names @Gap_Table@. Its rows at heights 0 and 1 are below the
-- first block; the table is next changed at height 6, in the second step.
-- The first step must catalog and freeze every table of the schema, or the
-- second step catalogs @Gap_Table@ with only its own rows, and reads below
-- F go to cold and miss the compacted rows.
compactedLateTableTest :: Assertion
compactedLateTableTest = withHistoryFixture hist $ \root -> withTempSQLiteConnection chainwebPragmas $ \twin -> do
    _psInitSchema (sqlitePactStore twin)
    commitAll twin hist
    withSQLiteConnection (root </> sqliteDirName </> chainDbFileName cid) chainwebPragmas $ \db ->
        forM_ [db, twin] compact
    withPlainNode root $ \n -> do
        outcomes <- runToEnd (_nEnv n)
        outcomes @?= [Froze (frontierAt 4), Froze (frontierAt 7), NothingToDo]
        splitOf n twin $ \s -> do
            forM_ ("Gap_Table" : dataTables) $ \t -> compareTable s bounds t t
            compareKeys s bounds "Gap_Table" "Gap_Table"
  where
    hist = gap <$> history
    gap b
        | h == 1 = b
            { _bcNewTables = _bcNewTables b ++ ["Gap_Table"]
            , _bcRows = _bcRows b ++ [("Gap_Table", [("g", 19, "{\"g\":1}"), ("h", 19, "{\"h\":1}")])]
            }
        | h == 6 = b
            { _bcRows = _bcRows b ++ [("Gap_Table", [("g", 69, "{\"g\":6}")])] }
        | otherwise = b
      where
        h = fromIntegral (_bcHeight b) :: Int
    compact db = do
        P4SQL.exec_ db "DELETE FROM BlockHistory WHERE blockheight < 2"
        P4SQL.exec_ db "DELETE FROM VersionedTableMutation WHERE blockheight < 2"
        P4SQL.exec_ db "DELETE FROM VersionedTableCreation WHERE createBlockheight < 2"
