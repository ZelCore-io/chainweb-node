{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.Test.Pact.Store.Freezer
--
-- Tests for the freezer step (storage redesign P3, spec 6.2 and 6.3): read,
-- write, verify, publish and trim, crash safety at every hook, the halts, and
-- the trim statements' query plans.
--
-- The fixture is a SQLite file written through '_psCommitBlock' with a
-- Pact 4-style history, and an untouched twin with the same history.
--
module Chainweb.Test.Pact.Store.Freezer
( tests

  -- * Fixture, shared with "Chainweb.Test.Pact.Store.Split"
, cid
, tipHeight
, mkHash
, block
, commitAll
, dataTables
, withFixture
, Node(..)
, Crash(..)
, quietLogger
, testConfig
, withNode
, withNodeW
, withPlainNode
, runToEnd
, finalFrontier
, frontierAt
, dumpTables
, bookkeeping
, masterOf
) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import Data.Bits (shiftL, (.|.))
import Data.Int (Int64)
import Data.IORef
import Data.List (sort, sortOn)
import qualified Data.Map.Strict as M
import Data.Maybe (isJust, isNothing)
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word

import qualified Database.RocksDB.Base as R
import qualified Database.SQLite3.Direct as SQ3

import System.Directory
import System.FilePath
import System.IO.Temp (withSystemTempDirectory)
import System.LogLevel

import Test.Tasty
import Test.Tasty.HUnit

import qualified Pact.Types.Persistence as Pact4
import qualified Pact.Types.SQLite as P4SQL

-- internal modules

import Chainweb.BlockHash
import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Cold.Key
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Pact.Backend.Store.Freezer
import Chainweb.Pact.Backend.Store.Layout
import Chainweb.Pact.Backend.Store.SQLite
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils (chainDbFileName, chainwebPragmas, withSQLiteConnection, withTempSQLiteConnection)
import Chainweb.Storage.Table.RocksDB (RocksDb, _rocksDbHandle, writeBatchSync)
import Chainweb.Utils.Serialization (runGetEitherS, runPutS)

-- -------------------------------------------------------------------------- --
-- History

cid :: ChainId
cid = unsafeChainId 0

tipHeight :: Int
tipHeight = 12

mkHash :: Int -> Word8 -> BlockHash
mkHash h salt = either error id $ runGetEitherS decodeBlockHash
    (B.replicate 31 (fromIntegral h) <> B.singleton salt)

-- | Block @h@ uses txids @[10h, 10h + 10)@.
block :: Int -> BlockCommit
block h = BlockCommit
    { _bcHeight = fromIntegral h
    , _bcHash = mkHash h 0
    , _bcEndTxId = tx 10
    , _bcNewTables = [ "coin_table" | h == 0 ] ++ [ "Late_Table" | h == 6 ]
    , _bcRows = filter (not . null . snd)
        [ ("coin_table", concat
            -- bob before alice at the same txid: rowid order is not key order
            [ [ ("bob", tx 1, v "bob") | even h ]
            -- several transactions write one key in a block (Pact 4)
            , [ ("alice", tx 1, v "a1"), ("alice", tx 2, v "a2"), ("alice", tx 3, v "a3") ]
            , [ ("k\0x", tx 2, v "nul") | h `mod` 3 == 1 ]
            , [ ("k\0", tx 5, v "nul2") | h == 2 || h == 9 ]
            , [ ("carol", tx 4, v "carol") | h == 1 ]
            ])
        , ("SYS:Pacts", [ ("p" <> n h, tx 4, v "start") ] ++ [ ("p" <> n (h - 1), tx 4, v "cont") | h > 0 ])
        , ("SYS:KeySets", [ ("ks", tx 6, v "ks") | h `mod` 3 == 0 ])
        , ("SYS:Modules", [ ("m", tx 7, v "mod") | h == 0 || h == 5 ])
        , ("Late_Table", concat
            [ [ ("x", tx 1, v "x1"), ("x", tx 8, v "x8"), ("y\0", tx 8, v "y") ] | h >= 6 ])
        ]
    , _bcTxHashes = [ "txhash-" <> n h ]
    }
  where
    tx :: Int -> Pact4.TxId
    tx i = fromIntegral (10 * h + i)
    v s = "{\"" <> s <> "\":" <> n h <> "}"
    n = B8.pack . show

commitAll :: SQLiteEnv -> [BlockCommit] -> IO ()
commitAll db bs = do
    let ps = sqlitePactStore db
    forM_ bs $ \b -> _psCommitBlock ps b >>= either (fail . show) return

history :: [BlockCommit]
history = block <$> [0 .. tipHeight]

-- | Tables the freezer moves to the cold store.
frozenNames :: [Text]
frozenNames = ["coin_table", "Late_Table", "SYS:Pacts", "SYS:KeySets"]

dataTables :: [Text]
dataTables = frozenNames ++ ["SYS:Modules", "SYS:ModuleSources", "SYS:Namespaces"]

-- -------------------------------------------------------------------------- --
-- Fixture

-- | A database root @<tmp>/0@ with the node's SQLite file under @sqlite@,
-- and the twin, an untouched copy of the same history.
withFixture :: (FilePath -> SQLiteEnv -> IO a) -> IO a
withFixture k = withSystemTempDirectory "freezer" $ \tmp -> do
    let root = tmp </> "0"
    createDirectoryIfMissing True (root </> sqliteDirName)
    withSQLiteConnection (root </> sqliteDirName </> chainDbFileName cid) chainwebPragmas $ \db -> do
        _psInitSchema (sqlitePactStore db)
        commitAll db history
    withTempSQLiteConnection chainwebPragmas $ \twin -> do
        _psInitSchema (sqlitePactStore twin)
        commitAll twin history
        k root twin

testConfig :: PactHistoryConfig
testConfig = defaultPactHistoryConfig
    { _phStore = HistorySplit
    , _phFreezeDepth = 2
    , _phFreezeStepHeights = 3
    }

data Node = Node
    { _nEnv :: FreezerEnv
    , _nStore :: ColdStore
    , _nCold :: ColdChain
    , _nPact :: SQLiteEnv
    }

quietLogger :: GenericLogger
quietLogger = genericLogger Error (\_ -> return ())

-- | A node start: resolve the layout, open the Pact connection, the cold
-- store and the chain, and build a freezer environment.
withNode
    :: FilePath
    -> PactHistoryConfig
    -> (RocksDb -> [R.BatchOp] -> IO ())
    -> (Node -> FreezerHooks)
    -> (Node -> IO ())
        -- ^ before each trim chunk
    -> (Node -> IO a)
    -> IO a
withNode = withNodeW defaultTrimWindow

withNodeW
    :: TrimWindow
    -> FilePath
    -> PactHistoryConfig
    -> (RocksDb -> [R.BatchOp] -> IO ())
    -> (Node -> FreezerHooks)
    -> (Node -> IO ())
    -> (Node -> IO a)
    -> IO a
withNodeW tw root cfg writer mkHooks gate k = do
    layout <- resolvePactLayout quietLogger HistorySplit False root
    dir <- currentSqliteDir layout
    withSQLiteConnection (dir </> chainDbFileName cid) chainwebPragmas $ \db -> do
        lock <- newMVar db
        withColdStore (root </> coldDirName) ColdReadWrite 8388608 $ \cs -> do
            cc <- openColdChainWith writer cs cid
            stats <- newIORef emptyFreezerStats
            let node = Node env cs cc db
                env = FreezerEnv
                    { _feChain = cid
                    , _feCold = cc
                    , _feLayout = layout
                    , _feConfig = cfg
                    , _feWithPactDb = \act -> withMVar lock act
                    , _feBeforeTrimChunk = gate node
                    , _feAfterTrimChunk = \_ -> return ()
                    , _fePollMicros = 50_000
                    , _feSplitCounters = Nothing
                    , _feCacheStats = coldCacheStats cs
                    , _feShadowDifferences = return Nothing
                    , _feTrimWindow = tw
                    , _feLogger = quietLogger
                    , _feHooks = mkHooks node
                    , _feStats = stats
                    }
            k node

-- | A node whose trim chunks are one txid wide, so that each table's trim of
-- a step spans many chunks.
withSmallChunkNode :: FilePath -> (Node -> FreezerHooks) -> (Node -> IO a) -> IO a
withSmallChunkNode root hooks = withNodeW (TrimWindow 1 1 1) root testConfig writeBatchSync hooks (const (return ()))

withSmallChunkNodeGate :: FilePath -> (Node -> IO ()) -> (Node -> IO a) -> IO a
withSmallChunkNodeGate root = withNodeW (TrimWindow 1 1 1) root testConfig writeBatchSync (const noHooks)

withPlainNode :: FilePath -> (Node -> IO a) -> IO a
withPlainNode root = withNode root testConfig writeBatchSync (const noHooks) (const (return ()))

-- | 'freezeOnce' until it has nothing to do; the outcomes in order.
runToEnd :: FreezerEnv -> IO [StepOutcome]
runToEnd env = freezeOnce env >>= \case
    NothingToDo -> return [NothingToDo]
    o@(Froze _) -> (o :) <$> runToEnd env
    o -> return [o]

frontierAt :: Int -> Frontier
frontierAt h = Frontier (fromIntegral h) (mkHash h 0) (fromIntegral (10 * h + 10))

-- | The frontier after a full run: tip 12, depth 2, steps of 3 heights.
finalFrontier :: Frontier
finalFrontier = frontierAt 8

data Crash = Crash Int
    deriving Show
instance Exception Crash

-- -------------------------------------------------------------------------- --
-- Reading state

type Row = (Int64, B.ByteString, Word64, B.ByteString)

rowsOf :: SQLiteEnv -> Text -> IO [Row]
rowsOf db t = do
    rs <- P4SQL.qry_ db ("SELECT rowid, rowkey, txid, rowdata FROM [" <> SQ3.Utf8 (T.encodeUtf8 t) <> "] ORDER BY rowid")
        [P4SQL.RInt, P4SQL.RText, P4SQL.RInt, P4SQL.RBlob]
    forM rs $ \case
        [P4SQL.SInt i, P4SQL.SText (SQ3.Utf8 k), P4SQL.SInt x, P4SQL.SBlob d] -> return (i, k, fromIntegral x, d)
        r -> fail (show r)

-- | Every table's rows in rowid order, every column quoted.
dumpTables :: SQLiteEnv -> [Text] -> IO [(Text, [[Text]])]
dumpTables db names = forM names $ \t -> do
    let tu = SQ3.Utf8 (T.encodeUtf8 t)
    cols <- (\rs -> [ c | [_, P4SQL.SText c] <- rs ]) <$> P4SQL.qry_ db ("PRAGMA table_info('" <> tu <> "')") [P4SQL.RInt, P4SQL.RText]
    rows <- P4SQL.qry_ db
        ("SELECT " <> mconcat (intercalate' ", " ("quote(rowid)" : fmap (\c -> "quote(" <> c <> ")") cols))
            <> " FROM [" <> tu <> "] ORDER BY rowid")
        (P4SQL.RText <$ ("rowid" : cols))
    return (t, fmap (fmap (\case P4SQL.SText (SQ3.Utf8 x) -> T.decodeUtf8 x; s -> T.pack (show s))) rows)
  where
    intercalate' sep = foldr (\x acc -> if null acc then [x] else x : sep : acc) []

bookkeeping :: [Text]
bookkeeping = ["BlockHistory", "VersionedTableCreation", "VersionedTableMutation", "TransactionIndex"]

masterOf :: SQLiteEnv -> IO [[Text]]
masterOf db = fmap (fmap (\case P4SQL.SText (SQ3.Utf8 x) -> T.decodeUtf8 x; s -> T.pack (show s)))
    <$> P4SQL.qry_ db "SELECT type, name, tbl_name, quote(sql) FROM sqlite_master ORDER BY name"
        [P4SQL.RText, P4SQL.RText, P4SQL.RText, P4SQL.RText]

-- | Every key and value of the cold store, without the stray key that the
-- shared open path writes.
coldEntries :: ColdStore -> IO [(B.ByteString, B.ByteString)]
coldEntries cs = do
    ks <- filter isKeyspace <$> coldRawKeys cs
    forM ks $ \k -> (,) k . maybe "" id <$> coldRawGet cs k
  where
    isKeyspace k = case B.uncons k of
        Just (c, _) -> c >= 0x41 && c <= 0x5A
        Nothing -> False

be32 :: B.ByteString -> Maybe Word32
be32 b
    | B.length b == 4 = Just $ B.foldl' (\a x -> a `shiftL` 8 .|. fromIntegral x) 0 b
    | otherwise = Nothing

coldTableId :: ColdStore -> Text -> IO (Maybe TableId)
coldTableId cs t = fmap TableId . (be32 =<<) <$> coldRawGet cs (tableIdKey cid (foldTableName t))

-- | The @R@ entries of the cold store, per table: (row key, txid, value).
coldRowsByTable :: ColdStore -> IO (M.Map Text [(B.ByteString, Word64, B.ByteString)])
coldRowsByTable cs = do
    ids <- fmap concat $ forM frozenNames $ \t -> maybe [] (\i -> [(i, t)]) <$> coldTableId cs t
    es <- coldEntries cs
    let rs = [ (t, (rk, x, val))
             | (k, val) <- es, B.take 1 k == "R"
             , Just (_, tid, rk, x) <- [decodeRowKey k]
             , Just t <- [lookup tid ids]
             ]
    return $ M.fromListWith (flip (++)) [ (t, [r]) | (t, r) <- rs ]

-- | The state a test compares: SQLite dumps and every cold entry.
snapshot :: FilePath -> IO ([(Text, [[Text]])], [[Text]], [(B.ByteString, B.ByteString)])
snapshot root = do
    layout <- resolvePactLayout quietLogger HistorySplit False root
    dir <- currentSqliteDir layout
    (d, m) <- withSqliteReadOnly (dir </> chainDbFileName cid) $ \db ->
        (,) <$> dumpTables db (dataTables ++ bookkeeping) <*> masterOf db
    c <- withColdStore (root </> coldDirName) ColdReadOnly 8388608 coldEntries
    return (d, m, c)

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = testGroup "Freezer step"
    [ testCase "before trim, every SQLite row below F has exactly one identical R entry" beforeTrimTest
    , testCase "after trim: per key, SQLite holds the rows >= F plus the newest < F; SYS:Pacts none < F" afterTrimRowsTest
    , testCase "after trim: VersionedTableCreation, VersionedTableMutation, BlockHistory, TransactionIndex and sqlite_master unchanged" afterTrimBookkeepingTest
    , testCase "after trim: every surviving row keeps its rowid; rowsAtTxId at and above F equals the twin's, order included" afterTrimOrderTest
    , testCase "crash after read, write, verify, publish and each trim chunk: resume reaches the no-crash state" crashResumeTest
    , testCase "at every crash point SQLite holds a newest-suffix of each key and every missing row is in cold below the verified frontier" crashInvariantTest
    , testCase "after every trim chunk (both trim rules): SQLite holds a newest-suffix of each key, so a SQLite hit is the legacy answer" perChunkStateTest
    , testCase "an exception mid-trim in a running process: the next call resumes the trim from the stored position" trimExceptionTest
    , testCase "a restart after a finished trim runs no trim chunk" noRetrimTest
    , testCase "a large step trims in ascending txid windows: one chunk per window, none re-walks an earlier one" largeStepWindowTest
    , testCase "corrupt R value between write and verify: HaltedVerify, verified frontier and SQLite unchanged" corruptTest
    , testCase "verify view: a step that creates a new table verifies; a corrupted value in that table fails verify" newTableVerifyTest
    , testCase "cold write failure: freezeOnce throws, frontiers and SQLite unchanged, SQLite still answers" writeFailureTest
    , testCase "crash between write and publish, then a rewind and another block at the written height: HaltedHash, nothing visible" crashRewindTest
    , testCase "BlockHistory hash changed at the frontier height: TrimHashMismatch, nothing deleted" hashMismatchTest
    , testCase "step ends at a block boundary at the byte cap; an oversized block is a step of its own" byteCapTest
    , testCase "EXPLAIN QUERY PLAN of both trim statements uses indexes, no full scan" explainTest
    , testCase "the first trim renames sqlite to sqlite-split and writes the marker before any DELETE" splitMarkerTest
    , testCase "a chain whose genesis is above the step heights freezes from its first block" lateGenesisTest
    , testCase "a compacted-style BlockHistory that starts at a high height freezes from its first row" compactedHistoryTest
    ]

-- | A fixture whose node SQLite file holds the given history, without a twin.
withHistoryFixture :: [BlockCommit] -> (FilePath -> IO a) -> IO a
withHistoryFixture hist k = withSystemTempDirectory "freezer" $ \tmp -> do
    let root = tmp </> "0"
    createDirectoryIfMissing True (root </> sqliteDirName)
    withSQLiteConnection (root </> sqliteDirName </> chainDbFileName cid) chainwebPragmas $ \db -> do
        _psInitSchema (sqlitePactStore db)
        commitAll db hist
    k root

-- | Genesis at height 1000 (mainnet chains 10-19 start at 852,054): the
-- first step must start at the first @BlockHistory@ row, not at height 0.
lateGenesisTest :: Assertion
lateGenesisTest = withHistoryFixture (shift <$> history) $ \root -> withPlainNode root $ \n -> do
    outcomes <- runToEnd (_nEnv n)
    outcomes @?= (Froze . at <$> [2, 5, 8]) ++ [NothingToDo]
    _ccVerified (_nCold n) >>= (@?= Just (at 8))
  where
    off = 1000
    shift b = b { _bcHeight = _bcHeight b + off }
    at h = (frontierAt h) { _frHeight = fromIntegral h + off }

-- | A compacted database keeps only the @BlockHistory@ and
-- @VersionedTableMutation@ rows from the compaction target up.
compactedHistoryTest :: Assertion
compactedHistoryTest = withHistoryFixture history $ \root -> do
    withSQLiteConnection (root </> sqliteDirName </> chainDbFileName cid) chainwebPragmas $ \db -> do
        P4SQL.exec_ db "DELETE FROM BlockHistory WHERE blockheight < 5"
        P4SQL.exec_ db "DELETE FROM VersionedTableMutation WHERE blockheight < 5"
    withPlainNode root $ \n -> do
        outcomes <- runToEnd (_nEnv n)
        outcomes @?= [Froze (frontierAt 7), NothingToDo]
        _ccVerified (_nCold n) >>= (@?= Just (frontierAt 7))
        -- the rows below the first block that no mutation entry of a planned
        -- height names are in the first step too: ks at heights 0, 3 and 6
        cold <- coldRowsByTable (_nStore n)
        sort [ x | (_, x, _) <- M.findWithDefault [] "SYS:KeySets" cold ] @?= [6, 36, 66]
        assertBool "coin_table rows of the compacted heights are cold" $
            any (\(_, x, _) -> x < 50) (M.findWithDefault [] "coin_table" cold)

-- | Every SQLite row of a frozen table with @txid < F@ has exactly one R
-- entry with the same bytes, and cold holds nothing else.
assertColdComplete :: String -> ColdStore -> SQLiteEnv -> Frontier -> IO ()
assertColdComplete msg cs db f = do
    cold <- coldRowsByTable cs
    forM_ frozenNames $ \t -> do
        sq <- rowsOf db t
        let expected = sort [ (k, x, d) | (_, k, x, d) <- sq, x < w64 (_frEndTxId f) ]
        assertEqual (msg <> ": " <> T.unpack t) expected (sort (M.findWithDefault [] t cold))

w64 :: Pact4.TxId -> Word64
w64 (Pact4.TxId x) = x

beforeTrimTest :: Assertion
beforeTrimTest = withFixture $ \root twin -> do
    -- the trim gate stops the first trim before any chunk
    r <- try $ withNode root testConfig writeBatchSync (const noHooks) (const (throwIO (Crash 0))) $ \n ->
        freezeOnce (_nEnv n)
    case r of
        Left (Crash _) -> return ()
        Right o -> assertFailure $ "expected the gate to stop the trim, got " <> show o
    withPlainNode root $ \n -> do
        _ccVerified (_nCold n) >>= (@?= Just (frontierAt 2))
        assertColdComplete "node SQLite, before trim" (_nStore n) (_nPact n) (frontierAt 2)
    -- after the full run, the same against the untouched twin
    withPlainNode root $ \n -> do
        void $ runToEnd (_nEnv n)
        _ccVerified (_nCold n) >>= (@?= Just finalFrontier)
        assertColdComplete "twin, after the full run" (_nStore n) twin finalFrontier

-- | The twin's rows that a trimmed file keeps.
expectedAfterTrim :: SQLiteEnv -> Frontier -> Text -> IO [Row]
expectedAfterTrim twin f t = do
    rs <- rowsOf twin t
    let fe = w64 (_frEndTxId f)
        newest = M.fromListWith max [ (k, x) | (_, k, x, _) <- rs, x < fe ]
        keep (_, k, x, _)
            | not (frozenTable t) = True
            | x >= fe = True
            | t == "SYS:Pacts" = False
            | otherwise = M.lookup k newest == Just x
    return (filter keep rs)

afterTrimRowsTest :: Assertion
afterTrimRowsTest = withFixture $ \root twin -> withPlainNode root $ \n -> do
    outcomes <- runToEnd (_nEnv n)
    outcomes @?= (Froze . frontierAt <$> [2, 5, 8]) ++ [NothingToDo]
    forM_ dataTables $ \t -> do
        expected <- expectedAfterTrim twin finalFrontier t
        actual <- rowsOf (_nPact n) t
        assertEqual (T.unpack t) expected actual
    -- the trim did delete something: coin_table had many alice versions
    total <- length <$> rowsOf (_nPact n) "coin_table"
    twinTotal <- length <$> rowsOf twin "coin_table"
    assertBool "rows were trimmed" (total < twinTotal)
    pacts <- rowsOf (_nPact n) "SYS:Pacts"
    assertBool "SYS:Pacts keeps no row below F" $ all (\(_, _, x, _) -> x >= 90) pacts

afterTrimBookkeepingTest :: Assertion
afterTrimBookkeepingTest = withFixture $ \root twin -> withPlainNode root $ \n -> do
    void $ runToEnd (_nEnv n)
    a <- dumpTables (_nPact n) bookkeeping
    b <- dumpTables twin bookkeeping
    a @?= b
    ma <- masterOf (_nPact n)
    mb <- masterOf twin
    ma @?= mb

afterTrimOrderTest :: Assertion
afterTrimOrderTest = withFixture $ \root twin -> withPlainNode root $ \n -> do
    void $ runToEnd (_nEnv n)
    forM_ dataTables $ \t -> do
        twinRows <- M.fromList . fmap (\r@(i, _, _, _) -> (i, r)) <$> rowsOf twin t
        rs <- rowsOf (_nPact n) t
        forM_ rs $ \r@(i, _, _, _) -> assertEqual ("rowid " <> show i <> " of " <> T.unpack t) (Just r) (M.lookup i twinRows)
    let ps = sqlitePactStore (_nPact n)
        pt = sqlitePactStore twin
        fe = fromIntegral (w64 (_frEndTxId finalFrontier)) :: Int
    forM_ ["coin_table", "Late_Table"] $ \t -> forM_ [fe .. 10 * tipHeight + 10] $ \x -> do
        a <- _psRowsAtTxId ps t (fromIntegral x)
        b <- _psRowsAtTxId pt t (fromIntegral x)
        assertEqual (T.unpack t <> " at txid " <> show x) b a

-- -------------------------------------------------------------------------- --
-- Crash tests

-- | Every hook and the trim gate count; the k-th throws 'Crash'.
countingHooks :: IORef Int -> Int -> (Node -> FreezerHooks, Node -> IO ())
countingHooks ref k = (const hooks, const tick)
  where
    tick = do
        i <- atomicModifyIORef' ref (\i -> (i + 1, i + 1))
        when (i == k) $ throwIO (Crash i)
    hooks = FreezerHooks tick tick tick tick tick

-- | The number of hook calls of a run without a crash, and its end state.
baseline :: IO (Int, ([(Text, [[Text]])], [[Text]], [(B.ByteString, B.ByteString)]))
baseline = withFixture $ \root _ -> do
    ref <- newIORef 0
    let (hooks, gate) = countingHooks ref 0
    void $ withCrashNode root hooks gate $ runToEnd . _nEnv
    (,) <$> readIORef ref <*> snapshot root

-- | Trim windows of 4 txids: each step's trim (30 txids) is 8 chunks, so the
-- crash points include the middle of every trim.
withCrashNode :: FilePath -> (Node -> FreezerHooks) -> (Node -> IO ()) -> (Node -> IO a) -> IO a
withCrashNode root = withNodeW (TrimWindow 4 4 4) root testConfig writeBatchSync

-- | Crashes at each hook call in turn; runs the check after the crash, then
-- resumes with a fresh start and compares with the no-crash state.
crashEach :: (Int -> FilePath -> SQLiteEnv -> IO ()) -> Assertion
crashEach check = do
    (n, ref) <- baseline
    assertBool ("a run has hook calls: " <> show n) (n > 20)
    forM_ [1 .. n] $ \k -> withFixture $ \root twin -> do
        calls <- newIORef 0
        let (hooks, gate) = countingHooks calls k
        r <- try $ withCrashNode root hooks gate $ runToEnd . _nEnv
        case r of
            Left (Crash i) -> i @?= k
            Right o -> assertFailure $ "crash point " <> show k <> " was not reached: " <> show o
        check k root twin
        outcomes <- withPlainNode root $ runToEnd . _nEnv
        assertEqual ("resume after crash " <> show k <> " ends") (Just NothingToDo) (lastMay outcomes)
        s <- snapshot root
        assertBool ("state after crash " <> show k <> " and resume equals the no-crash state") (s == ref)
  where
    lastMay [] = Nothing
    lastMay xs = Just (last xs)

crashResumeTest :: Assertion
crashResumeTest = crashEach (\_ _ _ -> return ())

crashInvariantTest :: Assertion
crashInvariantTest = crashEach $ \k root twin -> do
    layout <- resolvePactLayout quietLogger HistorySplit False root
    dir <- currentSqliteDir layout
    withSqliteReadOnly (dir </> chainDbFileName cid) $ \db ->
        withColdStore (root </> coldDirName) ColdReadOnly 8388608 $ \cs -> do
            meta <- coldRawGet cs (metaKey cid) >>= \case
                Nothing -> return emptyColdMeta
                Just b -> either fail return (decodeColdMeta b)
            checkTrimState ("crash " <> show k) db twin cs (_cmVerified meta)

-- | The state reads need at any moment of a trim (spec 4.3, 6.3), checked
-- against the untouched twin:
--
-- * per key of every frozen table, SQLite's rows are a newest-suffix of the
--   twin's (same rowids and bytes): the trim deletes oldest-first, so a SQLite
--   hit at any bound, the tip included, is the legacy answer, and a key with
--   no SQLite row left is wholly below F;
-- * every row missing from SQLite is in cold, with the same bytes, below the
--   VERIFIED frontier;
-- * the tables the freezer does not move equal the twin's.
--
checkTrimState :: String -> SQLiteEnv -> SQLiteEnv -> ColdStore -> Maybe Frontier -> IO ()
checkTrimState msg db twin cs verified = do
    let ve = w64 (frontierTxId verified)
    forM_ frozenNames $ \t -> do
        hot <- rowsOf db t
        tw <- rowsOf twin t
        tid <- coldTableId cs t
        let byKey rs = M.map (sortOn (\(_, _, x, _) -> x)) $
                M.fromListWith (flip (++)) [ (k, [r]) | r@(_, k, _, _) <- rs ]
            hk = byKey hot
            tk = byKey tw
            ctx = msg <> ": " <> T.unpack t
        assertBool (ctx <> ": SQLite has no key the twin lacks") (M.keysSet hk `S.isSubsetOf` M.keysSet tk)
        forM_ (M.toList tk) $ \(k, trs) -> do
            let hrs = M.findWithDefault [] k hk
                (gone, kept) = splitAt (length trs - length hrs) trs
            assertEqual (ctx <> ", key " <> show k <> ": SQLite's rows are a newest-suffix of the twin's") kept hrs
            unless (null hrs) $
                assertEqual (ctx <> ", key " <> show k <> ": SQLite's newest row is the legacy tip answer") (last trs) (last hrs)
            forM_ gone $ \r@(_, _, x, d) -> do
                inCold <- case tid of
                    Just t' | x < ve -> (== Just d) <$> coldRawGet cs (rowKey cid t' k x)
                    _ -> return False
                unless inCold $ assertFailure $ ctx <> ": row " <> show r
                    <> " is neither in SQLite nor in cold below the verified frontier"
    forM_ (filter (not . frozenTable) dataTables) $ \t -> do
        a <- rowsOf db t
        b <- rowsOf twin t
        assertEqual (msg <> ": " <> T.unpack t <> " is not trimmed") b a

-- | Small trim chunks, so that each table's trim of a step spans many of
-- them; after every chunk the state satisfies 'checkTrimState'. The fixture
-- has keys with several versions per step in a 'KeepAnchors' table
-- (@coin_table@, @Late_Table@) and pacts with a start and a continuation in
-- the 'DropAll' table @SYS:Pacts@.
perChunkStateTest :: Assertion
perChunkStateTest = withFixture $ \root twin -> do
    chunks <- newIORef (0 :: Int)
    let hooks n = noHooks
            { _fhAfterTrimChunk = do
                i <- atomicModifyIORef' chunks (\i -> (i + 1, i + 1))
                v <- _ccVerified (_nCold n)
                checkTrimState ("after trim chunk " <> show i) (_nPact n) twin (_nStore n) v
            }
    withSmallChunkNode root hooks $ \n -> do
        outcomes <- runToEnd (_nEnv n)
        outcomes @?= (Froze . frontierAt <$> [2, 5, 8]) ++ [NothingToDo]
        forM_ dataTables $ \t -> do
            expected <- expectedAfterTrim twin finalFrontier t
            rowsOf (_nPact n) t >>= assertEqual (T.unpack t) expected
    c <- readIORef chunks
    assertBool ("the trims spanned many chunks: " <> show c) (c > 30)

-- | An exception in the middle of the last step's trim, in a running
-- process (Task 25 retries in the same process): the next 'freezeOnce'
-- finishes that trim from the stored position before it does anything else.
trimExceptionTest :: Assertion
trimExceptionTest = withFixture $ \root twin -> do
    thrown <- newIORef False
    calls <- newIORef (0 :: Int)
    let gate n = do
            v <- _ccVerified (_nCold n)
            when (v == Just finalFrontier) $ do
                i <- atomicModifyIORef' calls (\i -> (i + 1, i + 1))
                t <- readIORef thrown
                when (i == 2 && not t) $ writeIORef thrown True >> throwIO (Crash i)
    withSmallChunkNodeGate root gate $ \n -> do
        r <- try (runToEnd (_nEnv n))
        case r of
            Left (Crash _) -> return ()
            Right o -> assertFailure $ "expected the trim to throw, got " <> show o
        trimFrom <- _cmTrimFrom <$> _ccMeta (_nCold n)
        assertBool ("the stored trim position moved inside the step: " <> show trimFrom)
            (trimFrom > _frEndTxId (frontierAt 5) && trimFrom < _frEndTxId finalFrontier)
        -- the same process calls again
        freezeOnce (_nEnv n) >>= (@?= NothingToDo)
        forM_ dataTables $ \t -> do
            expected <- expectedAfterTrim twin finalFrontier t
            rowsOf (_nPact n) t >>= assertEqual (T.unpack t) expected
        (_cmTrimFrom <$> _ccMeta (_nCold n)) >>= (@?= _frEndTxId finalFrontier)

-- | The stored trim position reaches F when a trim finishes, so a restart
-- does not trim the last step again.
noRetrimTest :: Assertion
noRetrimTest = withFixture $ \root _ -> do
    withPlainNode root $ void . runToEnd . _nEnv
    calls <- newIORef (0 :: Int)
    withNode root testConfig writeBatchSync (const noHooks) (const (modifyIORef' calls (+ 1))) $ \n -> do
        freezeOnce (_nEnv n) >>= (@?= NothingToDo)
        _fsTrimChunks <$> readIORef (_feStats (_nEnv n)) >>= (@?= 0)
    readIORef calls >>= (@?= 0)

-- | A step of 6,000 txids over 500 keys written every 500 txids and one hot
-- key written at every txid, trimmed with a fixed window of 100 txids: 60
-- chunks, each deleting exactly its window's superseded rows, and the stored
-- trim position moves one window per chunk. A chunk's work is its window, so
-- the count of chunks is rows / window, whatever the number of keys.
largeStepWindowTest :: Assertion
largeStepWindowTest = withSystemTempDirectory "freezer-large" $ \tmp -> do
    let root = tmp </> "0"
        perBlock = 2000 :: Int
        tbl' = "big_table"
        big h = BlockCommit
            { _bcHeight = fromIntegral h
            , _bcHash = mkHash h 0
            , _bcEndTxId = fromIntegral ((h + 1) * perBlock)
            , _bcNewTables = [ tbl' | h == 0 ]
            , _bcRows =
                [ (tbl', concat
                    [ [ ("k" <> B8.pack (show (i `mod` 500)), fromIntegral i, "{}")
                      , ("hot", fromIntegral i, "{}")
                      ]
                    | i <- [h * perBlock .. (h + 1) * perBlock - 1]
                    ])
                ]
            , _bcTxHashes = [ "txhash-" <> B8.pack (show h) ]
            }
        cfg = testConfig { _phFreezeDepth = 1, _phFreezeStepHeights = 3 }
    createDirectoryIfMissing True (root </> sqliteDirName)
    withSQLiteConnection (root </> sqliteDirName </> chainDbFileName cid) chainwebPragmas $ \db -> do
        _psInitSchema (sqlitePactStore db)
        commitAll db (big <$> [0 .. 4])
    chunks <- newIORef []
    let hooks n = noHooks
            { _fhAfterTrimChunk = do
                pos <- _cmTrimFrom <$> _ccMeta (_nCold n)
                rows <- _fsRowsTrimmed <$> readIORef (_feStats (_nEnv n))
                modifyIORef' chunks ((pos, rows) :)
            }
    withNodeW (TrimWindow 100 100 100) root cfg writeBatchSync hooks (const (return ())) $ \n -> do
        outcomes <- runToEnd (_nEnv n)
        outcomes @?= [Froze (Frontier 2 (mkHash 2 0) 6000), NothingToDo]
        cs <- reverse <$> readIORef chunks
        _fsTrimChunks <$> readIORef (_feStats (_nEnv n)) >>= (@?= 60)
        -- the stored position moves one window per chunk
        fst <$> cs @?= [ fromIntegral (100 * i) | i <- [1 .. 60 :: Int] ]
        -- each chunk deletes its window's superseded rows, nothing more: the
        -- k-keys' newest rows below F are at 5500..5999, the hot key's at 5999
        let deleted = zipWith (-) (snd <$> cs) (0 : (snd <$> cs))
        deleted @?= replicate 55 200 ++ replicate 4 100 ++ [99]
        rs <- rowsOf (_nPact n) tbl'
        length [ () | (_, _, x, _) <- rs, x < 6000 ] @?= 501
        -- heights 3 and 4 (txids 6000 to 9999) stay whole
        length rs @?= 8000 + 501

-- -------------------------------------------------------------------------- --
-- Halts

putRaw :: ColdStore -> B.ByteString -> B.ByteString -> IO ()
putRaw cs = R.put (_rocksDbHandle (coldStoreRocksDb cs)) R.defaultWriteOptions

-- | Corrupts the R entry of (table, key, txid) when it exists.
corrupt :: ColdStore -> Text -> B.ByteString -> Word64 -> IO ()
corrupt cs t key x = coldTableId cs t >>= \case
    Nothing -> assertFailure $ "no table id for " <> T.unpack t
    Just tid -> do
        let k = rowKey cid tid key x
        coldRawGet cs k >>= \case
            Nothing -> assertFailure $ "no R entry to corrupt for " <> show (t, key, x)
            Just _ -> putRaw cs k "{\"corrupt\":1}"

corruptTest :: Assertion
corruptTest = withFixture $ \root twin -> do
    let hooks n = noHooks { _fhAfterWrite = corrupt (_nStore n) "coin_table" "alice" 2 }
    withNode root testConfig writeBatchSync hooks (const (return ())) $ \n -> do
        freezeOnce (_nEnv n) >>= \case
            HaltedVerify e -> do
                _vfTable e @?= "coin_table"
                _vfTxId e @?= Just 2
            o -> assertFailure $ "expected HaltedVerify, got " <> show o
        _ccVerified (_nCold n) >>= (@?= Nothing)
        _ccWritten (_nCold n) >>= (@?= Just (frontierAt 2))
        -- the rows are invisible to readers
        _ccReadNewest (_nCold n) "coin_table" "alice" maxBound >>= (@?= Nothing)
    -- a restart verifies again and halts again
    withPlainNode root $ \n -> do
        freezeOnce (_nEnv n) >>= \case
            HaltedVerify e -> _vfTable e @?= "coin_table"
            o -> assertFailure $ "expected HaltedVerify after a restart, got " <> show o
        _ccVerified (_nCold n) >>= (@?= Nothing)
        -- SQLite is untouched and still answers like the legacy store
        a <- dumpTables (_nPact n) (dataTables ++ bookkeeping)
        b <- dumpTables twin (dataTables ++ bookkeeping)
        a @?= b
        _psReadNewest (sqlitePactStore (_nPact n)) "coin_table" "alice" (Just 3) >>= (@?= Right (Just "{\"a2\":0}"))
    doesDirectoryExist (root </> splitDirName) >>= (@?= False)

newTableVerifyTest :: Assertion
newTableVerifyTest = do
    -- a step that creates Late_Table (heights 6 to 8) verifies through the
    -- freezer view, while the published reads do not see it yet
    withFixture $ \root _ -> do
        seen <- newIORef []
        let hooks n = noHooks
                { _fhAfterWrite = do
                    w <- _ccWritten (_nCold n)
                    when (w == Just (frontierAt 8)) $ do
                        let c = _nCold n
                        tid <- _ccTableId c "Late_Table"
                        viewRows <- _fvRowsAtTxId (_ccFreezerView c) "late_table" 68
                        readRows <- _ccRowsAtTxId c "Late_Table" 68
                        viewRow <- _fvRowAt (_ccFreezerView c) "Late_Table" "y\0" 68
                        writeIORef seen [isNothing tid, viewRows == Just [("x", "{\"x8\":6}"), ("y\0", "{\"y\":6}")], isNothing readRows, viewRow == Just "{\"y\":6}"]
                }
        withNode root testConfig writeBatchSync hooks (const (return ())) $ \n -> do
            outcomes <- runToEnd (_nEnv n)
            outcomes @?= (Froze . frontierAt <$> [2, 5, 8]) ++ [NothingToDo]
            readIORef seen >>= (@?= [True, True, True, True])
            _ccTableId (_nCold n) "Late_Table" >>= assertBool "published" . isJust
    -- a corrupted value of the new table fails verify
    withFixture $ \root _ -> do
        let hooks n = noHooks
                { _fhAfterWrite = do
                    w <- _ccWritten (_nCold n)
                    when (w == Just (frontierAt 8)) $ corrupt (_nStore n) "Late_Table" "y\0" 68
                }
        withNode root testConfig writeBatchSync hooks (const (return ())) $ \n -> do
            outcomes <- runToEnd (_nEnv n)
            case outcomes of
                [Froze _, Froze _, HaltedVerify e] -> do
                    _vfTable e @?= "Late_Table"
                    _vfTxId e @?= Just 68
                o -> assertFailure $ "expected a HaltedVerify on the third step, got " <> show o
            _ccVerified (_nCold n) >>= (@?= Just (frontierAt 5))

data DiskFull = DiskFull
    deriving Show
instance Exception DiskFull

writeFailureTest :: Assertion
writeFailureTest = withFixture $ \root twin -> do
    let failing _ _ = throwIO DiskFull
    withNode root testConfig failing (const noHooks) (const (return ())) $ \n -> do
        r <- try (freezeOnce (_nEnv n))
        case r of
            Left DiskFull -> return ()
            Right o -> assertFailure $ "expected the write failure to propagate, got " <> show o
        _ccVerified (_nCold n) >>= (@?= Nothing)
        _ccWritten (_nCold n) >>= (@?= Nothing)
        a <- dumpTables (_nPact n) (dataTables ++ bookkeeping)
        b <- dumpTables twin (dataTables ++ bookkeeping)
        a @?= b
        _psReadNewest (sqlitePactStore (_nPact n)) "coin_table" "alice" Nothing >>= (@?= Right (Just "{\"a3\":12}"))
    doesDirectoryExist (root </> splitDirName) >>= (@?= False)

crashRewindTest :: Assertion
crashRewindTest = withFixture $ \root _ -> do
    let hooks _ = noHooks { _fhAfterWrite = throwIO (Crash 1) }
    r <- try $ withNode root testConfig writeBatchSync hooks (const (return ())) $ runToEnd . _nEnv
    case r of
        Left (Crash _) -> return ()
        Right o -> assertFailure $ "expected the crash, got " <> show o
    -- the node rewinds below the written height 2 and writes another block 2
    withSQLiteConnection (root </> sqliteDirName </> chainDbFileName cid) chainwebPragmas $ \db -> do
        forM_ (dataTables ++ ["Late_Table"]) $ \t ->
            P4SQL.exec' db ("DELETE FROM [" <> SQ3.Utf8 (T.encodeUtf8 t) <> "] WHERE txid >= ?") [P4SQL.SInt 20]
        P4SQL.exec' db "DELETE FROM BlockHistory WHERE blockheight >= ?" [P4SQL.SInt 2]
        P4SQL.exec' db "DELETE FROM VersionedTableMutation WHERE blockheight >= ?" [P4SQL.SInt 2]
        P4SQL.exec' db "DELETE FROM TransactionIndex WHERE blockheight >= ?" [P4SQL.SInt 2]
        P4SQL.exec' db "DELETE FROM VersionedTableCreation WHERE createBlockheight >= ?" [P4SQL.SInt 2]
        commitAll db [ (block 2) { _bcHash = mkHash 2 7, _bcRows = [("coin_table", [("alice", 21, "{\"other\":2}")])] } ]
    withPlainNode root $ \n -> do
        freezeOnce (_nEnv n) >>= (@?= HaltedHash (frontierAt 2))
        _ccVerified (_nCold n) >>= (@?= Nothing)
        -- no stale cold row is visible
        _ccReadNewest (_nCold n) "coin_table" "alice" maxBound >>= (@?= Nothing)
        _ccRowsAtTxId (_nCold n) "coin_table" 21 >>= (@?= Nothing)
        _ccDistinctKeys (_nCold n) "coin_table" maxBound >>= (@?= [])
        _psReadNewest (sqlitePactStore (_nPact n)) "coin_table" "alice" Nothing >>= (@?= Right (Just "{\"other\":2}"))
        freezeOnce (_nEnv n) >>= (@?= HaltedHash (frontierAt 2))

hashMismatchTest :: Assertion
hashMismatchTest = withFixture $ \root twin -> do
    let hooks n = noHooks
            { _fhAfterPublish = P4SQL.exec' (_nPact n) "UPDATE BlockHistory SET hash = ? WHERE blockheight = ?"
                [P4SQL.SBlob (runPutS (encodeBlockHash (mkHash 2 9))), P4SQL.SInt 2]
            }
    withNode root testConfig writeBatchSync hooks (const (return ())) $ \n -> do
        freezeOnce (_nEnv n) >>= (@?= HaltedHash (frontierAt 2))
        _ccVerified (_nCold n) >>= (@?= Just (frontierAt 2))
        _fsRowsTrimmed <$> readIORef (_feStats (_nEnv n)) >>= (@?= 0)
        a <- dumpTables (_nPact n) dataTables
        b <- dumpTables twin dataTables
        a @?= b
        -- the halt holds
        freezeOnce (_nEnv n) >>= (@?= HaltedHash (frontierAt 2))
        -- trimStep on its own reports the mismatch
        trimStep (_nEnv n) (frontierAt 2) 0 >>= (@?= TrimHashMismatch)
    -- a restart retries the trim and halts again, deleting nothing
    withPlainNode root $ \n -> do
        freezeOnce (_nEnv n) >>= (@?= HaltedHash (frontierAt 2))
        a <- dumpTables (_nPact n) dataTables
        b <- dumpTables twin dataTables
        a @?= b

-- | The bytes 'readStep' counts for a block's rows.
blockBytes :: Int -> Int
blockBytes h = sum
    [ B.length k + B.length d + 8
    | (t, rs) <- _bcRows (block h), frozenTable t, (k, _, d) <- rs
    ]

byteCapTest :: Assertion
byteCapTest = withFixture $ \root twin -> do
    let b0 = blockBytes 0
        stepTo cap = withNode root testConfig { _phFreezeStepBytes = cap } writeBatchSync (const noHooks) (const (return ())) $ \n ->
            withFreezerReadConnection (_nEnv n) $ \db -> planStep (_nEnv n) db >>= \case
                Nothing -> assertFailure "no plan" >> undefined
                Just plan -> do
                    length (_spBlocks plan) @?= 3
                    readStep (_nEnv n) db plan
    s1 <- stepTo 1
    _stpTo s1 @?= frontierAt 0
    s2 <- stepTo b0
    _stpTo s2 @?= frontierAt 0
    s3 <- stepTo (b0 + 1)
    _stpTo s3 @?= frontierAt 1
    -- the rows of blocks 0 and 1, per table, in rowid order
    forM_ (_stpTables s3) $ \(t, rows) -> do
        expected <- filter (\(_, _, x, _) -> x < 20) <$> rowsOf twin t
        assertEqual (T.unpack t) [ StepRow k (fromIntegral x) d | (_, k, x, d) <- expected ] rows
    sort (fst <$> _stpTables s3) @?= sort ["coin_table", "SYS:Pacts", "SYS:KeySets"]
    s4 <- stepTo maxBound
    _stpTo s4 @?= frontierAt 2
    -- with a cap of one byte every block is a step of its own
    withNode root testConfig { _phFreezeStepBytes = 1 } writeBatchSync (const noHooks) (const (return ())) $ \n -> do
        outcomes <- runToEnd (_nEnv n)
        outcomes @?= (Froze . frontierAt <$> [0 .. 7]) ++ [NothingToDo]
        forM_ dataTables $ \t -> do
            expected <- expectedAfterTrim twin (frontierAt 7) t
            rowsOf (_nPact n) t >>= assertEqual (T.unpack t) expected

explainTest :: Assertion
explainTest = withFixture $ \root _ -> withPlainNode root $ \n -> do
    keep <- sqliteExplainTrim (_nPact n) "coin_table" KeepAnchors
    drop' <- sqliteExplainTrim (_nPact n) "SYS:Pacts" DropAll
    forM_ (keep ++ drop') $ \d ->
        assertBool ("no full scan: " <> T.unpack d) (not ("SCAN " `T.isPrefixOf` d) || "SCAN CONSTANT" `T.isPrefixOf` d)
    keep @?=
        [ "SEARCH coin_table USING INTEGER PRIMARY KEY (rowid=?)"
        , "LIST SUBQUERY 1"
        , "SEARCH n USING INDEX coin_table_ix (txid>? AND txid<?)"
        , "SEARCH o USING COVERING INDEX sqlite_autoindex_coin_table_1 (rowkey=? AND txid<?)"
        , "SEARCH coin_table USING INTEGER PRIMARY KEY (rowid=?)"
        , "LIST SUBQUERY 2"
        , "SEARCH o USING INDEX coin_table_ix (txid>? AND txid<?)"
        , "CORRELATED SCALAR SUBQUERY 1"
        , "SEARCH n USING COVERING INDEX sqlite_autoindex_coin_table_1 (rowkey=? AND txid>? AND txid<?)"
        ]
    -- one window, atomic: every SYS:Pacts row below the window's end
    drop' @?= [ "SEARCH SYS:Pacts USING INDEX SYS:Pacts_ix (txid<?)" ]

splitMarkerTest :: Assertion
splitMarkerTest = withFixture $ \root twin -> do
    seen <- newIORef Nothing
    let gate n = readIORef seen >>= \case
            Just _ -> return ()
            Nothing -> do
                marker <- doesFileExist (root </> splitDirName </> splitMarkerFileName)
                legacy <- doesDirectoryExist (root </> sqliteDirName)
                a <- dumpTables (_nPact n) dataTables
                b <- dumpTables twin dataTables
                writeIORef seen (Just (marker, legacy, a == b))
    withNode root testConfig writeBatchSync (const noHooks) gate $ \n -> do
        doesDirectoryExist (root </> splitDirName) >>= (@?= False)
        void $ runToEnd (_nEnv n)
    readIORef seen >>= (@?= Just (True, False, True))
    readSplitMarker (root </> splitDirName) >>= \case
        Just m -> do
            _smFirstTrimChain m @?= Just cid
            _smFirstTrimHeight m @?= Just 2
        Nothing -> assertFailure "no marker"
