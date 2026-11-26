{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.Test.Pact.Store.Schedule
--
-- Tests for the freezer's scheduling (storage redesign P3, spec 6.3 and 6.5):
-- the trim gate at the tip and during catch-up, the node-wide semaphore, the
-- halt on a failed cold write, the drain, the stats, a rewind racing a
-- freezer step (parked pointer M3) and a trim chunk inside an outer
-- transaction (N1). The trim benchmark runs only with
-- @CHAINWEB_TRIM_BENCH=1@.
--
module Chainweb.Test.Pact.Store.Schedule
( tests
) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Lens ((&), (.~))
import Control.Monad

import qualified Data.Aeson as A
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import Data.IORef
import Data.Maybe (isJust, isNothing)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import GHC.Clock (getMonotonicTime)

import System.Directory
import System.Environment (lookupEnv)
import System.FilePath
import System.IO (hPutStrLn, stderr)
import System.IO.Error (fullErrorType, ioeSetErrorString, mkIOError)
import System.IO.Temp (withSystemTempDirectory)
import System.LogLevel
import System.Timeout (timeout)

import Test.Tasty
import Test.Tasty.HUnit

import qualified Pact.Types.Persistence as Pact4
import qualified Pact.Types.SQLite as P4SQL

-- internal modules

import Chainweb.BlockHeader.Internal
import Chainweb.Logger
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Pact.Backend.Store.Freezer
import Chainweb.Pact.Backend.Store.Layout
import Chainweb.Pact.Backend.Store.SQLite
import Chainweb.Pact.Backend.Store.Split
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Pact.Backend.Utils (chainDbFileName, chainwebPragmas, withSQLiteConnection)
import Chainweb.Storage.Table.RocksDB (writeBatchSync)
import Chainweb.Test.Pact.Store.Freezer hiding (tests)
import Chainweb.Test.TestVersions (slowForkingCpmTestVersion)
import Chainweb.Test.Utils (petersen)
import Chainweb.Time (Micros(..), Time(..), TimeSpan(..), epoch, getCurrentTimeIntegral)

tests :: TestTree
tests = testGroup "Freezer schedule"
    [ testCase "tip mode: no chunk starts while a block waits" tipModeTest
    , testCase "catch-up mode: chunk time over 60 s stays within the duty cycle plus one chunk" catchUpTest
    , testCase "never more than _phFreezerConcurrency steps in read, write or verify at once" concurrencyTest
    , testCase "cold write fails with ENOSPC: FreezerHalted logged, no trim, reads still equal the legacy twin" enospcTest
    , testCase "drainFreezer ends with planStep = Nothing" drainTest
    , testCase "FreezerStats: file bytes >= live bytes; the peak never decreases" statsTest
    , testCase "M3: a rewind below a step being written waits for its publish and is refused; one above it does not wait" rewindWaitsTest
    , testCase "M3: a rewind during a step's read drops the step; nothing is written" rewindDropsTest
    , testCase "N1: a trim chunk inside an outer transaction is refused; nothing deleted, the trim position stays" outerTxnTest
    , testCase "trim benchmark (runs with CHAINWEB_TRIM_BENCH=1)" trimBench
    ]

-- -------------------------------------------------------------------------- --
-- Helpers

parentAt :: Int -> ParentHeader
parentAt h = ParentHeader $ genesisBlockHeader (slowForkingCpmTestVersion petersen) cid
    & blockHash .~ mkHash h 0
    & blockHeight .~ fromIntegral h

-- | The split store over a node's Pact connection.
splitOfNode :: Node -> IO PactStore
splitOfNode n = do
    ref <- newIORef emptySplitCounters
    return $ splitPactStore SplitEnv
        { _seChain = cid
        , _seHot = sqlitePactStore (_nPact n)
        , _seCold = _nCold n
        , _seCounters = ref
        }

-- | Runs an action on the Pact connection under the node's lock, as the
-- Pact thread runs under @cpModuleCacheVar@.
underLock :: Node -> IO a -> IO a
underLock n act = _feWithPactDb (_nEnv n) (const act)

capturingLogger :: LogLevel -> IO (GenericLogger, IO [T.Text])
capturingLogger level = do
    ref <- newIORef []
    return (genericLogger level (\t -> atomicModifyIORef' ref (\ls -> (t : ls, ()))), reverse <$> readIORef ref)

withFixtures :: Int -> ([FilePath] -> IO a) -> IO a
withFixtures 0 k = k []
withFixtures i k = withFixture $ \root _ -> withFixtures (i - 1) (k . (root :))

withNodes :: [FilePath] -> (Node -> FreezerHooks) -> ([Node] -> IO a) -> IO a
withNodes [] _ k = k []
withNodes (r : rs) hooks k =
    withNode r testConfig writeBatchSync hooks (const (return ())) $ \n ->
        withNodes rs hooks (k . (n :))

waitUntil :: IO Bool -> IO ()
waitUntil p = p >>= \case
    True -> return ()
    False -> threadDelay 20_000 >> waitUntil p

stats :: Node -> IO FreezerStats
stats = readIORef . _feStats . _nEnv

-- -------------------------------------------------------------------------- --
-- Gate

tipModeTest :: Assertion
tipModeTest = withFixture $ \root _ -> do
    waiting <- newTVarIO True
    window <- newTrimDutyWindow
    now <- getCurrentTimeIntegral
    let tg = TrimGateEnv
            { _tgBlockWaiting = readTVar waiting
            , _tgLatestBlockTime = return (Just now)
            , _tgConfig = testConfig
            , _tgWindow = window
            , _tgClock = realGateClock
            }
    withNode root testConfig writeBatchSync (const noHooks) (\_ -> trimGate tg) $ \n -> do
        a <- async (freezeOnce (_nEnv n))
        threadDelay 1_500_000
        poll a >>= \r -> assertBool "freezeOnce waits at the gate" (isNothing r)
        -- the step is published; its trim waits
        _ccVerified (_nCold n) >>= (@?= Just (frontierAt 2))
        _fsTrimChunks <$> stats n >>= (@?= 0)
        atomically (writeTVar waiting False)
        timeout 10_000_000 (wait a) >>= (@?= Just (Froze (frontierAt 2)))
        c <- _fsTrimChunks <$> stats n
        assertBool "the trim ran once no block waited" (c > 0)

catchUpTest :: Assertion
catchUpTest = do
    clock <- newIORef (0 :: Double)
    window <- newTrimDutyWindow
    let gc = GateClock
            { _gcMonotonic = readIORef clock
            , _gcWall = return (Time (TimeSpan (Micros 3_600_000_000)))
            , _gcSleep = \d -> modifyIORef' clock (+ d)
            }
        duty = 0.1
        tg = TrimGateEnv
            { _tgBlockWaiting = return True
            , _tgLatestBlockTime = return (Just epoch)
            , _tgConfig = testConfig { _phTrimDutyCycle = duty, _phCatchUpLagSeconds = 600 }
            , _tgWindow = window
            , _tgClock = gc
            }
        chunk = 0.25
    starts <- forM [1 .. 800 :: Int] $ \_ -> do
        trimGate tg
        s <- readIORef clock
        modifyIORef' clock (+ chunk)
        recordTrimChunk tg chunk
        return s
    end <- readIORef clock
    let inSpan a = sum
            [ min (s + chunk) (a + 60) - max s a
            | s <- starts, s + chunk > a, s < a + 60
            ]
        worst = maximum (inSpan <$> [0, 0.5 .. end - 60])
    assertBool ("worst 60 s span " <> show worst) (worst <= duty * 60 + chunk + 1e-9)
    let share = fromIntegral (length starts) * chunk / end
    assertBool ("the trim is not starved: share " <> show share) (share >= 0.09)

-- -------------------------------------------------------------------------- --
-- Running

concurrencyTest :: Assertion
concurrencyTest = withFixtures 4 $ \roots -> do
    active <- newTVarIO (0 :: Int)
    peak <- newTVarIO 0
    sem <- newQSem (_phFreezerConcurrency testConfig)
    let enter = atomically $ do
            a <- (+ 1) <$> readTVar active
            writeTVar active a
            modifyTVar' peak (max a)
        leave = atomically $ modifyTVar' active (subtract 1)
        hooks _ = noHooks
            { _fhAfterRead = enter >> threadDelay 30_000
            , _fhAfterVerify = leave
            }
    withNodes roots hooks $ \ns -> do
        as <- forM ns $ \n -> async (runFreezer (_nEnv n) sem)
        done <- timeout 60_000_000 $ forM_ ns $ \n ->
            waitUntil ((== Just finalFrontier) <$> _ccVerified (_nCold n))
        mapM_ cancel as
        done @?= Just ()
    _phFreezerConcurrency testConfig @?= 2
    readTVarIO peak >>= (@?= 2)

enospcTest :: Assertion
enospcTest = withFixture $ \root twin -> do
    (logger, logs) <- capturingLogger Error
    let enospc = mkIOError fullErrorType "writeBatchSync" Nothing Nothing
            `ioeSetErrorString` "No space left on device"
        failing _ _ = throwIO enospc
    withNode root testConfig failing (const noHooks) (const (return ())) $ \n -> do
        sem <- newQSem 2
        timeout 30_000_000 (runFreezer ((_nEnv n) { _feLogger = logger }) sem) >>= (@?= Just ())
        ls <- logs
        assertBool ("FreezerHalted at Error: " <> show ls) (any ("FreezerHalted" `T.isInfixOf`) ls)
        assertBool ("names the full disk: " <> show ls) (any ("No space left" `T.isInfixOf`) ls)
        _fsTrimChunks <$> stats n >>= (@?= 0)
        _ccWritten (_nCold n) >>= (@?= Nothing)
        _ccVerified (_nCold n) >>= (@?= Nothing)
        a <- dumpTables (_nPact n) (dataTables ++ bookkeeping)
        b <- dumpTables twin (dataTables ++ bookkeeping)
        a @?= b
        split <- splitOfNode n
        let legacy = sqlitePactStore twin
        forM_ ["coin_table", "SYS:Pacts", "Late_Table"] $ \t -> do
            ks <- _psDistinctKeys legacy (KeysAsStored Nothing) t
            _psDistinctKeys split (KeysAsStored Nothing) t >>= (@?= ks)
            forM_ (either (const []) id ks ++ ["absent"]) $ \k -> forM_ [Nothing, Just 50, Just 100] $ \bound -> do
                x <- _psReadNewest legacy t k bound
                _psReadNewest split t k bound >>= (@?= x)
    doesDirectoryExist (root </> splitDirName) >>= (@?= False)

drainTest :: Assertion
drainTest = withFixture $ \root _ -> do
    (logger, logs) <- capturingLogger Info
    -- a gate that never opens: the drain does not use it
    let closed _ = forever (threadDelay 1_000_000)
    withNode root testConfig writeBatchSync (const noHooks) closed $ \n -> do
        let env = (_nEnv n) { _feLogger = logger }
        timeout 30_000_000 (drainFreezer env) >>= (@?= Just ())
        withFreezerReadConnection env (planStep env) >>= (@?= Nothing)
        _ccVerified (_nCold n) >>= (@?= Just finalFrontier)
        _cmTrimFrom <$> _ccMeta (_nCold n) >>= (@?= _frEndTxId finalFrontier)
        ls <- logs
        assertBool ("FreezerDrained logged: " <> show ls)
            (any (\l -> "FreezerDrained" `T.isInfixOf` l && "seconds" `T.isInfixOf` l) ls)

statsTest :: Assertion
statsTest = withFixture $ \root _ -> withPlainNode root $ \n -> do
    let env = _nEnv n
        go peaks = freezeOnce env >>= \case
            Froze _ -> do
                st <- stats n
                assertBool ("file bytes >= live bytes: " <> show (_fsHotFileBytes st, _fsHotLiveBytes st))
                    (_fsHotFileBytes st >= _fsHotLiveBytes st && _fsHotLiveBytes st > 0)
                assertBool "the peak is at least the file size" (_fsHotPeakBytes st >= _fsHotFileBytes st)
                go (_fsHotPeakBytes st : peaks)
            NothingToDo -> return (reverse peaks)
            o -> assertFailure ("unexpected " <> show o) >> return []
    peaks <- go []
    assertBool "several steps" (length peaks >= 2)
    assertBool ("the peak never decreases: " <> show peaks) (and (zipWith (<=) peaks (drop 1 peaks)))
    st <- stats n
    _fsSteps st @?= length peaks
    assertBool "user and SYS rows counted apart" (_fsRowsUser st > 0 && _fsRowsSys st > 0)
    assertBool "bytes, write and verify time" (_fsBytes st > 0 && _fsWriteMs st > 0 && _fsVerifyMs st > 0)
    assertBool "trim percentiles" (isJust (trimChunkPercentile 0.99 st))
    let json = T.decodeUtf8 (BL.toStrict (A.encode (freezerStatsJson cid st)))
    forM_ ["FreezerStats", "rowsUser", "rowsSys", "trimP99Ms", "hotFileBytes", "hotLiveBytes", "hotPeakBytes", "coldCacheHits", "split"] $ \k ->
        assertBool ("stats JSON has " <> T.unpack k) (k `T.isInfixOf` json)

-- -------------------------------------------------------------------------- --
-- M3: rewinds and freezer steps

rewindWaitsTest :: Assertion
rewindWaitsTest = withFixture $ \root _ -> do
    written <- newEmptyMVar
    release <- newEmptyMVar
    let hooks _ = noHooks { _fhAfterWrite = putMVar written () >> takeMVar release }
    withNode root testConfig writeBatchSync hooks (const (return ())) $ \n -> do
        split <- splitOfNode n
        fr <- async (freezeOnce (_nEnv n))
        takeMVar written
        -- the step to height 2 (end txid 30) is written, not published
        _ccWritten (_nCold n) >>= (@?= Just (frontierAt 2))
        -- a rewind above the step does not wait
        r1 <- timeout 5_000_000 $ underLock n $ _psWithBatch split $ _psRewindDbTo split (Just (parentAt 10))
        r1 @?= Just 110
        -- a rewind below the step's end waits for the step
        rw <- async $ underLock n $ try $ _psWithBatch split $ _psRewindDbTo split (Just (parentAt 1))
        threadDelay 500_000
        poll rw >>= \r -> assertBool "the rewind waits for the step" (isNothing r)
        putMVar release ()
        timeout 10_000_000 (wait rw) >>= \case
            Just (Left e) -> e @?= RewindBelowFrontier cid 20 (frontierAt 2)
            r -> assertFailure $ "expected RewindBelowFrontier, got " <> show (fmap (either show show) r)
        timeout 10_000_000 (wait fr) >>= (@?= Just (Froze (frontierAt 2)))
        fmap (\(h, _, _) -> h) <$> sqliteTip (_nPact n) >>= (@?= Just 10)

rewindDropsTest :: Assertion
rewindDropsTest = withFixture $ \root _ -> do
    readDone <- newEmptyMVar
    release <- newEmptyMVar
    let hooks _ = noHooks { _fhAfterRead = putMVar readDone () >> takeMVar release }
    withNode root testConfig writeBatchSync hooks (const (return ())) $ \n -> do
        split <- splitOfNode n
        fr <- async (freezeOnce (_nEnv n))
        takeMVar readDone
        -- the step to height 2 is read; the hot store rewinds to height 1
        r <- underLock n $ _psWithBatch split $ _psRewindDbTo split (Just (parentAt 1))
        r @?= 20
        putMVar release ()
        timeout 10_000_000 (wait fr) >>= (@?= Just StepDropped)
        _ccWritten (_nCold n) >>= (@?= Nothing)
        _ccVerified (_nCold n) >>= (@?= Nothing)
    -- the next step reads the rewound store: too short to freeze
    withPlainNode root $ \n -> freezeOnce (_nEnv n) >>= (@?= NothingToDo)

-- -------------------------------------------------------------------------- --
-- N1: trim position and outer transactions

outerTxnTest :: Assertion
outerTxnTest = withFixture $ \root twin -> withPlainNode root $ \n -> do
    P4SQL.exec_ (_nPact n) "BEGIN;"
    try (freezeOnce (_nEnv n)) >>= \case
        Left (e :: SomeException) -> assertBool ("refused: " <> show e) ("outer transaction" `T.isInfixOf` T.pack (show e))
        Right o -> assertFailure $ "expected the trim to be refused, got " <> show o
    P4SQL.exec_ (_nPact n) "ROLLBACK;"
    _ccVerified (_nCold n) >>= (@?= Just (frontierAt 2))
    _cmTrimFrom <$> _ccMeta (_nCold n) >>= (@?= 0)
    _fsTrimChunks <$> stats n >>= (@?= 0)
    a <- dumpTables (_nPact n) dataTables
    b <- dumpTables twin dataTables
    a @?= b
    -- the next call resumes the trim
    void $ runToEnd (_nEnv n)
    _cmTrimFrom <$> _ccMeta (_nCold n) >>= (@?= _frEndTxId finalFrontier)

-- -------------------------------------------------------------------------- --
-- Trim benchmark

-- | Blocks @0..150@: 50 user tables of 200 keys each, 20 rows per table and
-- block, one txid per row index (so one txid holds 50 rows); 5 @SYS:Pacts@
-- rows per block; and the table @big@, whose 25,000 keys are written at a
-- single txid in block 40 and again at a single txid in block 100 (N2).
-- About 200k rows below the frontier.
benchHistory :: [BlockCommit]
benchHistory = blk <$> [0 .. 150]
  where
    tables = [ "bt" <> T.pack (show i) | i <- [10 .. 59 :: Int] ]
    blk h = BlockCommit
        { _bcHeight = fromIntegral h
        , _bcHash = mkHash (h `mod` 251) (fromIntegral (h `div` 251) + 1)
        , _bcEndTxId = tx h 1000
        , _bcNewTables = if h == 0 then "big" : tables else []
        , _bcRows =
            [ (t, [ (key ((h * 7 + j) `mod` 200), tx h j, val h j) | j <- [0 .. 19] ]) | t <- tables ]
            ++ [ ("SYS:Pacts", [ ("p" <> n (h * 5 + j), tx h (100 + j), val h j) | j <- [0 .. 4] ]) ]
            ++ [ ("big", [ ("b" <> n i, tx h 500, val h i) | i <- [0 .. 24_999 :: Int] ]) | h == 40 || h == 100 ]
        , _bcTxHashes = [ "bench-" <> n h ]
        }
    tx :: Int -> Int -> Pact4.TxId
    tx h j = fromIntegral (1000 * h + j)
    key i = "k" <> n i
    n :: Int -> B8.ByteString
    n = B8.pack . show
    val h j = "{\"balance\":" <> n (h * 1000 + j) <> ",\"guard\":{\"keys\":[\"368820f80c324bbc7c2b0610688a7da43e39f91d118732671cd9c7500ff43cca\"],\"pred\":\"keys-all\"}}"

trimBench :: Assertion
trimBench = lookupEnv "CHAINWEB_TRIM_BENCH" >>= \case
    Nothing -> return ()
    -- @fixed100@: a fixed window of 100 txids; 9 of 10 windows hold almost
    -- no rows, so their time is the per-table statement cost of a chunk
    Just mode -> withSystemTempDirectory "trim-bench" $ \tmp -> do
        let root = tmp </> "0"
        createDirectoryIfMissing True (root </> sqliteDirName)
        withSQLiteConnection (root </> sqliteDirName </> chainDbFileName cid) chainwebPragmas $ \db -> do
            _psInitSchema (sqlitePactStore db)
            commitAll db benchHistory
        let cfg = testConfig
                { _phFreezeDepth = 1
                , _phFreezeStepHeights = 140
                , _phFreezeStepBytes = 1_000_000_000_000
                }
        let tw = if mode == "fixed100" then fixedTrimWindow 100 else defaultTrimWindow
        withNodeW tw root cfg writeBatchSync (const noHooks) (const (return ())) $ \n -> do
            t0 <- getMonotonicTime
            o <- freezeOnce (_nEnv n)
            t1 <- getMonotonicTime
            st <- stats n
            let pct q = maybe "-" show (trimChunkPercentile q st)
            hPutStrLn stderr $ unlines
                [ ""
                , "TRIM BENCHMARK (" <> mode <> "): outcome " <> show o
                , "  rows frozen: user " <> show (_fsRowsUser st) <> ", SYS " <> show (_fsRowsSys st)
                    <> "; bytes " <> show (_fsBytes st)
                , "  rows trimmed: " <> show (_fsRowsTrimmed st)
                , "  chunks: " <> show (_fsTrimChunks st) <> "; final window " <> show (_fsTrimWindow st)
                , "  chunk ms: p10 " <> pct 0.1 <> ", p50 " <> pct 0.5 <> ", p90 " <> pct 0.9 <> ", p99 " <> pct 0.99 <> ", max " <> show (_fsTrimMaxMs st)
                , "  trim commit max ms " <> show (_fsTrimCommitMaxMs st) <> "; GC max ms in a chunk " <> show (_fsTrimGcMaxMs st)
                , "  trim ms total (under the lock) " <> show (_fsTrimMs st)
                    <> "; trim position writes ms " <> show (_fsTrimPositionMs st)
                , "  write ms " <> show (_fsWriteMs st) <> "; verify ms " <> show (_fsVerifyMs st)
                , "  step total s " <> show (t1 - t0)
                ]
            case o of
                Froze _ -> return ()
                _ -> assertFailure (show o)
