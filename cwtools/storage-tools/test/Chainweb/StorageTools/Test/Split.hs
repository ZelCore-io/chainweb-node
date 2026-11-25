{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.StorageTools.Test.Split
--
-- Tests of the measurement and comparison harness for the split and shadow
-- Pact history stores (storage redesign P3, Task 29): the in-process side on
-- a split database, frontier sampling, the report's frontier split and
-- latencies, @cw-measure split-size@ and @txlog-digest@.
--
-- The fixture is the in-process fixture ("Chainweb.StorageTools.Test.InProcess")
-- replayed twice from its block database: once in sqlite mode (the sqlite
-- twin) and once in split mode with a freeze depth of 2 (the split twin),
-- whose freezer runs to the end, so that it is trimmed and has a frontier on
-- each chain. Every test runs on its own copies.
--
module Chainweb.StorageTools.Test.Split
( tests
) where

import Control.Monad

import Data.Aeson
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString as B
import Data.List (find, nub)
import qualified Data.Map.Strict as M
import Data.Maybe (isJust)
import Data.Word (Word64)
import qualified Data.Text as T

import qualified Database.RocksDB.Base as R

import System.Directory
import System.FilePath ((</>))
import System.IO.Temp (createTempDirectory)

import Test.Tasty
import Test.Tasty.HUnit

-- internal modules

import Chainweb.BlockHeight (BlockHeight(..))
import Chainweb.ChainId
import Chainweb.Mempool.Consensus (mkMempoolConsensus)
import Chainweb.Mempool.Mempool (noopMempool)
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Cold.Key (chainByte, decodeKeyIdList, encodeKeyIdList, metaKey)
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Pact.Backend.Store.Layout (coldDirName, splitDirName)
import Chainweb.Pact.Backend.Store.Node
import Chainweb.Pact.Service.BlockValidation (pactSyncToBlock)
import Chainweb.Pact.Service.PactInProcApi (withPactServiceDrain)
import Chainweb.Payload.PayloadStore.RocksDB (newPayloadDb)
import Chainweb.Storage.Table.RocksDB
import Chainweb.StorageTools.Compare.InProcess
import Chainweb.StorageTools.Compare.Report
import Chainweb.StorageTools.Compare.Run (defaultRunOptions, runCompareClasses)
import Chainweb.StorageTools.Compare.Sample
import Chainweb.StorageTools.Compare.Types
import Chainweb.StorageTools.Measure.Split
import Chainweb.StorageTools.Test.InProcess hiding (tests)
import Chainweb.TreeDB (maxEntry)
import Chainweb.WebBlockHeaderDB (getWebBlockHeaderDb, initWebBlockHeaderDb)

-- -------------------------------------------------------------------------- --
-- Fixture

data Twins = Twins
    { _twFx :: !Fixture
    , _twSqlite :: !FilePath
    , _twSplit :: !FilePath
    }

-- | The freeze depth of the split twin.
--
twinDepth :: BlockHeight
twinDepth = 2

buildTwins :: IO Twins
buildTwins = do
    fx <- buildFixture
    let sq = _fxRoot fx </> "twin-sqlite"
        sp = _fxRoot fx </> "twin-split"
    replay fx HistorySqlite sq
    replay fx HistorySplit sp
    -- SST files, as after a node's shutdown flush: split-size measures them
    withColdStore (sp </> coldDirName) ColdReadWrite 8_388_608 (flushRocksDb . coldStoreRocksDb)
    return (Twins fx sq sp)

removeTwins :: Twins -> IO ()
removeTwins = removeFixture . _twFx

-- | Replays the Pact state of the fixture from its block database into a new
-- database directory: a copy of its @rocksDb@ and an empty @sqlite@, synced
-- to the tip on each chain; in split mode the freezer is drained after the
-- replay, as with @--only-sync-pact@.
--
replay :: Fixture -> PactHistoryStore -> FilePath -> IO ()
replay fx mode dst = do
    copyTree (_fxDb fx </> "rocksDb") (dst </> "rocksDb")
    createDirectoryIfMissing True (dst </> "sqlite")
    withRocksDb (dst </> "rocksDb") modernDefaultOptions $ \rdb -> do
        wbhdb <- initWebBlockHeaderDb rdb fxVersion
        let pdb = newPayloadDb rdb
        withStorage $ \st -> forM_ fxChains $ \cid -> do
            bhdb <- getWebBlockHeaderDb wbhdb cid
            mp <- noopMempool
            mpc <- mkMempoolConsensus mp bhdb (Just pdb)
            tip <- maxEntry bhdb
            withPactServiceDrain fxVersion cid quietLogger Nothing mpc bhdb pdb st fxPactConfig $ \q drain -> do
                pactSyncToBlock tip q
                void drain
  where
    cfg = defaultPactHistoryConfig
        { _phStore = mode
        , _phFreezeDepth = twinDepth
        , _phFreezeStepHeights = 1
        , _phColdBlockCacheBytes = 8_388_608
        }
    withStorage k = case mode of
        HistorySqlite -> sqliteOnlyStorage (dst </> "sqlite") >>= k
        _ -> withPactStorage quietLogger fxVersion cfg dst False $ \st ->
            k st { _pstFreezerPollMicros = 100_000 }

copyTwin :: Twins -> (Twins -> FilePath) -> String -> IO FilePath
copyTwin tw which name = do
    dir <- createTempDirectory (_fxRoot (_twFx tw)) name
    copyTree (which tw) dir
    return dir

splitSpec :: FilePath -> InProcessSpec
splitSpec dir = (e2eSpec dir) { _ipPactHistory = HistorySplit }

-- | The sqlite twin as the oracle, the split twin as the test side, on
-- copies.
--
withTwinSides :: Twins -> String -> (FilePath -> FilePath -> Side -> Side -> IO a) -> IO a
withTwinSides tw name act = do
    a <- copyTwin tw _twSqlite (name <> "-sqlite")
    b <- copyTwin tw _twSplit (name <> "-split")
    withInProcessSide quietLogger (e2eSpec a) $ \sa ->
        withInProcessSide quietLogger (splitSpec b) $ \sb -> act a b sa sb

nearFrontier :: [BlockHeight] -> BlockHeight -> Bool
nearFrontier fs h = any (\f -> max f h - min f h <= fromIntegral boundaryDistance) fs

baseSpec :: Int -> SampleSpec
baseSpec n = SampleSpec
    { _ssSeed = 7
    , _ssCount = n
    , _ssClasses = allRequestClasses
    , _ssCoverage = fullCoverage
    , _ssBoundaries = []
    , _ssBoundaryShare = boundaryShare
    , _ssHistoryMax = historyMaxHeight fxVersion
    , _ssPactChains = Nothing
    }

-- | 'everyClassTargets' (all at or below height 21) and targets at height 24,
-- above the frontier of the split twin.
--
splitTargets :: [((RequestClass, ChainId, BlockHeight), [Word64])]
splitTargets = everyClassTargets <>
    [ ((ClsHeader, cid0, 24), [1, 2])
    , ((ClsPayload, cid0, 24), [1, 2])
    , ((ClsLocal, cid0, 24), [1 .. 3])
    ]

-- | Header requests at two heights below and one at or above the frontier of
-- chain 0.
--
headerTargets :: BlockHeight -> [((RequestClass, ChainId, BlockHeight), [Word])]
headerTargets f =
    [ ((ClsHeader, cid0, 3), [1, 2])
    , ((ClsHeader, cid0, f), [1])
    ]

perClassOf :: Report -> T.Text -> Maybe Object
perClassOf rep cls = case toJSON rep of
    Object o -> do
        Object pc <- KM.lookup "perClass" o
        Object c <- KM.lookup (K.fromText cls) pc
        return c
    _ -> Nothing

meta :: FilePath -> IO [Maybe B.ByteString]
meta dir = withColdStore (dir </> coldDirName) ColdReadOnly 8_388_608 $ \cs ->
    forM fxChains $ \cid -> coldRawGet cs (metaKey cid)

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = withResource buildTwins removeTwins $ \getTw -> testGroup "Split harness"
    [ testCase "in-process split side answers every class with zero differences against the sqlite side" $ do
        tw <- getTw
        -- the split twin is trimmed and has a frontier below its tip
        doesDirectoryExist (_twSplit tw </> splitDirName) >>= assertBool "sqlite-split exists"
        withTwinSides tw "every-class" $ \_ b sa sb -> do
            fs <- readFrontierHeights b fxChains
            assertEqual "a frontier on each chain" (length fxChains) (M.size fs)
            forM_ (M.elems fs) $ \f -> assertBool ("frontier " <> show f) (f > 12 && f < finalHeight)
            api <- oracleApi sa
            reqs <- concat <$> mapM (\(t, seeds) -> generated api seeds t) everyClassTargets
            rep <- reportExpectClasses allRequestClasses
                <$> runCompareClasses quietLogger defaultRunOptions (const fullCoverage) sa sb reqs
            let summary = T.unpack (reportSummary rep)
            assertEqual ("no difference: " <> summary) 0 (reportDiffCount rep)
            assertEqual ("no order-only txlog: " <> summary) 0 (reportClassTotal rep KindOrderOnly)
            forM_ allRequestClasses $ \cls ->
                assertBool ("same 2xx for " <> show cls <> ": " <> summary) (reportSame2xx rep cls > 0)
            reportVerdict rep @?= Pass

    , testCase "frontier heights join the sampling boundaries; report counts targets below and above" $ do
        tw <- getTw
        fs <- readFrontierHeights (_twSplit tw) fxChains
        let spec = withFrontierBoundaries (M.elems fs) (baseSpec 400)
        _ssBoundaries spec @?= nub (M.elems fs)
        let near s = length [ () | (_, _, h) <- sampleTargets s, nearFrontier (M.elems fs) h ]
        assertBool ("more targets near the frontier: " <> show (near (baseSpec 400), near spec))
            (near spec > near (baseSpec 400))
        withTwinSides tw "frontier-report" $ \_ _ sa sb -> do
            api <- oracleApi sa
            let f0 = fs M.! cid0
            reqs <- concat <$> mapM (\(t, seeds) -> generated api (map fromIntegral seeds) t) (headerTargets f0)
            rep <- reportFrontiers fs
                <$> runCompareClasses quietLogger defaultRunOptions (const fullCoverage) sa sb reqs
            reportDiffCount rep @?= 0
            reportFrontierCount rep ClsHeader BelowFrontier @?= 2
            reportFrontierCount rep ClsHeader AboveFrontier @?= 1
            case perClassOf rep "header" of
                Just o -> do
                    KM.lookup "belowFrontier" o @?= Just (Number 2)
                    KM.lookup "aboveFrontier" o @?= Just (Number 1)
                    assertBool "latencyUs" (isJust (KM.lookup "latencyUs" o))
                Nothing -> assertFailure "no perClass.header in the JSON report"

    , testCase "--frontier-targets 50: every target within 5 heights of a frontier" $ do
        tw <- getTw
        fs <- readFrontierHeights (_twSplit tw) fxChains
        let ts = frontierTargets (M.elems fs) 50 (baseSpec 1000)
        length ts @?= 50
        forM_ ts $ \t@(_, _, h) -> assertBool ("near a frontier: " <> show t) (nearFrontier (M.elems fs) h)
        assertBool "more than one class" (length (nub [ c | (c, _, _) <- ts ]) > 1)
        frontierTargets [] 50 (baseSpec 1000) @?= []

    , testCase "latency fields present, ordered p50 <= p99 <= max" $ do
        tw <- getTw
        fs <- readFrontierHeights (_twSplit tw) fxChains
        withTwinSides tw "latency" $ \_ _ sa sb -> do
            api <- oracleApi sa
            reqs <- concat <$> mapM (\(t, seeds) -> generated api seeds t) splitTargets
            rep <- reportFrontiers fs
                <$> runCompareClasses quietLogger defaultRunOptions (const fullCoverage) sa sb reqs
            let stats = [ (cls, side, l) | cls <- allRequestClasses, side <- [minBound .. maxBound], Just l <- [reportLatency rep cls side] ]
            assertBool "latencies below the frontier" (any (\(_, s, _) -> s == BelowFrontier) stats)
            assertBool "latencies above the frontier" (any (\(_, s, _) -> s == AboveFrontier) stats)
            forM_ stats $ \(cls, side, l) -> assertBool (show (cls, side, l)) $
                _lsP50 l <= _lsP99 l && _lsP99 l <= _lsMax l
            forM_ allRequestClasses $ \cls ->
                case perClassOf rep (requestClassText cls) >>= KM.lookup "latencyUs" of
                    Just (Object o) -> forM_ ["below", "above"] $ \k ->
                        assertBool (show (cls, k)) (KM.member k o)
                    x -> assertFailure $ "latencyUs of " <> show cls <> ": " <> show x

    , testCase "--frontier-from splits a sqlite run at the split twin's frontier" $ do
        tw <- getTw
        fsSplit <- readFrontierHeights (_twSplit tw) fxChains
        fsSqlite <- readFrontierHeights (_twSqlite tw) fxChains
        assertBool "the sqlite twin has no frontier" (M.null fsSqlite)
        a <- copyTwin tw _twSqlite "from-a"
        b <- copyTwin tw _twSqlite "from-b"
        withInProcessSide quietLogger (e2eSpec a) $ \sa ->
            withInProcessSide quietLogger (e2eSpec b) $ \sb ->
                withTwinSides tw "from-split" $ \_ _ sa' sb' -> do
                    api <- oracleApi sa
                    reqs <- concat <$> mapM (\(t, seeds) -> generated api seeds t) splitTargets
                    let run x y = reportFrontiers fsSplit
                            <$> runCompareClasses quietLogger defaultRunOptions (const fullCoverage) x y reqs
                    repSqlite <- run sa sb
                    repSplit <- run sa' sb'
                    reportFrontierHeights repSqlite @?= fsSplit
                    forM_ allRequestClasses $ \cls -> forM_ [minBound .. maxBound] $ \side ->
                        assertEqual (show (cls, side))
                            (reportFrontierCount repSplit cls side)
                            (reportFrontierCount repSqlite cls side)
                    assertBool "targets below" (sum [ reportFrontierCount repSqlite c BelowFrontier | c <- allRequestClasses ] > 0)
                    assertBool "targets above" (sum [ reportFrontierCount repSqlite c AboveFrontier | c <- allRequestClasses ] > 0)

    , testCase "split-size: keyspace sum within 1% of total-sst-files-size; live bytes <= file bytes" $ do
        tw <- getTw
        dir <- copyTwin tw _twSplit "split-size"
        sz <- measureSplitSize dir fxChains
        let total = _spzTotalSstBytes sz
            ks = sum [ n | c <- _spzChains sz, n <- M.elems (_cszColdKeyspaceBytes c) ]
        assertBool ("SST files: " <> show sz) (total > 0)
        assertBool ("keyspaces " <> show ks <> " vs total " <> show total) $
            fromIntegral (max ks total - min ks total) <= (fromIntegral total / 100 :: Double)
        -- the fixture's cold store fits in one data block, which
        -- approximateSize attributes to a single range (block granularity):
        -- only the sum over all chains and keyspaces is checked
        forM_ (_spzChains sz) $ \c -> do
            assertBool ("live bytes: " <> show c) $
                _cszHotLiveBytes c > 0 && _cszHotLiveBytes c <= _cszHotFileBytes c

    , testCase "txlog-digest: sqlite twin and split twin equal; swapping two rows of one txid in a T list changes the digest" $ do
        tw <- getTw
        sq <- copyTwin tw _twSqlite "digest-sqlite"
        sp <- copyTwin tw _twSplit "digest-split"
        fs <- readFrontierHeights sp fxChains
        forM_ fxChains $ \cid -> do
            d1 <- txlogDigest quietLogger fxVersion sq cid HistorySqlite Nothing
            d2 <- txlogDigest quietLogger fxVersion sp cid HistorySplit Nothing
            assertBool "rows hashed" (sum (map _tdRows (M.elems (_txdTables d1))) > 0)
            _txdTables d2 @?= _txdTables d1
            _txdAll d2 @?= _txdAll d1
            -- below the frontier: the rows the split twin reads from cold
            let below = Just (fs M.! cid)
            b1 <- txlogDigest quietLogger fxVersion sq cid HistorySqlite below
            b2 <- txlogDigest quietLogger fxVersion sp cid HistorySplit below
            _txdAll b2 @?= _txdAll b1
            assertBool "fewer rows below the frontier" $
                sum (map _tdRows (M.elems (_txdTables b1))) < sum (map _tdRows (M.elems (_txdTables d1)))
        beforeSwap <- txlogDigest quietLogger fxVersion sp cid0 HistorySplit Nothing
        -- swap two key ids of a T list of chain 0
        swapped <- withColdStore (sp </> coldDirName) ColdReadWrite 8_388_608 $ \cs -> do
            ks <- coldRawKeys cs
            let tKeys = [ k | k <- ks, B.take 2 k == B.pack [0x54, chainByte cid0] ]
            vals <- forM tKeys $ \k -> (,) k <$> coldRawGet cs k
            case find (\(_, v) -> maybe False ((>= 2) . length . nub) (v >>= decodeKeyIdList)) vals of
                Just (k, Just v) | Just (x : y : rest) <- decodeKeyIdList v, x /= y -> do
                    R.put (_rocksDbHandle (coldStoreRocksDb cs)) R.defaultWriteOptions k (encodeKeyIdList (y : x : rest))
                    return True
                _ -> return False
        assertBool "a T list with two rows" swapped
        afterSwap <- txlogDigest quietLogger fxVersion sp cid0 HistorySplit Nothing
        assertBool "the digest changed" (_txdAll afterSwap /= _txdAll beforeSwap)

    , testCase "the in-process side never writes the cold store (directory listing and meta unchanged)" $ do
        tw <- getTw
        fs <- readFrontierHeights (_twSplit tw) fxChains
        withTwinSides tw "cold-ro" $ \_ b sa sb -> do
            -- taken while the side has the cold store open: it opened it
            -- read-only and wrote nothing
            beforeList <- listing (b </> coldDirName)
            beforeMeta <- meta b
            assertBool "meta present" (all isJust beforeMeta)
            api <- oracleApi sa
            reqs <- concat <$> mapM (\(t, seeds) -> generated api seeds t)
                [ ((ClsHistory, cid0, 8), [1 .. 4]), ((ClsLocal, cid0, 8), [1 .. 4]), ((ClsHeader, cid0, fs M.! cid0), [1]) ]
            rep <- runCompareClasses quietLogger defaultRunOptions (const fullCoverage) sa sb reqs
            reportDiffCount rep @?= 0
            afterList <- listing (b </> coldDirName)
            afterList @?= beforeList
            afterMeta <- meta b
            afterMeta @?= beforeMeta
        -- and after the side closed
        b' <- copyTwin tw _twSplit "cold-ro-closed"
        l0 <- listing (b' </> coldDirName)
        m0 <- meta b'
        withInProcessSide quietLogger (splitSpec b') $ \_ -> return ()
        l1 <- listing (b' </> coldDirName)
        m1 <- meta b'
        l1 @?= l0
        m1 @?= m0
    ]
