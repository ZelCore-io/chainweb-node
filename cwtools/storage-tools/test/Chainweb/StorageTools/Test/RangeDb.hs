{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.StorageTools.Test.RangeDb
--
-- Tests of the range databases and of the payload measurement. The source is a
-- 'TestBlockDb' that is served by 'serviceApiApplication' on a loopback
-- socket; nothing in these tests reaches the network.
--
module Chainweb.StorageTools.Test.RangeDb
( tests
) where

import Control.Lens (view)
import Control.Monad

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.HashMap.Strict as HM
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import Data.Word

import Network.Wai.Handler.Warp (testWithApplication)

import qualified Streaming.Prelude as SP

import System.Directory (doesDirectoryExist)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import System.LogLevel (LogLevel(..))

import Test.Tasty
import Test.Tasty.HUnit

-- internal modules

import Chainweb.BlockHeader
import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Cut.CutHashes
import Chainweb.CutDB (cutHashesTable)
import Chainweb.Graph (pairChainGraph)
import Chainweb.Logger
import Chainweb.Miner.Coordinator (MiningCoordination)
import Chainweb.Pact.RestAPI.Server (PactServerData)
import Chainweb.Payload
import Chainweb.Payload.PayloadStore
import Chainweb.Payload.PayloadStore.RocksDB
import Chainweb.Payload.RestAPI (PayloadBatchLimit(..))
import Chainweb.RestAPI
import Chainweb.Storage.Table
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning
import Chainweb.StorageTools.Http
import Chainweb.StorageTools.Measure.Payload
import Chainweb.StorageTools.RangeDb
import Chainweb.Test.Cut (offsetBlockTime)
import Chainweb.Test.Cut.TestBlockDb
import Chainweb.Test.TestVersions (barebonesTestVersion)
import Chainweb.Time (second)
import Chainweb.TreeDB (entries)
import Chainweb.Utils (T2(..), int)
import Chainweb.Version
import Chainweb.WebBlockHeaderDB (getWebBlockHeaderDb)

-- -------------------------------------------------------------------------- --
-- Source

testVer :: ChainwebVersion
testVer = barebonesTestVersion pairChainGraph

cids :: [ChainId]
cids = [unsafeChainId 0, unsafeChainId 1]

-- | The height of the source.
--
maxH :: BlockHeight
maxH = 5

silent :: GenericLogger
silent = genericLogger Error (\_ -> return ())

-- | A distinct payload for each block, with a few transactions.
--
mkPayload :: ChainId -> BlockHeight -> PayloadWithOutputs
mkPayload cid h = newPayloadWithOutputs
    (MinerData $ "miner-" <> tag)
    (CoinbaseOutput $ "coinbase-" <> tag)
    (V.fromList
        [ (Transaction ("tx-" <> tag <> B8.pack (show i)), TransactionOutput (B8.replicate (40 + 7 * i) 'x' <> tag))
        | i <- [1 .. 3]
        ])
  where
    tag = B8.pack (show cid <> "-" <> show h)

-- | Mines blocks round robin until each chain has the given height.
--
mineTo :: TestBlockDb -> BlockHeight -> IO ()
mineTo tdb target = do
    hs <- forM cids $ \cid -> (,) cid . view blockHeight <$> getParentTestBlockDb tdb cid
    unless (all ((>= target) . snd) hs) $ do
        forM_ hs $ \(cid, h) -> when (h < target) $ do
            let h' = succ h
            void $ addTestBlockDb tdb h' (Nonce (int h')) (offsetBlockTime second) cid (mkPayload cid h')
        mineTo tdb target

data Source = Source
    { _srcRocks :: !RocksDb
    , _srcDb :: !TestBlockDb
    , _srcApi :: !ServiceApi
    }

-- | Mines a source up to 'maxH' and serves it on a loopback socket with an
-- ephemeral port.
--
withSource :: (Source -> IO a) -> IO a
withSource = withSourceTo maxH

withSourceTo :: BlockHeight -> (Source -> IO a) -> IO a
withSourceTo height act = withTempRocksDb "storage-tools-source" $ \rdb -> do
    T2 tdb srcRocks <- mkTestBlockDbIO testVer rdb
    mineTo tdb height
    bhdbs <- forM cids $ \cid -> (,) cid <$> getWebBlockHeaderDb (_bdbWebBlockHeaderDb tdb) cid
    let dbs :: ChainwebServerDbs () RocksDbTable
        dbs = ChainwebServerDbs
            { _chainwebServerCutDb = Nothing
            , _chainwebServerBlockHeaderDbs = bhdbs
            , _chainwebServerMempools = []
            , _chainwebServerPayloadDbs = [(cid, _bdbPayloadDb tdb) | cid <- cids]
            , _chainwebServerPeerDbs = []
            }
        app = serviceApiApplication testVer dbs
            ([] :: [(ChainId, PactServerData GenericLogger RocksDbTable)])
            (Nothing :: Maybe (MiningCoordination GenericLogger RocksDbTable))
            (HeaderStream False)
            Nothing
            (PayloadBatchLimit 1000)
    testWithApplication (return app) $ \port -> do
        lim <- newLimiter 1_000 Nothing
        api <- newServiceApi ("http://127.0.0.1:" <> T.pack (show port)) testVer lim
        act (Source srcRocks tdb api)

-- -------------------------------------------------------------------------- --
-- Utils

diffReport :: DiffSummary -> String
diffReport = show

assertNoDiff :: String -> DiffSummary -> Assertion
assertNoDiff msg d = assertBool (msg <> ": " <> diffReport d) (diffIsEmpty d)

-- | Every entry of every payload and header namespace of the destination is in
-- the source, with the same bytes.
--
assertSubset :: Source -> RocksDb -> Assertion
assertSubset src dst = do
    d <- diffRangeDbs (_srcRocks src) dst (allNamespaces testVer)
    forM_ (_dsNamespaces d) $ \n -> do
        assertEqual (B8.unpack (_ndNamespace n) <> ": only in the destination") 0 (_ndOnlyRight n)
        assertEqual (B8.unpack (_ndNamespace n) <> ": differing values") 0 (_ndDiffering n)

headersAtHeight :: Source -> ChainId -> BlockHeight -> IO [BlockHeader]
headersAtHeight src cid h = do
    bdb <- getWebBlockHeaderDb (_bdbWebBlockHeaderDb $ _srcDb src) cid
    entries bdb Nothing Nothing (Just $ fromIntegral h) (Just $ fromIntegral h) SP.toList_

payloadKeysOf :: Source -> [(ChainId, BlockHeight)] -> IO (S.Set (BlockHeight, BlockPayloadHash))
payloadKeysOf src targets = fmap (S.fromList . concat) $ forM targets $ \(cid, h) -> do
    hdrs <- headersAtHeight src cid h
    return [(h, view blockPayloadHash hdr) | hdr <- hdrs]

namespaceSize :: RocksDb -> B.ByteString -> IO Int
namespaceSize db ns = length <$> readNamespace db ns

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = testGroup "Range DB"
    [ testCase "range DB equals the source on header and payload namespaces" $
        withSource $ \src -> withTempRocksDb "storage-tools-range" $ \dst -> do
            s <- buildRangeDb silent (_srcApi src) (RangeSpec maxH) dst
            d <- diffRangeDbs (_srcRocks src) dst (allNamespaces testVer)
            assertNoDiff "range DB differs from the source" d
            -- the comparison is not vacuous
            forM_ (_dsNamespaces d) $ \n ->
                assertBool (B8.unpack (_ndNamespace n) <> " is empty") (_ndLeft n > 0)
            -- 2 chains, heights 0 .. maxH
            assertEqual "headers" (2 * (int maxH + 1)) (_rsHeaders s)
            assertEqual "payloads" (2 * (int maxH + 1)) (_rsPayloads s)
            assertEqual "cuts" 1 (_rsCuts s)

    , testCase "range DB has one cut with every chain at the max height" $
        withSource $ \src -> withTempRocksDb "storage-tools-range" $ \dst -> do
            _ <- buildRangeDb silent (_srcApi src) (RangeSpec maxH) dst
            n <- namespaceSize dst "CutHashes"
            assertEqual "number of cuts" 1 n
            ch <- withTableIterator (unCasify $ cutHashesTable dst) $ \it -> do
                iterLast it
                iterValue it
            case ch of
                Nothing -> assertFailure "no cut"
                Just c -> do
                    assertEqual "chains of the cut" (S.fromList cids) (S.fromList $ HM.keys $ _cutHashes c)
                    forM_ (HM.toList $ _cutHashes c) $ \(cid, bh) -> do
                        assertEqual ("height of chain " <> show cid) maxH (_bhwhHeight bh)
                        hdrs <- headersAtHeight src cid maxH
                        assertEqual ("hash of chain " <> show cid) (map (view blockHash) hdrs) [_bhwhHash bh]

    , testCase "range DB of a source with several header pages and payload batches equals the source" $
        -- 2 * 1100 headers: 4 header pages per chain; 1101 payloads per chain: 2 batches
        withSourceTo 1_100 $ \src -> withTempRocksDb "storage-tools-range" $ \dst -> do
            top <- probeMaxHeight (_srcApi src) (unsafeChainId 1)
            assertEqual "probeMaxHeight" 1_100 top
            s <- buildRangeDb silent (_srcApi src) (RangeSpec 1_100) dst
            assertEqual "headers" (2 * 1_101) (_rsHeaders s)
            assertEqual "payloads" (2 * 1_101) (_rsPayloads s)
            d <- diffRangeDbs (_srcRocks src) dst (allNamespaces testVer)
            assertNoDiff "range DB differs from the source" d

    , testCase "rangeDbInfo prints the highest cut per chain" $
        withSource $ \src -> withSystemTempDirectory "storage-tools-info" $ \dir -> do
            let dbPath = dir </> "range-db"
            -- no flush: rangeDbInfo applies the write ahead log
            withRocksDb dbPath modernDefaultOptions $ \dst ->
                void $ buildRangeDb silent (_srcApi src) (RangeSpec maxH) dst
            info <- rangeDbInfo dbPath
            let ls = T.lines info
            assertBool ("highest cut in\n" <> T.unpack info) (any (T.isPrefixOf "highest cut: height ") ls)
            forM_ cids $ \cid -> do
                [hdr] <- headersAtHeight src cid maxH
                assertBool ("chain " <> show cid <> " in\n" <> T.unpack info) $
                    any (T.isInfixOf ("chain " <> chainIdToText cid <> ": height " <> T.pack (show maxH)
                        <> ", hash " <> T.pack (show $ view blockHash hdr))) ls
            assertBool ("BlockPayload2 in\n" <> T.unpack info)
                ("  BlockPayload2: " <> T.pack (show (2 * int maxH + 1 :: Int)) `elem` ls)

    , testCase "corruptEntry makes diffRangeDbs report exactly that key" $
        withSource $ \src -> withTempRocksDb "storage-tools-range" $ \dst -> do
            _ <- buildRangeDb silent (_srcApi src) (RangeSpec maxH) dst
            let ns = "BlockPayload2"
            entriesBefore <- readNamespace dst ns
            let n = 3
            corruptEntry dst ns n
            d <- diffRangeDbs (_srcRocks src) dst (allNamespaces testVer)
            assertEqual "differing keys of all namespaces"
                [(ns, [fst (entriesBefore !! n)])]
                [ (_ndNamespace nd, _ndKeys nd) | nd <- _dsNamespaces d, not (null (_ndKeys nd)) ]
            assertEqual "number of differing entries" 1 (sum $ _ndDiffering <$> _dsNamespaces d)
            assertEqual "no other differences" 0
                (sum $ (\nd -> _ndOnlyLeft nd + _ndOnlyRight nd) <$> _dsNamespaces d)
            -- the corrupted value differs in the last byte only
            entriesAfter <- readNamespace dst ns
            let (_, vb) = entriesBefore !! n
                (_, va) = entriesAfter !! n
            assertEqual "length of the value" (B.length vb) (B.length va)
            assertEqual "all but the last byte" (B.init vb) (B.init va)
            assertBool "last byte" (B.last vb /= B.last va)

    -- before Task 17 runs them against production (ruling 4.13)
    , testCase "sampleRangeDb seed 1: payloads of exactly the drawn (chain, height) targets, equal to the source; no cut" $
        withSource $ \src -> withTempRocksDb "storage-tools-sample" $ \dst -> do
            let count = 5
                targets = sampleTargets testVer 1 count maxH
            assertEqual "number of drawn targets" count (length targets)
            assertEqual "targets are distinct" count (S.size $ S.fromList targets)
            assertBool "targets are within the history"
                (all (\(c, h) -> c `elem` cids && h <= maxH) targets)
            s <- sampleRangeDb silent (_srcApi src) 1 count dst
            assertEqual "no skipped targets" 0 (_rsSkipped s)
            expected <- payloadKeysOf src targets
            assertEqual "number of payloads in the summary" count (_rsPayloads s)
            actualN <- namespaceSize dst "BlockPayload2"
            assertEqual "payloads in BlockPayload2" (S.size expected) actualN
            let srcPdb = _bdbPayloadDb (_srcDb src)
                dstPdb = newPayloadDb dst
            forM_ (S.toList expected) $ \(h, ph) -> do
                a <- lookupPayloadWithHeight srcPdb (Just h) ph
                b <- lookupPayloadWithHeight dstPdb (Just h) ph
                assertBool ("payload at height " <> show h <> " is in the source") (a /= Nothing)
                assertEqual ("payload at height " <> show h) a b
            assertSubset src dst
            assertEqual "no cut" 0 =<< namespaceSize dst "CutHashes"
            forM_ cids $ \cid ->
                assertEqual ("no headers of chain " <> show cid) 0
                    =<< namespaceSize dst ("BlockHeader/" <> B8.pack (show (chainIdInt cid :: Int)) <> "/header")

    , testCase "sampleTargets is deterministic in the seed" $ do
            let t1 = sampleTargets testVer 1 5 maxH
            assertEqual "same seed" t1 (sampleTargets testVer 1 5 maxH)
            assertBool "other seed" (t1 /= sampleTargets testVer 2 5 maxH)
            assertEqual "more targets than slots"
                (2 * (int maxH + 1)) (length $ sampleTargets testVer 1 1_000 maxH)

    , testCase "contiguousRangeDb c a b: payloads of chain c at heights a..b only, equal to the source" $
        withSource $ \src -> withTempRocksDb "storage-tools-contiguous" $ \dst -> do
            let cid = unsafeChainId 1
            s <- contiguousRangeDb silent (_srcApi src) cid 2 4 dst
            expected <- payloadKeysOf src [(cid, h) | h <- [2 .. 4]]
            assertEqual "three payloads" 3 (S.size expected)
            assertEqual "payloads in the summary" 3 (_rsPayloads s)
            assertEqual "payloads in BlockPayload2" 3 =<< namespaceSize dst "BlockPayload2"
            let srcPdb = _bdbPayloadDb (_srcDb src)
                dstPdb = newPayloadDb dst
            forM_ (S.toList expected) $ \(h, ph) -> do
                a <- lookupPayloadWithHeight srcPdb (Just h) ph
                b <- lookupPayloadWithHeight dstPdb (Just h) ph
                assertBool ("payload at height " <> show h <> " is in the source") (a /= Nothing)
                assertEqual ("payload at height " <> show h) a b
            assertSubset src dst
            assertEqual "no cut" 0 =<< namespaceSize dst "CutHashes"

    , testCase "measurePayloadSizes: logical bytes equal the source's key+value bytes; SST bytes > 0 under both tunings" $
        withSource $ \src -> withSystemTempDirectory "storage-tools-measure" $ \dir -> do
            let dbPath = dir </> "range-db"
                workPath = dir </> "work"
            withRocksDb dbPath modernDefaultOptions $ \dst -> do
                _ <- buildRangeDb silent (_srcApi src) (RangeSpec maxH) dst
                flushRocksDb dst
            sizes <- measurePayloadSizes dbPath workPath
            let reported = _psNamespaces sizes
            assertBool "namespaces are reported" (not $ null reported)
            -- all non-empty namespaces are reported, and only those
            nonEmpty <- withReadOnlyRocksDb dbPath modernDefaultOptions $ \db ->
                fmap concat $ forM (allNamespaces testVer <> metadataNamespaces) $ \ns -> do
                    es <- readNamespace db ns
                    return [ns | not (null es)]
            assertEqual "reported namespaces" (S.fromList nonEmpty) (S.fromList $ _nsName <$> reported)
            withReadOnlyRocksDb dbPath modernDefaultOptions $ \db ->
                forM_ reported $ \r -> do
                    es <- readNamespace db (_nsName r)
                    let ns = B8.unpack (_nsName r)
                    assertEqual (ns <> ": entries") (length es) (_nsEntries r)
                    assertEqual (ns <> ": logical bytes")
                        (sum [fromIntegral (B.length k + B.length v) | (k, v) <- es] :: Word64)
                        (_nsLogicalBytes r)
                    forM_ [("default", _nsDefault r), ("zstd", _nsZstd r)] $ \(name, t) -> do
                        assertBool (ns <> ": SST bytes after flush under " <> name) (_tsFlushedBytes t > 0)
                        assertBool (ns <> ": SST bytes after compaction under " <> name) (_tsCompactedBytes t > 0)
            -- the tunings took effect on the written files
            forM_ reported $ \r -> do
                assertBool "default tuning writes Snappy" (all (== "Snappy") (_tsCompression $ _nsDefault r))
                assertBool "zstd tuning writes ZSTD" (all (== "ZSTD") (_tsCompression $ _nsZstd r))
                assertBool "compression is reported" (not $ null $ _tsCompression $ _nsZstd r)
            -- the work copies are removed
            gone <- not <$> doesDirectoryExist workPath
            assertBool "work directory is removed" gone

            -- reads
            lat <- measurePayloadReads dbPath defaultTuning 7 1
            assertEqual "number of reads" 7 (_latCount lat)
            assertBool "percentiles are ordered"
                (_latP50Ns lat <= _latP95Ns lat && _latP95Ns lat <= _latP99Ns lat)
            lat' <- measurePayloadReads dbPath payloadZstdTuning 1_000 1
            assertEqual "all payloads" (2 * int maxH + 1) (_latCount lat')
    ]
