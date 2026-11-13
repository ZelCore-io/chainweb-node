{-# LANGUAGE BangPatterns #-}
-- |
-- Module: Main
-- Copyright: Copyright © 2024 Kadena LLC.
-- License: MIT
-- Maintainer: Lars Kuhtz <lars@kadena.io>
-- Stability: experimental
--
-- Tests for expensive data maintenance operations, most notably compaction.
--
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Main
( main
) where

import Control.Monad
import Data.Foldable (toList)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.Vector as V
import qualified Options.Applicative as O

import Chainweb.BlockHeader (Nonce(..))
import Chainweb.BlockHeight
import Chainweb.Graph
import Chainweb.Logger (GenericLogger, genericLogger)
import qualified Chainweb.Pact.Backend.Compaction as Compact
import Chainweb.Payload
import Chainweb.Payload.PayloadStore
import Chainweb.Storage.Table
import Chainweb.Storage.Table.RocksDB
import Chainweb.Test.Cut (offsetBlockTime)
import Chainweb.Test.Cut.TestBlockDb
import Chainweb.Test.TestVersions
import Chainweb.Test.Utils
import Chainweb.Time (second)
import Chainweb.Utils (T2(..), int)
import Chainweb.Version
import System.IO.Temp
import System.LogLevel
import Test.Tasty
import Test.Tasty.HUnit
import qualified Chainweb.Test.MultiNode

main :: IO ()
main = defaultMain suite

loglevel :: LogLevel
loglevel = Warn

-- note that because these tests run in parallel they must all use distinct rocksdb and sqlite dirs.
suite :: TestTree
suite = independentSequentialTestGroup "CompactionTests"
    [ testCaseSteps "compact-resume" $ \step ->
        withTempRocksDb "compact-resume-test-rocks-src" $ \srcRocksDb ->
        withTempRocksDb "compact-resume-test-rocks-target" $ \targetRocksDb ->
        withSystemTempDirectory "compact-resume-test-pact-src" $ \srcPactDbDir ->
        withSystemTempDirectory "compact-resume-test-pact-target" $ \targetPactDbDir -> do
        Chainweb.Test.MultiNode.compactAndResumeTest loglevel (fastForkingCpmTestVersion pairChainGraph) 6 srcRocksDb targetRocksDb srcPactDbDir targetPactDbDir step
    , testCaseSteps "compact-live-node" $ \step ->
        withTempRocksDb "compact-live-test-rocks" $ \rdb ->
        withSystemTempDirectory "compact-live-test-pact-src" $ \srcPactDbDir ->
        withSystemTempDirectory "compact-live-test-pact-target" $ \targetPactDbDir -> do
        Chainweb.Test.MultiNode.compactLiveNodeTest loglevel (fastForkingCpmTestVersion twentyChainGraph) 1 rdb srcPactDbDir targetPactDbDir step
    , testCase "compact with --store-merkle-trees false leaves no tree entries" compactNoTreesTest
    ]

-- -------------------------------------------------------------------------- --
-- Merkle trees in the compacted RocksDB

treeNamespaces :: [B.ByteString]
treeNamespaces = ["TransactionTree", "TransactionTree2", "OutputTree", "OutputTree2"]

namespaceSize :: RocksDb -> B.ByteString -> IO Int
namespaceSize db ns = withTableIterator tbl $ \it ->
    let go !n = iterEntry it >>= \case
            Nothing -> return n
            Just _ -> iterNext it >> go (n + 1)
    in go 0
  where
    tbl :: RocksDbTable B.ByteString B.ByteString
    tbl = newTable db (Codec id return) (Codec id return) [ns]

compactNoTreesTest :: Assertion
compactNoTreesTest = do
    -- the flag of the compact tool
    let parse args = O.getParseResult $ O.execParserPure O.defaultPrefs Compact.configParserInfo
            (["--from", "src", "--to", "dst", "--log-dir", "logs"] <> args)
        storeTrees = fmap (_payloadDbConfigStoreTrees . Compact.compactPayloadDbConfig) . parse
    storeTrees [] @?= Just StoreMerkleTrees
    storeTrees ["--store-merkle-trees", "true"] @?= Just StoreMerkleTrees
    storeTrees ["--store-merkle-trees", "false"] @?= Just DoNotStoreMerkleTrees
    forM_ [["--store-merkle-trees"], ["--store-merkle-trees", "no"]] $ \args ->
        assertBool ("rejects " <> show args) (null (parse args))

    -- the copy of the payloads
    withTempRocksDb "compact-trees-src" $ \srcBase -> do
        T2 tdb src <- mkTestBlockDbIO v srcBase
        forM_ [1 .. maxH] $ \h -> forM_ (chainIds v) $ \cid ->
            void $ addTestBlockDb tdb h (Nonce (int h)) (offsetBlockTime second) cid (payloadAt cid h)
        forM_ [StoreMerkleTrees, DoNotStoreMerkleTrees] $ \st ->
            withTempRocksDb "compact-trees-target" $ \tgt -> do
                Compact.doCompactRocksDbWith (PayloadDbConfig st) silent v (toList (chainIds v)) 0 src tgt
                payloads <- namespaceSize tgt "BlockPayload2"
                -- maxH blocks on each chain, and the genesis payload that both chains share
                assertEqual (show st <> ": payloads") (2 * int maxH + 1) payloads
                trees <- forM treeNamespaces $ \ns -> (,) ns <$> namespaceSize tgt ns
                case st of
                    StoreMerkleTrees -> forM_ ["TransactionTree2", "OutputTree2"] $ \ns ->
                        assertEqual (show st <> ": " <> B8.unpack ns) (Just payloads) (lookup ns trees)
                    DoNotStoreMerkleTrees ->
                        assertEqual (show st <> ": tree entries") [(ns, 0) | ns <- treeNamespaces] trees
  where
    v = barebonesTestVersion pairChainGraph
    maxH = 4 :: BlockHeight
    silent :: GenericLogger
    silent = genericLogger Error (\_ -> return ())
    payloadAt cid h = newPayloadWithOutputs
        (MinerData $ "miner-" <> tag)
        (CoinbaseOutput $ "coinbase-" <> tag)
        (V.fromList [(Transaction ("tx-" <> tag), TransactionOutput ("out-" <> tag))])
      where
        tag = B8.pack (show cid <> "-" <> show h)
