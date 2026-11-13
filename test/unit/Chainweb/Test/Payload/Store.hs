{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Test.Payload.Store
-- License: MIT
-- Stability: experimental
--
-- Tests of the block payload store: the bytes that 'addPayload' writes, the
-- optional Merkle-tree writes and the single batch per payload.
--
module Chainweb.Test.Payload.Store
( tests
) where

import Control.Exception (SomeException, try)
import Control.Monad (forM_)

import Data.Aeson (Value(..), eitherDecode, encode, toJSON)
import qualified Data.Aeson.KeyMap as KM
import Data.Maybe (isNothing)
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Vector as V

import qualified Database.RocksDB.Iterator as I
import qualified Database.RocksDB.ReadOptions as R

import Options.Applicative
    (ParserResult(..), defaultPrefs, execParserPure, getParseResult, info, renderFailure)

import System.Directory (listDirectory)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)

import Test.Tasty
import Test.Tasty.HUnit

-- internal modules

import Chainweb.BlockHeight
import Chainweb.Chainweb.Configuration
import Chainweb.MerkleUniverse
import Chainweb.Payload
import Chainweb.Payload.PayloadStore
import Chainweb.Payload.PayloadStore.RocksDB
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning
import Chainweb.Storage.Table
import Chainweb.Test.Utils (golden)
import Chainweb.Version.Mainnet

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = testGroup "Payload store"
    [ golden "payload-store-six-entries" sixEntries
    , testCase "trees off: payload, index, transactions, outputs present; both tree tables Nothing"
        treesOffTest
    , testCase "addPayload is all-or-nothing" allOrNothingTest
    , testCase "storeMerkleTrees: default True; --store-merkle-trees true and false parse, any other value fails; JSON round trip"
        storeMerkleTreesConfigTest
    , testCase "zstd-dict: payloads added then flushed live only in ZSTD SSTs with level 3, max_dict_bytes 65536, zstd_max_train_bytes 6553600, 32 KB blocks"
        zstdDictFilesTest
    , testCase "mixed files: Snappy half, zstd half, reopened under each setting: every payload byte-equal"
        mixedFilesTest
    , testCase "--payload-compression parses snappy and zstd-dict; any other value fails naming both"
        payloadCompressionOptionTest
    ]

-- -------------------------------------------------------------------------- --
-- Fixed payload

fixedHeight :: BlockHeight
fixedHeight = 7

fixedTxs :: BlockTransactions_ ChainwebMerkleHashAlgorithm
fixedTxTree :: TransactionTree_ ChainwebMerkleHashAlgorithm
(fixedTxTree, fixedTxs) = newBlockTransactions
    (MinerData "fixed-miner")
    (V.fromList [Transaction "fixed-tx-0", Transaction "fixed-tx-1"])

fixedOuts :: BlockOutputs_ ChainwebMerkleHashAlgorithm
fixedOutTree :: OutputTree_ ChainwebMerkleHashAlgorithm
(fixedOutTree, fixedOuts) = newBlockOutputs
    (CoinbaseOutput "fixed-coinbase")
    (V.fromList [TransactionOutput "fixed-out-0", TransactionOutput "fixed-out-1"])

addFixedPayload :: PayloadDb RocksDbTable -> IO ()
addFixedPayload pdb =
    addPayload pdb fixedHeight fixedTxs fixedTxTree fixedOuts fixedOutTree

-- -------------------------------------------------------------------------- --
-- Raw entries

-- | All entries of the tables of the database namespace, in key order, as
-- written to disk.
--
rawEntries :: RocksDb -> IO [(B.ByteString, B.ByteString)]
rawEntries rdb = R.withReadOptions mempty $ \opts ->
    I.withIter (_rocksDbHandle rdb) opts $ \it -> do
        I.iterFirst it
        let go acc = I.iterEntry it >>= \case
                Nothing -> return (reverse acc)
                Just e -> I.iterNext it >> go (e : acc)
        filter (B.isPrefixOf prefix . fst) <$> go []
  where
    prefix = _rocksDbNamespace rdb <> "-"

-- | One line per entry, key and value in hex.
--
showEntries :: [(B.ByteString, B.ByteString)] -> BL.ByteString
showEntries es = BL.fromStrict $ B8.unlines
    [ B16.encode k <> " " <> B16.encode v | (k, v) <- es ]

-- | The six entries that 'addPayload' writes with the default configuration.
-- The expected file was created from chainweb-node 3.2.1.
--
sixEntries :: IO BL.ByteString
sixEntries = withTempRocksDb "payload-store-golden" $ \rdb -> do
    addFixedPayload (newPayloadDb rdb)
    showEntries <$> rawEntries rdb

-- -------------------------------------------------------------------------- --
-- Merkle trees and batches

treesOffTest :: Assertion
treesOffTest = withTempRocksDb "payload-store-trees-off" $ \rdb -> do
    let pdb = newPayloadDbWith (PayloadDbConfig DoNotStoreMerkleTrees) rdb
        tdb = _transactionDb pdb
        cache = _payloadCache pdb
        payload = blockPayload fixedTxs fixedOuts
    addFixedPayload pdb

    p <- tableLookup (_newTransactionDbBlockPayloadsTbl tdb) (fixedHeight, casKey payload)
    assertBool "payload missing" (p == Just payload)
    h <- tableLookup (_transactionDbBlockPayloadHeightsTbl tdb) (casKey payload)
    h @?= Just fixedHeight
    txs <- tableLookup (_newTransactionDbBlockTransactionsTbl tdb) (fixedHeight, casKey fixedTxs)
    assertBool "transactions missing" (txs == Just fixedTxs)
    outs <- tableLookup (_newBlockOutputsTbl $ _payloadCacheBlockOutputs cache) (fixedHeight, casKey fixedOuts)
    assertBool "outputs missing" (outs == Just fixedOuts)

    txTree <- tableLookup (_newTransactionTreeStoreTbl $ _payloadCacheTransactionTrees cache) (fixedHeight, casKey fixedTxTree)
    assertBool "transaction tree stored" (isNothing txTree)
    outTree <- tableLookup (_newOutputTreeStoreTbl $ _payloadCacheOutputTrees cache) (fixedHeight, casKey fixedOutTree)
    assertBool "output tree stored" (isNothing outTree)

    es <- rawEntries rdb
    length es @?= 4

-- | The outputs table is taken from a second RocksDB instance, which
-- 'updateBatch' rejects. Nothing of the payload may be written to either
-- instance.
--
allOrNothingTest :: Assertion
allOrNothingTest =
    withTempRocksDb "payload-store-batch-a" $ \rdbA ->
    withTempRocksDb "payload-store-batch-b" $ \rdbB -> do
        let pdbA = newPayloadDb rdbA
            pdbB = newPayloadDb rdbB
            pdb = pdbA
                { _payloadCache = (_payloadCache pdbA)
                    { _payloadCacheBlockOutputs = _payloadCacheBlockOutputs (_payloadCache pdbB)
                    }
                }
        r <- try @SomeException $ addFixedPayload pdb
        case r of
            Left _ -> return ()
            Right () -> assertFailure "addPayload did not throw"
        esA <- rawEntries rdbA
        assertBool ("entries in the first instance: " <> show esA) (null esA)
        esB <- rawEntries rdbB
        assertBool ("entries in the second instance: " <> show esB) (null esB)

-- -------------------------------------------------------------------------- --
-- Configuration

parseArgs :: [String] -> Maybe (ChainwebConfiguration -> ChainwebConfiguration)
parseArgs = getParseResult
    . execParserPure defaultPrefs (info pChainwebConfiguration mempty)

storeMerkleTreesConfigTest :: Assertion
storeMerkleTreesConfigTest = do
    let def = defaultChainwebConfiguration Mainnet01
    assertBool "default is not True" (_configStoreMerkleTrees def)

    let parsed args c = _configStoreMerkleTrees . ($ c) <$> parseArgs args
    parsed ["--store-merkle-trees", "true"] def { _configStoreMerkleTrees = False }
        @?= Just True
    parsed ["--store-merkle-trees", "false"] def @?= Just False
    parsed [] def @?= Just True
    forM_ [["--store-merkle-trees"], ["--store-merkle-trees", "yes"], ["--store-merkle-trees", "TRUE"], ["--store-merkle-trees", "1"], ["--store-merkle-trees", ""]] $ \args ->
        assertBool ("parsed: " <> show args) (null (parsed args def))

    forM_ [True, False] $ \b -> do
        let c = def { _configStoreMerkleTrees = b }
        case toJSON c of
            Object o -> KM.lookup "storeMerkleTrees" o @?= Just (Bool b)
            v -> assertFailure $ "not an object: " <> show v
        (_configStoreMerkleTrees <$> eitherDecode (encode c)) @?= Right b

-- -------------------------------------------------------------------------- --
-- Payload compression

-- | Payload @i@ at height @i@, with ten transactions and ten outputs that
-- differ per payload.
--
addIndexedPayloads :: PayloadDb RocksDbTable -> [Int] -> IO ()
addIndexedPayloads pdb is = forM_ is $ \i -> do
    let item n j = B8.pack $ "{\"" <> n <> "\":" <> show i <> ",\"j\":" <> show j
            <> ",\"data\":\"" <> concat (replicate 20 (show (i * j))) <> "\"}"
        (txTree, txs) = newBlockTransactions
            (MinerData $ item "miner" (0 :: Int))
            (V.fromList [Transaction (item "tx" j) | j <- [0 .. 9 :: Int]])
        (outTree, outs) = newBlockOutputs
            (CoinbaseOutput $ item "coinbase" (0 :: Int))
            (V.fromList [TransactionOutput (item "out" j) | j <- [0 .. 9 :: Int]])
    addPayload pdb (fromIntegral i) txs txTree outs outTree

withPayloadRocksDb :: FilePath -> PayloadCompression -> (RocksDb -> IO a) -> IO a
withPayloadRocksDb dir c = withRocksDbTuned dir modernDefaultOptions (payloadCompressionTuning c)

zstdDictFilesTest :: Assertion
zstdDictFilesTest = withSystemTempDirectory "payload-store-zstd" $ \dir -> do
    withPayloadRocksDb dir PayloadZstdDict $ \rdb -> do
        addIndexedPayloads (newPayloadDb rdb) [0 .. 199]
        flushRocksDb rdb
    info' <- sstCompressionInfo dir
    assertBool "no SST files" (not (M.null info'))
    forM_ (M.toList info') $ \(f, (c, o)) -> do
        assertEqual ("compression of " <> f) "ZSTD" c
        forM_ ["level=3;", "max_dict_bytes=65536;", "zstd_max_train_bytes=6553600;"] $ \opt ->
            assertBool ("compression options of " <> f <> " have " <> B8.unpack opt <> ": " <> show o)
                (opt `B.isInfixOf` o)
    -- the block size is not a property of an SST file; RocksDB records the
    -- table options that it writes the files with in its OPTIONS file
    opts <- latestOptionsFile dir
    assertBool ("block_size=32768 in the OPTIONS file:\n" <> B8.unpack opts)
        ("\n  block_size=32768\n" `B.isInfixOf` opts)

-- | The newest OPTIONS file of a closed database.
--
latestOptionsFile :: FilePath -> IO B.ByteString
latestOptionsFile dir = do
    fs <- filter isOptions <$> listDirectory dir
    case L.sortOn (read @Int . drop (length prefix)) fs of
        [] -> assertFailure ("no OPTIONS file in " <> dir)
        xs -> B.readFile (dir </> last xs)
  where
    prefix = "OPTIONS-"
    isOptions f = prefix `L.isPrefixOf` f && all (`elem` ['0' .. '9']) (drop (length prefix) f)

-- | Node-level twin of the engine-level @mixedCompressionReadTest@ of
-- chainweb-storage.
--
mixedFilesTest :: Assertion
mixedFilesTest = withSystemTempDirectory "payload-store-mixed" $ \dir -> do
    withPayloadRocksDb dir PayloadSnappy $ \rdb -> do
        addIndexedPayloads (newPayloadDb rdb) [0 .. 99]
        flushRocksDb rdb
    withPayloadRocksDb dir PayloadZstdDict $ \rdb -> do
        addIndexedPayloads (newPayloadDb rdb) [100 .. 199]
        flushRocksDb rdb
    info' <- sstCompressionInfo dir
    assertEqual ("compression of the SST files: " <> show info')
        ["Snappy", "ZSTD"] (L.sort $ L.nub $ fst <$> M.elems info')

    expected <- withTempRocksDb "payload-store-mixed-reference" $ \ref -> do
        addIndexedPayloads (newPayloadDb ref) [0 .. 199]
        rawEntries ref
    length expected @?= 1200
    forM_ [PayloadSnappy, PayloadZstdDict] $ \c ->
        withPayloadRocksDb dir c $ \rdb -> do
            es <- rawEntries rdb
            assertBool ("entries differ, reopened with " <> show c) (es == expected)

payloadCompressionOptionTest :: Assertion
payloadCompressionOptionTest = do
    let def = defaultChainwebConfiguration Mainnet01
    _configPayloadCompression def @?= PayloadSnappy
    assertBool "snappy tuning" (payloadCompressionTuning PayloadSnappy == defaultTuning)
    assertBool "zstd-dict tuning" (payloadCompressionTuning PayloadZstdDict == payloadZstdTuning)

    let parsed args c = _configPayloadCompression . ($ c) <$> parseArgs args
    parsed ["--payload-compression", "snappy"] def { _configPayloadCompression = PayloadZstdDict }
        @?= Just PayloadSnappy
    parsed ["--payload-compression", "zstd-dict"] def @?= Just PayloadZstdDict
    parsed [] def @?= Just PayloadSnappy
    forM_ ["zstd", "Snappy", "lz4", ""] $ \v ->
        case execParserPure defaultPrefs (info pChainwebConfiguration mempty) ["--payload-compression", v] of
            Failure f -> do
                let (msg, _) = renderFailure f "chainweb-node"
                assertBool ("message does not name snappy: " <> msg) ("snappy" `L.isInfixOf` msg)
                assertBool ("message does not name zstd-dict: " <> msg) ("zstd-dict" `L.isInfixOf` msg)
            _ -> assertFailure $ "parsed: " <> show v

    forM_ [minBound .. maxBound] $ \c -> do
        let cfg = def { _configPayloadCompression = c }
        (_configPayloadCompression <$> eitherDecode (encode cfg)) @?= Right c
