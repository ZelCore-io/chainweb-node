{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

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

import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import qualified Data.Vector as V

import qualified Database.RocksDB.Iterator as I
import qualified Database.RocksDB.ReadOptions as R

import Test.Tasty

-- internal modules

import Chainweb.BlockHeight
import Chainweb.MerkleUniverse
import Chainweb.Payload
import Chainweb.Payload.PayloadStore
import Chainweb.Payload.PayloadStore.RocksDB
import Chainweb.Storage.Table.RocksDB
import Chainweb.Test.Utils (golden)

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = testGroup "Payload store"
    [ golden "payload-store-six-entries" sixEntries
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
