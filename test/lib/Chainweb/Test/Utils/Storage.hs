{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Test.Utils.Storage
-- License: MIT
-- Stability: experimental
--
-- Storage options for the tests, from the environment variable
-- @CHAINWEB_TEST_STORAGE@, a comma separated list of:
--
-- * @no-merkle-trees@: payload stores do not write the Merkle trees.
-- * @zstd-dict@: RocksDB instances are opened with the tuning of
--   @--payload-compression zstd-dict@.
-- * @pact-split@: the Pact history store is @split@.
-- * @pact-shadow@: the Pact history store is @shadow@.
-- * @freeze-depth=N@: the freeze depth is @N@ (at least 1).
--
-- Unset or empty, the tests use the defaults of chainweb-node 3.2.1. An
-- unknown item is an error, so that a typo does not silently test the
-- defaults.
--
module Chainweb.Test.Utils.Storage
( TestStorageOption(..)
, testStorageOptions
, parseTestStorageOptions
, testPayloadDbConfig
, testStorageTuning
, testPactHistoryConfig
, testPactHistoryConfigFor
) where

import qualified Data.Text as T
import Data.Word (Word64)

import System.Environment (lookupEnv)

import Text.Read (readMaybe)

-- internal modules

import Chainweb.BlockHeight (BlockHeight(..))
import Chainweb.Chainweb.Configuration (PayloadCompression(..), payloadCompressionTuning)
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Payload.PayloadStore
import Chainweb.Storage.Table.RocksDB.Tuning (RocksDbTuning)

-- -------------------------------------------------------------------------- --
-- Test Storage Options

data TestStorageOption
    = TestNoMerkleTrees
    | TestZstdDict
    | TestPactSplit
    | TestPactShadow
    | TestFreezeDepth BlockHeight
    deriving (Show, Eq)

testStorageOptions :: IO [TestStorageOption]
testStorageOptions = lookupEnv "CHAINWEB_TEST_STORAGE" >>= \case
    Nothing -> return []
    Just s -> either (ioError . userError) return
        $ parseTestStorageOptions (T.pack s)

-- | Parses the value of @CHAINWEB_TEST_STORAGE@. An unknown item, a freeze
-- depth that is not a whole number of at least 1, and both @pact-split@ and
-- @pact-shadow@ are errors.
--
parseTestStorageOptions :: T.Text -> Either String [TestStorageOption]
parseTestStorageOptions s = do
    opts <- traverse parseItem
        $ filter (not . T.null)
        $ T.strip <$> T.splitOn "," s
    if TestPactSplit `elem` opts && TestPactShadow `elem` opts
    then Left "CHAINWEB_TEST_STORAGE: pact-split and pact-shadow exclude each other"
    else return opts
  where
    parseItem = \case
        "no-merkle-trees" -> Right TestNoMerkleTrees
        "zstd-dict" -> Right TestZstdDict
        "pact-split" -> Right TestPactSplit
        "pact-shadow" -> Right TestPactShadow
        x | Just n <- T.stripPrefix "freeze-depth=" x -> case readMaybe @Word64 (T.unpack n) of
            Just d | d >= 1 -> Right (TestFreezeDepth (BlockHeight d))
            _ -> Left $ "CHAINWEB_TEST_STORAGE: freeze-depth must be a whole number of at least 1, got "
                <> show n
        x -> Left $ "CHAINWEB_TEST_STORAGE: unknown item "
            <> show x <> "; known items: no-merkle-trees, zstd-dict, pact-split, pact-shadow, freeze-depth=N"

-- | The payload store configuration for the tests.
--
testPayloadDbConfig :: IO PayloadDbConfig
testPayloadDbConfig = do
    opts <- testStorageOptions
    return $ PayloadDbConfig
        { _payloadDbConfigStoreTrees = if TestNoMerkleTrees `elem` opts
            then DoNotStoreMerkleTrees
            else StoreMerkleTrees
        }

-- | The RocksDB tuning for the tests, to be applied before a database is
-- opened: that of @--payload-compression zstd-dict@ with @zstd-dict@,
-- otherwise that of the default @snappy@, which changes no option.
--
testStorageTuning :: IO RocksDbTuning
testStorageTuning = do
    opts <- testStorageOptions
    return $ payloadCompressionTuning $ if TestZstdDict `elem` opts
        then PayloadZstdDict
        else PayloadSnappy

-- | The Pact history configuration for the tests: 'defaultPactHistoryConfig'
-- with the store and the freeze depth of @CHAINWEB_TEST_STORAGE@.
--
testPactHistoryConfig :: IO PactHistoryConfig
testPactHistoryConfig = testPactHistoryConfigFor <$> testStorageOptions

testPactHistoryConfigFor :: [TestStorageOption] -> PactHistoryConfig
testPactHistoryConfigFor = foldl apply defaultPactHistoryConfig
  where
    apply c = \case
        TestPactSplit -> c { _phStore = HistorySplit }
        TestPactShadow -> c { _phStore = HistoryShadow }
        TestFreezeDepth d -> c { _phFreezeDepth = d }
        _ -> c
