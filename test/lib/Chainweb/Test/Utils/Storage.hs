{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.Test.Utils.Storage
-- License: MIT
-- Stability: experimental
--
-- Storage options for the tests, from the environment variable
-- @CHAINWEB_TEST_STORAGE@, a comma separated list of:
--
-- * @no-merkle-trees@: payload stores do not write the Merkle trees.
--
-- Unset or empty, the tests use the defaults of chainweb-node 3.2.1. An
-- unknown item is an error, so that a typo does not silently test the
-- defaults.
--
module Chainweb.Test.Utils.Storage
( TestStorageOption(..)
, testStorageOptions
, testPayloadDbConfig
) where

import qualified Data.Text as T

import System.Environment (lookupEnv)

-- internal modules

import Chainweb.Payload.PayloadStore

-- -------------------------------------------------------------------------- --
-- Test Storage Options

data TestStorageOption
    = TestNoMerkleTrees
    deriving (Show, Eq)

testStorageOptions :: IO [TestStorageOption]
testStorageOptions = lookupEnv "CHAINWEB_TEST_STORAGE" >>= \case
    Nothing -> return []
    Just s -> traverse parseItem
        $ filter (not . T.null)
        $ T.strip <$> T.splitOn "," (T.pack s)
  where
    parseItem = \case
        "no-merkle-trees" -> return TestNoMerkleTrees
        x -> ioError $ userError $ "CHAINWEB_TEST_STORAGE: unknown item "
            <> show x <> "; known items: no-merkle-trees"

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
