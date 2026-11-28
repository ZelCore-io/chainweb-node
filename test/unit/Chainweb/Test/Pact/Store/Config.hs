{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}

-- |
-- Module: Chainweb.Test.Pact.Store.Config
--
-- Tests for the options of the Pact history store (Task 19).
--
module Chainweb.Test.Pact.Store.Config
( tests
) where

import Control.Monad
import Control.Monad.Except (runExceptT)
import Control.Monad.Writer (runWriterT)

import Data.Aeson (Value(..), eitherDecode, eitherDecodeFileStrict, encode, toJSON)
import qualified Data.Aeson.KeyMap as KM
import qualified Data.List as L
import qualified Data.Text as T

import Options.Applicative

import Test.Tasty
import Test.Tasty.HUnit

-- internal modules

import Chainweb.Chainweb.Configuration
import Chainweb.Graph (singletonChainGraph)
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Test.TestVersions (barebonesTestVersion)
import Chainweb.Test.Utils.Storage
import Chainweb.Version
import Chainweb.Version.Development (devnet)
import Chainweb.Version.Mainnet (pattern Mainnet01, mainnet)
import Chainweb.Version.RecapDevelopment (recapDevnet)
import Chainweb.Version.Testnet04 (pattern Testnet04, testnet04)
import Chainweb.Version.Testnet06 (testnet06)

tests :: TestTree
tests = testGroup "Pact history config"
    [ testCase "defaults: sqlite, depth 4000, step 64 MiB and 1000 heights, cache 1 GiB, freezer enabled"
        defaultsTest
    , testCase "every option parses and prints back; config JSON round-trips"
        roundTripTest
    , testCase "--pact-history-store bogus, --pact-freeze-depth 0, --pact-trim-duty-cycle 0 and 1.5 are rejected with the option named"
        rejectTest
    , testCase "default node configuration JSON differs from the base's only by the pactHistory key"
        baseJsonTest
    , testCase "split and shadow: accepted on mainnet01 and the test versions, refused on testnet04, testnet06 and recap-development; sqlite accepted everywhere"
        versionTest
    , testCase "CHAINWEB_TEST_STORAGE=pact-split,freeze-depth=3 gives split with depth 3; pact-splt is an error"
        testStorageTest
    ]

parser :: ParserInfo (ChainwebConfiguration -> ChainwebConfiguration)
parser = info pChainwebConfiguration mempty

parseArgs :: [String] -> Maybe (ChainwebConfiguration -> ChainwebConfiguration)
parseArgs = getParseResult . execParserPure defaultPrefs parser

def :: ChainwebConfiguration
def = defaultChainwebConfiguration Mainnet01

defaultsTest :: Assertion
defaultsTest = do
    let c = defaultPactHistoryConfig
    _phStore c @?= HistorySqlite
    _phFreezeDepth c @?= 4000
    _phFreezeStepBytes c @?= 67108864
    _phFreezeStepHeights c @?= 1000
    _phColdBlockCacheBytes c @?= 1073741824
    _phTrimDutyCycle c @?= 0.1
    _phTrimChunkRows c @?= 1000
    _phCatchUpLagSeconds c @?= 600
    _phFreezerConcurrency c @?= 2
    _phFreezerEnabled c @?= True
    _configPactHistory def @?= defaultPactHistoryConfig
    (_configPactHistory . ($ def) <$> parseArgs []) @?= Just defaultPactHistoryConfig

roundTripTest :: Assertion
roundTripTest = do
    -- the text form of every store
    forM_ [minBound .. maxBound] $ \s -> do
        pactHistoryStoreFromText (pactHistoryStoreToText s) @?= Just s
        (_phStore . _configPactHistory . ($ def) <$> parseArgs ["--pact-history-store", T.unpack (pactHistoryStoreToText s)])
            @?= Just s
        eitherDecode (encode s) @?= Right s
    -- every command line option
    let args =
            [ "--pact-history-store", "split"
            , "--pact-freeze-depth", "12"
            , "--pact-freeze-step-bytes", "1024"
            , "--pact-freeze-step-heights", "7"
            , "--pact-cold-block-cache", "4096"
            , "--pact-trim-duty-cycle", "0.25"
            , "--pact-trim-chunk-rows", "250"
            ]
        expected = defaultPactHistoryConfig
            { _phStore = HistorySplit
            , _phFreezeDepth = 12
            , _phFreezeStepBytes = 1024
            , _phFreezeStepHeights = 7
            , _phColdBlockCacheBytes = 4096
            , _phTrimDutyCycle = 0.25
            , _phTrimChunkRows = 250
            }
    (_configPactHistory . ($ def) <$> parseArgs args) @?= Just expected
    -- the cap's upper bound itself is accepted
    (_phTrimChunkRows . _configPactHistory . ($ def) <$> parseArgs ["--pact-trim-chunk-rows", "100000"])
        @?= Just 100000
    (_configPactHistory . ($ def) <$> parseArgs ["--pact-trim-duty-cycle", "1"])
        @?= Just defaultPactHistoryConfig { _phTrimDutyCycle = 1 }
    -- JSON, alone and in the node configuration
    let other = expected { _phCatchUpLagSeconds = 30, _phFreezerConcurrency = 5 }
    forM_ [defaultPactHistoryConfig, expected, other] $ \c -> do
        eitherDecode (encode c) @?= Right c
        let cfg = def { _configPactHistory = c }
        (_configPactHistory <$> eitherDecode (encode cfg)) @?= Right c
    -- partial JSON keeps the defaults of the missing keys
    eitherDecode "{\"store\":\"shadow\",\"freezeDepth\":3}"
        @?= Right defaultPactHistoryConfig { _phStore = HistoryShadow, _phFreezeDepth = 3 }
    -- the freezer switch is not a node option
    case toJSON defaultPactHistoryConfig of
        Object o -> assertBool "freezerEnabled is written" (not $ KM.member "freezerEnabled" o)
        v -> assertFailure $ "not an object: " <> show v
    eitherDecode "{\"freezerEnabled\":false}" @?= Right defaultPactHistoryConfig

rejectTest :: Assertion
rejectTest = do
    let cases =
            [ (["--pact-history-store", "bogus"], "pact-history-store")
            , (["--pact-history-store", "Split"], "pact-history-store")
            , (["--pact-freeze-depth", "0"], "pact-freeze-depth")
            , (["--pact-freeze-depth", "-1"], "pact-freeze-depth")
            , (["--pact-freeze-step-bytes", "0"], "pact-freeze-step-bytes")
            , (["--pact-freeze-step-heights", "0"], "pact-freeze-step-heights")
            , (["--pact-cold-block-cache", "0"], "pact-cold-block-cache")
            , (["--pact-trim-duty-cycle", "0"], "pact-trim-duty-cycle")
            , (["--pact-trim-duty-cycle", "1.5"], "pact-trim-duty-cycle")
            , (["--pact-trim-duty-cycle", "-0.1"], "pact-trim-duty-cycle")
            , (["--pact-trim-chunk-rows", "0"], "pact-trim-chunk-rows")
            , (["--pact-trim-chunk-rows", "100001"], "pact-trim-chunk-rows")
            ]
    forM_ cases $ \(args, opt) ->
        case execParserPure defaultPrefs parser args of
            Failure f -> do
                let (msg, _) = renderFailure f "chainweb-node"
                assertBool ("message does not name " <> opt <> ": " <> msg) (opt `L.isInfixOf` msg)
            _ -> assertFailure $ "parsed: " <> show args

    -- the same values from a configuration file fail validation
    let bad =
            [ (defaultPactHistoryConfig { _phFreezeDepth = 0 }, "freezeDepth")
            , (defaultPactHistoryConfig { _phTrimDutyCycle = 0 }, "trimDutyCycle")
            , (defaultPactHistoryConfig { _phTrimDutyCycle = 1.5 }, "trimDutyCycle")
            , (defaultPactHistoryConfig { _phFreezeStepBytes = 0 }, "freezeStepBytes")
            , (defaultPactHistoryConfig { _phFreezeStepHeights = 0 }, "freezeStepHeights")
            , (defaultPactHistoryConfig { _phTrimChunkRows = 0 }, "trimChunkRows")
            , (defaultPactHistoryConfig { _phTrimChunkRows = 100001 }, "trimChunkRows")
            , (defaultPactHistoryConfig { _phColdBlockCacheBytes = 0 }, "coldBlockCacheBytes")
            , (defaultPactHistoryConfig { _phFreezerConcurrency = 0 }, "freezerConcurrency")
            ]
    forM_ bad $ \(c, key) -> do
        r <- validate c
        case r of
            Left e -> assertBool ("message does not name " <> T.unpack key <> ": " <> T.unpack e) (key `T.isInfixOf` e)
            Right _ -> assertFailure $ "accepted: " <> show c
    -- the defaults pass without a warning
    validate defaultPactHistoryConfig >>= (@?= Right [])
    -- a depth below the startup rewind limit is accepted with a warning
    r <- validate defaultPactHistoryConfig { _phStore = HistorySplit, _phFreezeDepth = 3 }
    case r of
        Right [w] -> assertBool ("warning: " <> T.unpack w) ("refused" `T.isInfixOf` w)
        _ -> assertFailure $ "expected one warning: " <> show r
    validate defaultPactHistoryConfig { _phStore = HistorySplit, _phFreezeDepth = 1000 } >>= (@?= Right [])
    -- the node configuration runs the check
    r' <- runExceptT $ runWriterT $ validateChainwebConfiguration
        def { _configPactHistory = defaultPactHistoryConfig { _phFreezeDepth = 0 } }
    case r' of
        Left e -> assertBool ("node validation: " <> T.unpack e) ("freezeDepth" `T.isInfixOf` e)
        Right _ -> assertFailure "the node configuration accepted depth 0"
  where
    validate c = fmap snd <$> runExceptT (runWriterT (validatePactHistoryConfig c))

-- | The JSON of the default node configuration of the base
-- (@storage/3.2.1@ at 647a14a53), recorded with @chainweb-node --print-config@
-- of that commit, against the new one without @pactHistory@.
--
baseJsonTest :: Assertion
baseJsonTest = do
    base <- eitherDecodeFileStrict "test/golden/storage-3.2.1-default-chainweb-config.json"
        >>= either fail return
    case toJSON def of
        Object o -> do
            KM.lookup "pactHistory" o @?= Just (toJSON defaultPactHistoryConfig)
            Object (KM.delete "pactHistory" o) @?= base
        v -> assertFailure $ "not an object: " <> show v
    -- the base's JSON parses to the new default
    case base of
        Object _ -> (eitherDecode (encode base) :: Either String ChainwebConfiguration) @?= Right def
        _ -> assertFailure "the base configuration is not an object"

versionTest :: Assertion
versionTest = do
    let testVersion = barebonesTestVersion singletonChainGraph
        allowed = [mainnet, testVersion, devnet]
        refused = [testnet04, testnet06, recapDevnet]
    forM_ (allowed <> refused) $ \v ->
        validatePactHistoryVersion v HistorySqlite @?= Right ()
    forM_ [HistorySplit, HistoryShadow] $ \s -> do
        forM_ allowed $ \v ->
            validatePactHistoryVersion v s @?= Right ()
        forM_ refused $ \v -> case validatePactHistoryVersion v s of
            Left e -> do
                let name = getChainwebVersionName (_versionName v)
                assertBool ("message does not name " <> T.unpack name <> ": " <> T.unpack e) (name `T.isInfixOf` e)
                assertBool ("message does not name the mode: " <> T.unpack e) (pactHistoryStoreToText s `T.isInfixOf` e)
            Right () -> assertFailure $ "accepted " <> show s <> " on " <> show (_versionName v)
    -- the node configuration refuses at start
    r <- runExceptT $ runWriterT $ validateChainwebConfiguration
        (defaultChainwebConfiguration Testnet04)
            { _configPactHistory = defaultPactHistoryConfig { _phStore = HistorySplit } }
    case r of
        Left e -> assertBool ("node validation: " <> T.unpack e) ("testnet04" `T.isInfixOf` e)
        Right _ -> assertFailure "the node configuration accepted split on testnet04"

testStorageTest :: Assertion
testStorageTest = do
    let parse = parseTestStorageOptions
    parse "" @?= Right []
    parse "pact-split,freeze-depth=3" @?= Right [TestPactSplit, TestFreezeDepth 3]
    (testPactHistoryConfigFor <$> parse "pact-split,freeze-depth=3")
        @?= Right defaultPactHistoryConfig { _phStore = HistorySplit, _phFreezeDepth = 3 }
    (testPactHistoryConfigFor <$> parse " pact-shadow , freeze-depth=5 ")
        @?= Right defaultPactHistoryConfig { _phStore = HistoryShadow, _phFreezeDepth = 5 }
    (testPactHistoryConfigFor <$> parse "zstd-dict")
        @?= Right defaultPactHistoryConfig
    (testPactHistoryConfigFor <$> parse "no-merkle-trees,pact-split")
        @?= Right defaultPactHistoryConfig { _phStore = HistorySplit }
    forM_ ["pact-splt", "freeze-depth=0", "freeze-depth=x", "freeze-depth=", "pact-split,pact-shadow"] $ \s ->
        case parse s of
            Left e -> assertBool ("message is empty for " <> show s) (not (null e))
            Right r -> assertFailure $ "accepted " <> show s <> ": " <> show r
