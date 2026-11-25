{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.Config
-- License: MIT
-- Stability: experimental
--
-- The options of the Pact history store (spec 3, P3): which store keeps the
-- Pact history, and the freezer's depth, step sizes, cold block cache and
-- trim duty cycle.
--
-- With the defaults the node uses @sqlite@, the Pact store of the
-- @storage/3.2.1@ head: it opens no cold store and starts no freezer.
--
module Chainweb.Pact.Backend.Store.Config
( -- * History store
  PactHistoryStore(..)
, pactHistoryStoreToText
, pactHistoryStoreFromText
, pactHistoryStoreReader

  -- * Configuration
, PactHistoryConfig(..)
, phStore
, phFreezeDepth
, phFreezeStepBytes
, phFreezeStepHeights
, phColdBlockCacheBytes
, phTrimDutyCycle
, phCatchUpLagSeconds
, phFreezerConcurrency
, phFreezerEnabled
, defaultPactHistoryConfig
, pPactHistoryConfig
, validatePactHistoryConfig
, validatePactHistoryVersion
, startupRewindLimit
) where

import Configuration.Utils hiding (Error, Lens', disabled)

import Control.Lens hiding ((.=))
import Control.Monad
import Control.Monad.Catch (MonadThrow, throwM)
import Control.Monad.Except
import Control.Monad.Writer

import qualified Data.Text as T
import Data.Text (Text)
import Data.Word

import GHC.Generics

import Text.Read (readMaybe)

-- internal modules

import Chainweb.BlockHeight
import Chainweb.Utils (EncodingException(..), HasTextRepresentation(..), parseJsonFromText, sshow)
import Chainweb.Version
import Chainweb.Version.Development (devnet)
import Chainweb.Version.Mainnet (mainnet)

-- -------------------------------------------------------------------------- --
-- History store

-- | Where the node keeps the Pact history (spec 3, @--pact-history-store@).
--
data PactHistoryStore
    = HistorySqlite
        -- ^ the SQLite files only, as the @storage/3.2.1@ head (the default)
    | HistorySplit
        -- ^ a hot SQLite file per chain and one cold RocksDB store for the
        -- row versions below the frontier
    | HistoryShadow
        -- ^ the split store next to an untouched legacy SQLite file; every
        -- read goes to both and differences are logged (spec 7.5)
    deriving (Show, Eq, Ord, Enum, Bounded, Generic)

pactHistoryStoreToText :: PactHistoryStore -> Text
pactHistoryStoreToText HistorySqlite = "sqlite"
pactHistoryStoreToText HistorySplit = "split"
pactHistoryStoreToText HistoryShadow = "shadow"

pactHistoryStoreFromText :: MonadThrow m => Text -> m PactHistoryStore
pactHistoryStoreFromText = \case
    "sqlite" -> return HistorySqlite
    "split" -> return HistorySplit
    "shadow" -> return HistoryShadow
    x -> throwM $ TextFormatException $ unknownStore x

unknownStore :: Text -> Text
unknownStore x = "unknown Pact history store " <> sshow x
    <> ", expected sqlite, split or shadow"

-- | Reads @sqlite@, @split@ or @shadow@. Any other value fails with a message
-- that names all three.
--
pactHistoryStoreReader :: ReadM PactHistoryStore
pactHistoryStoreReader = eitherReader $ \s ->
    maybe (Left $ T.unpack $ unknownStore $ T.pack s) Right
        $ pactHistoryStoreFromText (T.pack s)

instance HasTextRepresentation PactHistoryStore where
    toText = pactHistoryStoreToText
    fromText = pactHistoryStoreFromText
    {-# INLINE toText #-}
    {-# INLINE fromText #-}

instance ToJSON PactHistoryStore where
    toJSON = toJSON . pactHistoryStoreToText
    {-# INLINE toJSON #-}

instance FromJSON PactHistoryStore where
    parseJSON = parseJsonFromText "PactHistoryStore"
    {-# INLINE parseJSON #-}

-- -------------------------------------------------------------------------- --
-- Configuration

data PactHistoryConfig = PactHistoryConfig
    { _phStore :: !PactHistoryStore
        -- ^ @--pact-history-store@
    , _phFreezeDepth :: !BlockHeight
        -- ^ @--pact-freeze-depth@: row versions older than this many heights
        -- below the tip are frozen
    , _phFreezeStepBytes :: !Int
        -- ^ @--pact-freeze-step-bytes@: the byte cap of one freeze step (one
        -- RocksDB batch)
    , _phFreezeStepHeights :: !BlockHeight
        -- ^ @--pact-freeze-step-heights@: the height cap of one freeze step
    , _phColdBlockCacheBytes :: !Int
        -- ^ @--pact-cold-block-cache@: the cold store's block cache
    , _phTrimDutyCycle :: !Double
        -- ^ @--pact-trim-duty-cycle@: the share of time trimming may take
        -- during catch-up, in (0, 1]
    , _phCatchUpLagSeconds :: !Int
        -- ^ a chain whose tip is older than this is catching up (config file
        -- only)
    , _phFreezerConcurrency :: !Int
        -- ^ the number of chains that freeze at the same time (config file
        -- only)
    , _phFreezerEnabled :: !Bool
        -- ^ whether the freezer runs. Not an option: the in-process harness of
        -- @cw-compare@ sets it to 'False'.
    } deriving (Show, Eq, Generic)

makeLenses ''PactHistoryConfig

-- | The defaults: @sqlite@; depth 4000 (spec 3); a step of at most 64 MiB (the
-- batch size M0 used on chains 3 and 19) and 1000 heights; a 1 GiB cold block
-- cache (as M0 measured); a trim duty cycle of 0.1 (spec 6.5, "about 10%");
-- catch-up lag 600 s and two chains freezing at once. The last four are start
-- values, tuned in gate B.
--
defaultPactHistoryConfig :: PactHistoryConfig
defaultPactHistoryConfig = PactHistoryConfig
    { _phStore = HistorySqlite
    , _phFreezeDepth = 4000
    , _phFreezeStepBytes = 67_108_864
    , _phFreezeStepHeights = 1_000
    , _phColdBlockCacheBytes = 1_073_741_824
    , _phTrimDutyCycle = 0.1
    , _phCatchUpLagSeconds = 600
    , _phFreezerConcurrency = 2
    , _phFreezerEnabled = True
    }

-- | The startup rewind limit (spec 2, fact 10). A freeze depth below it lets
-- startup rewinds reach the frontier, where they are refused.
--
startupRewindLimit :: BlockHeight
startupRewindLimit = 1000

-- | @_phFreezerEnabled@ is neither written nor read: it is not a node option.
--
instance ToJSON PactHistoryConfig where
    toJSON o = object
        [ "store" .= _phStore o
        , "freezeDepth" .= _phFreezeDepth o
        , "freezeStepBytes" .= _phFreezeStepBytes o
        , "freezeStepHeights" .= _phFreezeStepHeights o
        , "coldBlockCacheBytes" .= _phColdBlockCacheBytes o
        , "trimDutyCycle" .= _phTrimDutyCycle o
        , "catchUpLagSeconds" .= _phCatchUpLagSeconds o
        , "freezerConcurrency" .= _phFreezerConcurrency o
        ]

instance FromJSON (PactHistoryConfig -> PactHistoryConfig) where
    parseJSON = withObject "PactHistoryConfig" $ \o -> id
        <$< phStore ..: "store" % o
        <*< phFreezeDepth ..: "freezeDepth" % o
        <*< phFreezeStepBytes ..: "freezeStepBytes" % o
        <*< phFreezeStepHeights ..: "freezeStepHeights" % o
        <*< phColdBlockCacheBytes ..: "coldBlockCacheBytes" % o
        <*< phTrimDutyCycle ..: "trimDutyCycle" % o
        <*< phCatchUpLagSeconds ..: "catchUpLagSeconds" % o
        <*< phFreezerConcurrency ..: "freezerConcurrency" % o

instance FromJSON PactHistoryConfig where
    parseJSON v = parseJSON v <*> pure defaultPactHistoryConfig

-- -------------------------------------------------------------------------- --
-- Command line

-- | A reader for a whole number of at least 1. The message names the option.
--
positiveReader :: (Integral a) => String -> ReadM a
positiveReader opt = eitherReader $ \s -> case readMaybe @Integer s of
    Just n | n >= 1 && n <= fromIntegral (maxBound :: Int) -> Right (fromIntegral n)
    _ -> Left $ opt <> " must be a whole number of at least 1, got " <> show s

dutyCycleReader :: ReadM Double
dutyCycleReader = eitherReader $ \s -> case readMaybe @Double s of
    Just x | validDutyCycle x -> Right x
    _ -> Left $ "--pact-trim-duty-cycle must be a number in (0, 1], got " <> show s

validDutyCycle :: Double -> Bool
validDutyCycle x = x > 0 && x <= 1

pPactHistoryConfig :: MParser PactHistoryConfig
pPactHistoryConfig = id
    <$< phStore .:: option pactHistoryStoreReader
        % long "pact-history-store"
        <> metavar "sqlite|split|shadow"
        <> help "Where the Pact history is kept: sqlite (the SQLite files only), split (recent history in SQLite, older row versions in a cold RocksDB store), or shadow (split next to an untouched legacy SQLite file, comparing every read). split and shadow are refused on testnet04, testnet06 and recap-development; they are allowed on mainnet01, development and the test versions."
    <*< phFreezeDepth .:: option (BlockHeight <$> positiveReader @Word64 "--pact-freeze-depth")
        % long "pact-freeze-depth"
        <> metavar "HEIGHTS"
        <> help "With split or shadow: row versions more than this many heights below the tip move to the cold store. At least 1; below 1000, rewinds may reach the frontier and are refused."
    <*< phFreezeStepBytes .:: option (positiveReader "--pact-freeze-step-bytes")
        % long "pact-freeze-step-bytes"
        <> metavar "BYTES"
        <> help "With split or shadow: the most row bytes one freeze step copies to the cold store."
    <*< phFreezeStepHeights .:: option (BlockHeight <$> positiveReader @Word64 "--pact-freeze-step-heights")
        % long "pact-freeze-step-heights"
        <> metavar "HEIGHTS"
        <> help "With split or shadow: the most block heights one freeze step covers."
    <*< phColdBlockCacheBytes .:: option (positiveReader "--pact-cold-block-cache")
        % long "pact-cold-block-cache"
        <> metavar "BYTES"
        <> help "With split or shadow: the block cache of the cold store."
    <*< phTrimDutyCycle .:: option dutyCycleReader
        % long "pact-trim-duty-cycle"
        <> metavar "X"
        <> help "With split or shadow: the share of time, in (0, 1], that trimming SQLite may take while the node catches up."

-- -------------------------------------------------------------------------- --
-- Validation

-- | Checks the values, which may come from a configuration file. A depth
-- below 'startupRewindLimit' is accepted with a warning.
--
validatePactHistoryConfig :: ConfigValidation PactHistoryConfig []
validatePactHistoryConfig c = do
    when (_phFreezeDepth c < 1) $
        throwError "pactHistory.freezeDepth (--pact-freeze-depth) must be at least 1"
    unless (validDutyCycle (_phTrimDutyCycle c)) $
        throwError $ "pactHistory.trimDutyCycle (--pact-trim-duty-cycle) must be in (0, 1], got "
            <> sshow (_phTrimDutyCycle c)
    when (_phFreezeStepBytes c < 1) $
        throwError "pactHistory.freezeStepBytes (--pact-freeze-step-bytes) must be at least 1"
    when (_phFreezeStepHeights c < 1) $
        throwError "pactHistory.freezeStepHeights (--pact-freeze-step-heights) must be at least 1"
    when (_phColdBlockCacheBytes c < 1) $
        throwError "pactHistory.coldBlockCacheBytes (--pact-cold-block-cache) must be at least 1"
    when (_phFreezerConcurrency c < 1) $
        throwError "pactHistory.freezerConcurrency must be at least 1"
    when (_phCatchUpLagSeconds c < 1) $
        throwError "pactHistory.catchUpLagSeconds must be at least 1"
    when (_phStore c /= HistorySqlite && _phFreezeDepth c < startupRewindLimit) $
        tell
            [ "pactHistory.freezeDepth (--pact-freeze-depth) is " <> sshow (_phFreezeDepth c)
            <> ", below the startup rewind limit of " <> sshow startupRewindLimit
            <> " heights: rewinds may reach the frontier and will be refused"
            ]

-- | @split@ and @shadow@ are validated on mainnet01 only (Global Constraints,
-- network guard). They are refused on the other public versions (testnet04,
-- testnet06, recap-development) and allowed on the test versions (codes from
-- @0x80000000@, the test of 'Chainweb.Version.Guards.chainweb223PactInitCacheRebuild')
-- and on development. @sqlite@ is allowed everywhere.
--
validatePactHistoryVersion :: ChainwebVersion -> PactHistoryStore -> Either Text ()
validatePactHistoryVersion _ HistorySqlite = Right ()
validatePactHistoryVersion v s
    | allowed = Right ()
    | otherwise = Left $ "--pact-history-store " <> pactHistoryStoreToText s
        <> " is refused on chainweb version " <> getChainwebVersionName (_versionName v)
        <> ": split and shadow are allowed on mainnet01 only. Use --pact-history-store sqlite."
  where
    code = _versionCode v
    allowed = code == _versionCode mainnet
        || code >= ChainwebVersionCode 0x80000000
        || code == _versionCode devnet
