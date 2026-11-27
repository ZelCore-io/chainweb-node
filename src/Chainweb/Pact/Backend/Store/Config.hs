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
, phWalCheckpointer
, phWalCheckpoint
, WalCheckpointerMode(..)
, walCheckpointerOn
, WalCheckpointConfig(..)
, defaultWalCheckpointConfig
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
    , _phWalCheckpointer :: !WalCheckpointerMode
        -- ^ @--pact-wal-checkpointer@: whether a background thread
        -- checkpoints the Pact SQLite files instead of their commits (the
        -- commits keep a backstop autocheckpoint at
        -- '_wccRestartFrames')
    , _phWalCheckpoint :: !WalCheckpointConfig
        -- ^ the background checkpointer's thresholds (config file only)
    } deriving (Show, Eq, Generic)

-- | Whether the Pact SQLite files are checkpointed by a background thread
-- ("Chainweb.Pact.Backend.Store.WalCheckpointer"), with the connections'
-- autocheckpoint off. @auto@ (the default) is on in split and shadow mode
-- and off in sqlite mode, which stays as the @storage/3.2.1@ head.
data WalCheckpointerMode
    = WalCheckpointerAuto
    | WalCheckpointerOn
    | WalCheckpointerOff
    deriving (Show, Eq, Ord, Bounded, Enum, Generic)

walCheckpointerModeToText :: WalCheckpointerMode -> Text
walCheckpointerModeToText = \case
    WalCheckpointerAuto -> "auto"
    WalCheckpointerOn -> "on"
    WalCheckpointerOff -> "off"

walCheckpointerModeFromText :: Text -> Either String WalCheckpointerMode
walCheckpointerModeFromText t = case T.toLower t of
    "auto" -> Right WalCheckpointerAuto
    "on" -> Right WalCheckpointerOn
    "off" -> Right WalCheckpointerOff
    _ -> Left $ "the WAL checkpointer mode must be auto, on or off, got " <> show t

instance ToJSON WalCheckpointerMode where
    toJSON = toJSON . walCheckpointerModeToText

instance FromJSON WalCheckpointerMode where
    parseJSON = withText "WalCheckpointerMode" $ either fail return . walCheckpointerModeFromText

-- | Whether the background checkpointer runs for this configuration.
walCheckpointerOn :: PactHistoryConfig -> Bool
walCheckpointerOn c = case _phWalCheckpointer c of
    WalCheckpointerOn -> True
    WalCheckpointerOff -> False
    WalCheckpointerAuto -> _phStore c /= HistorySqlite

-- | The thresholds of the background checkpointer, in WAL frames (one page
-- each: 4 KiB on the Pact files, whose @page_size = 1024@ pragma comes after
-- the switch to WAL and so does not apply) and milliseconds.
data WalCheckpointConfig = WalCheckpointConfig
    { _wccPollMs :: !Int
        -- ^ how often the thread looks for a file whose next PASSIVE
        -- checkpoint is due (the earliest a checkpoint follows a commit)
    , _wccIntervalMs :: !Int
        -- ^ a PASSIVE checkpoint at least this often while frames are pending
    , _wccSoftFrames :: !Int
        -- ^ a PASSIVE checkpoint as soon as this many frames are expected to
        -- be pending, from the WAL's growth between the last two
        -- checkpoints (SQLite's autocheckpoint default is 1000)
    , _wccRestartFrames :: !Int
        -- ^ the writer's autocheckpoint while the thread serves it: a
        -- backstop for a WAL that the thread's checkpoints did not let
        -- restart (frames, checkpointed or not)
    , _wccFinalBusyMs :: !Int
        -- ^ the busy timeout of the final TRUNCATE checkpoint when a file's
        -- registration ends
    , _wccStatsLogSeconds :: !Int
        -- ^ how often each file's stats are logged (and at its stop)
    , _wccJournalSizeLimitBytes :: !Int
        -- ^ the writer's @journal_size_limit@ while the thread serves it:
        -- the first commit after a WAL restart truncates a longer @-wal@
        -- file to this size
    } deriving (Show, Eq, Generic)

makeLenses ''WalCheckpointConfig

-- | Poll 100 ms, PASSIVE at 1000 pending frames (SQLite's autocheckpoint
-- threshold) or every second, the writer's backstop at a WAL of 16,384
-- frames (64 MiB at 4 KiB pages), a final checkpoint that waits up to 1 s;
-- stats every 10 minutes; a WAL file truncated back to 64 MiB after a
-- restart (the backstop's size: at rest at most 64 MiB per served file,
-- about 1.3 GB on a 20-chain node in split mode and 2.6 GB in shadow mode).
defaultWalCheckpointConfig :: WalCheckpointConfig
defaultWalCheckpointConfig = WalCheckpointConfig
    { _wccPollMs = 100
    , _wccIntervalMs = 1_000
    , _wccSoftFrames = 1_000
    , _wccRestartFrames = 16_384
    , _wccFinalBusyMs = 1_000
    , _wccStatsLogSeconds = 600
    , _wccJournalSizeLimitBytes = 64 * 1024 * 1024
    }

instance ToJSON WalCheckpointConfig where
    toJSON o = object
        [ "pollMs" .= _wccPollMs o
        , "intervalMs" .= _wccIntervalMs o
        , "softFrames" .= _wccSoftFrames o
        , "restartFrames" .= _wccRestartFrames o
        , "finalBusyMs" .= _wccFinalBusyMs o
        , "statsLogSeconds" .= _wccStatsLogSeconds o
        , "journalSizeLimitBytes" .= _wccJournalSizeLimitBytes o
        ]

instance FromJSON (WalCheckpointConfig -> WalCheckpointConfig) where
    parseJSON = withObject "WalCheckpointConfig" $ \o -> id
        <$< wccPollMs ..: "pollMs" % o
        <*< wccIntervalMs ..: "intervalMs" % o
        <*< wccSoftFrames ..: "softFrames" % o
        <*< wccRestartFrames ..: "restartFrames" % o
        <*< wccFinalBusyMs ..: "finalBusyMs" % o
        <*< wccStatsLogSeconds ..: "statsLogSeconds" % o
        <*< wccJournalSizeLimitBytes ..: "journalSizeLimitBytes" % o

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
    , _phWalCheckpointer = WalCheckpointerAuto
    , _phWalCheckpoint = defaultWalCheckpointConfig
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
        , "walCheckpointer" .= _phWalCheckpointer o
        , "walCheckpoint" .= _phWalCheckpoint o
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
        <*< phWalCheckpointer ..: "walCheckpointer" % o
        <*< phWalCheckpoint %.: "walCheckpoint" % o

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
        <> help "Where the Pact history is kept: sqlite (the SQLite files only), split (recent history in SQLite, older row versions in a cold RocksDB store), or shadow (split next to an untouched legacy SQLite file, comparing every read; on a populated legacy directory, shadow needs a split side built from the same history). split and shadow are refused on testnet04, testnet06 and recap-development; they are allowed on mainnet01, development and the test versions."
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
        <> help "With split or shadow: the block cache of the cold store, in bytes (default 1 GiB, 1073741824)."
    <*< phTrimDutyCycle .:: option dutyCycleReader
        % long "pact-trim-duty-cycle"
        <> metavar "X"
        <> help "With split or shadow: the share of time, in (0, 1], that trimming SQLite may take while the node catches up."
    <*< phWalCheckpointer .:: option (eitherReader (walCheckpointerModeFromText . T.pack))
        % long "pact-wal-checkpointer"
        <> metavar "auto|on|off"
        <> help "Whether a background thread checkpoints the Pact SQLite files (WAL) instead of their commits. auto (the default) is on with split and shadow and off with sqlite."

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
    let w = _phWalCheckpoint c
    when (_wccPollMs w < 1 || _wccIntervalMs w < 1 || _wccFinalBusyMs w < 0 || _wccStatsLogSeconds w < 1) $
        throwError "pactHistory.walCheckpoint: pollMs, intervalMs and statsLogSeconds must be at least 1, finalBusyMs at least 0"
    when (_wccJournalSizeLimitBytes w < 0) $
        throwError "pactHistory.walCheckpoint: journalSizeLimitBytes must be at least 0"
    when (_wccSoftFrames w < 1 || _wccRestartFrames w < _wccSoftFrames w) $
        throwError "pactHistory.walCheckpoint: softFrames must be at least 1 and restartFrames at least softFrames"
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
        <> ": split and shadow are allowed on mainnet01, development and the test versions only. Use --pact-history-store sqlite."
  where
    code = _versionCode v
    allowed = code == _versionCode mainnet
        || code >= ChainwebVersionCode 0x80000000
        || code == _versionCode devnet
