{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.Node
-- License: MIT
-- Stability: experimental
--
-- The node wiring of the Pact history store (storage redesign, Task 27): the
-- node-wide 'PactStorage' (layout, cold store, options, freezer semaphore),
-- the per-chain 'ChainStorage' (connections and the 'ChainStore' that the
-- checkpointer runs on), and the start of a chain's freezer.
--
-- With @--pact-history-store sqlite@ (the default) this opens exactly what
-- the @storage/3.2.1@ head opens: one SQLite connection per chain
-- ('withSqliteDb'), no cold store and no freezer thread.
--
-- Shadow mode keeps a chain's broken state across restarts: when the split
-- side of a chain fails, the file 'shadowBrokenFileName' is written into
-- @sqlite-split@, and a later start runs that chain on the legacy store alone
-- and logs that the split side needs a reset.
--
-- All SQL is in "Chainweb.Pact.Backend.Store.SQLite".
--
module Chainweb.Pact.Backend.Store.Node
( -- * Node
  PactStorage(..)
, PactStorageError(..)
, pactStorageErrorMessage
, withPactStorage
, sqliteOnlyStorage
, refuseReset
, pactStorageDirs

  -- * Chain
, ChainStore(..)
, chainStorePactStore
, ChainStorage(..)
, ChainFreezer(..)
, sqliteChainStorage
, withChainStorage
, withChainFreezer
, drainChainFreezer
, shadowBrokenFileName
) where

import Control.Concurrent.Async (cancel, withAsync)
import Control.Concurrent.MVar
import Control.Concurrent.QSem
import Control.Concurrent.STM (STM)
import Control.Exception
import Control.Lens (set)
import Control.Monad

import Data.Aeson (object, (.=))
import qualified Data.Aeson as A
import qualified Data.ByteString.Lazy as BL
import Data.IORef
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word

import GHC.Clock (getMonotonicTime)

import System.Directory
import System.FilePath
import qualified System.Logger.Types as L
import System.LogLevel

-- internal modules

import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Pact.Backend.Store.Freezer
import Chainweb.Pact.Backend.Store.Layout
import Chainweb.Pact.Backend.Store.Shadow
import Chainweb.Pact.Backend.Store.Split
import Chainweb.Pact.Backend.Store.SQLite (sqliteHasBlockHistory, sqlitePactStore, withSqliteErrorDetail)
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils (startSqliteDb, stopSqliteDb, withSqliteDb)
import Chainweb.Time (Micros, Time)
import Chainweb.Utils (sshow)
import Chainweb.Version

-- -------------------------------------------------------------------------- --
-- Node

data PactStorage = PactStorage
    { _pstLayout :: !PactLayout
    , _pstCold :: !(Maybe ColdStore)
        -- ^ split and shadow: the cold store, open read-write
    , _pstHistory :: !PactHistoryConfig
    , _pstFreezerSem :: !QSem
        -- ^ the node-wide freezer semaphore, '_phFreezerConcurrency' units
    , _pstFreezerPollMicros :: !Int
        -- ^ 'defaultFreezerPollMicros'; the test helpers use 100 ms
    }

data PactStorageError
    = HistoryStoreRefused !Text
        -- ^ 'validatePactHistoryVersion'
    | SplitStartRefused !Text
        -- ^ the start checks of a chain in split mode
    deriving (Show, Eq)

instance Exception PactStorageError where
    displayException = T.unpack . pactStorageErrorMessage

pactStorageErrorMessage :: PactStorageError -> Text
pactStorageErrorMessage = \case
    HistoryStoreRefused t -> t
    SplitStartRefused t -> t

-- | @--reset-chain-databases@ is refused in split and shadow mode
-- ('ResetRefused'). The node calls this before it removes anything.
--
refuseReset :: PactHistoryConfig -> Bool -> IO ()
refuseReset cfg reset = when (reset && _phStore cfg /= HistorySqlite) $
    throwIO $ ResetRefused (_phStore cfg)

-- | Validates the store for the version, resolves the layout under the root
-- (@<db-dir>/0@) and, in split and shadow, opens the cold store read-write
-- with a block cache of '_phColdBlockCacheBytes'.
--
-- A reset in sqlite mode also removes @sqlite-split@ and @pactCold@, so that
-- no trimmed directory and no cold store of an earlier split or shadow run
-- outlives it (Task 19-20 pointer M3). The node removes the whole database
-- directory before this anyway.
--
withPactStorage
    :: Logger l
    => l
    -> ChainwebVersion
    -> PactHistoryConfig
    -> FilePath
        -- ^ @<db-dir>/0@
    -> Bool
        -- ^ reset
    -> (PactStorage -> IO a)
    -> IO a
withPactStorage logger v cfg root reset k = do
    either (throwIO . HistoryStoreRefused) return
        $ validatePactHistoryVersion v (_phStore cfg)
    refuseReset cfg reset
    when reset $ forM_ [splitDirName, coldDirName] $ \d -> do
        let p = root </> d
        e <- doesDirectoryExist p
        when e $ do
            removeDirectoryRecursive p
            logFunctionText logger Info $ "--reset-chain-databases: removed " <> T.pack p
    layout <- resolvePactLayout logger (_phStore cfg) reset root
    sem <- newQSem (_phFreezerConcurrency cfg)
    let st cold = PactStorage layout cold cfg sem defaultFreezerPollMicros
    case _plColdDir layout of
        Nothing -> k (st Nothing)
        Just d -> withColdStore d ColdReadWrite (_phColdBlockCacheBytes cfg) (k . st . Just)

-- | The storage of the @storage/3.2.1@ head on a SQLite directory, for tests
-- and tools: sqlite mode, no cold store, no freezer.
--
sqliteOnlyStorage :: FilePath -> IO PactStorage
sqliteOnlyStorage dir = do
    ref <- newIORef dir
    lock <- newMVar ()
    pending <- newIORef False
    sem <- newQSem 1
    return PactStorage
        { _pstLayout = PactLayout
            { _plRoot = takeDirectory dir
            , _plMode = HistorySqlite
            , _plSqliteDir = ref
            , _plLegacyDir = Nothing
            , _plColdDir = Nothing
            , _plRenameLock = lock
            , _plFirstTrimPending = pending
            }
        , _pstCold = Nothing
        , _pstHistory = defaultPactHistoryConfig
        , _pstFreezerSem = sem
        , _pstFreezerPollMicros = defaultFreezerPollMicros
        }

-- | The directories the database monitor measures, by name: @pactDb@ (the
-- current SQLite directory) and, when present, @pactCold@.
--
pactStorageDirs :: PactStorage -> IO [(Text, FilePath)]
pactStorageDirs st = do
    sql <- currentSqliteDir (_pstLayout st)
    return $ ("pactDb", sql) : [ ("pactCold", d) | Just d <- [_plColdDir (_pstLayout st)] ]

-- -------------------------------------------------------------------------- --
-- Chain

data ChainStore
    = ChainStoreSqlite
    | ChainStoreSplit !SplitEnv
    | ChainStoreShadow !ShadowEnv

-- | The store the checkpointer runs on ('cpStore').
chainStorePactStore :: SQLiteEnv -> ChainStore -> PactStore
chainStorePactStore sql = \case
    ChainStoreSqlite -> sqlitePactStore sql
    ChainStoreSplit e -> splitPactStore e
    ChainStoreShadow e -> shadowPactStore e

-- | What a chain's freezer needs.
data ChainFreezer = ChainFreezer
    { _cfChain :: !ChainId
    , _cfStorage :: !PactStorage
    , _cfCold :: !ColdChain
    , _cfColdStore :: !ColdStore
    , _cfHot :: !SQLiteEnv
        -- ^ the hot connection, which the trim uses under @cpModuleCacheVar@
    , _cfCounters :: !(IORef SplitCounters)
    , _cfShadowDifferences :: !(IO (Maybe Word64))
    , _cfLogger :: !GenericLogger
    }

data ChainStorage = ChainStorage
    { _cstSql :: !SQLiteEnv
        -- ^ the primary connection: the only one in sqlite mode, the hot one
        -- in split mode, the legacy one in shadow mode
    , _cstStore :: !ChainStore
    , _cstFreezer :: !(Maybe ChainFreezer)
    , _cstDrain :: !(IORef (Maybe (IO ())))
        -- ^ set while the freezer runs: stops it and runs it to the end
    }

-- | The storage of a chain on one SQLite connection: sqlite mode.
sqliteChainStorage :: SQLiteEnv -> IO ChainStorage
sqliteChainStorage sql = ChainStorage sql ChainStoreSqlite Nothing <$> newIORef Nothing

-- | The file in @sqlite-split@ that marks a chain whose split side broke in
-- shadow mode.
shadowBrokenFileName :: ChainId -> FilePath
shadowBrokenFileName cid = "SHADOW-BROKEN-chain-" <> T.unpack (chainIdToText cid) <> ".json"

-- | Opens the connections of a chain and builds its store.
--
-- * sqlite: 'withSqliteDb' on the SQLite directory, with the reset flag, as
--   the @storage/3.2.1@ head.
-- * split: the hot file in the current SQLite directory, the chain of the
--   cold store, and the start checks: the verified frontier's (height, hash)
--   is in @BlockHistory@, and the SQLite tip is at or above the written
--   frontier. Otherwise 'SplitStartRefused'. A new hot file, which has no
--   schema until the checkpointer creates it, counts as empty.
-- * shadow: the legacy file in @sqlite@ is the primary connection; the split
--   side's hot file is in @sqlite-split@. A chain marked broken by an earlier
--   run starts broken, without a freezer.
--
withChainStorage
    :: Logger l
    => l
    -> PactStorage
    -> ChainId
    -> Bool
        -- ^ reset (sqlite mode only; refused before in the other modes)
    -> (ChainStorage -> IO a)
    -> IO a
withChainStorage logger st cid reset k = do
    drain <- newIORef Nothing
    case (_phStore cfg, _pstCold st) of
        (HistorySqlite, _) -> do
            dir <- currentSqliteDir layout
            withSqliteDb cid logger dir reset $ \sql ->
                k (ChainStorage sql ChainStoreSqlite Nothing drain)
        (mode, Nothing) -> throwIO $ HistoryStoreRefused $
            "internal error: no cold store in mode " <> pactHistoryStoreToText mode
        (HistorySplit, Just cs) -> withHot $ \hot -> do
            cc <- openColdChain cs cid
            splitStartChecks cid hot cc
            counters <- newIORef emptySplitCounters
            let env = SplitEnv cid (sqlitePactStore hot) cc counters
            k ChainStorage
                { _cstSql = hot
                , _cstStore = ChainStoreSplit env
                , _cstFreezer = Just (freezer cs cc hot counters (return Nothing))
                , _cstDrain = drain
                }
        (HistoryShadow, Just cs) -> do
            legacyDir <- maybe (throwIO $ ShadowNeedsLegacy (_plRoot layout </> sqliteDirName)) return
                (_plLegacyDir layout)
            withSqliteDb cid logger legacyDir False $ \legacy -> withHot $ \hot -> do
                cc <- openColdChain cs cid
                counters <- newIORef emptySplitCounters
                diffs <- newIORef 0
                let brokenFile = _plRoot layout </> splitDirName </> shadowBrokenFileName cid
                wasBroken <- doesFileExist brokenFile
                when wasBroken $ logg Warn $ T.unwords
                    [ "Shadow mode: the split side of chain", chainIdToText cid
                    , "broke in an earlier run (" <> T.pack brokenFile <> ")."
                    , "The chain runs on the legacy store alone and is not compared."
                    , "To compare it again, stop the node, remove", T.pack (_plRoot layout </> splitDirName)
                    , "and", T.pack (_plRoot layout </> coldDirName) <> ", and start again in shadow mode."
                    ]
                broken <- newIORef wasBroken
                logState <- newShadowLogState
                let env = ShadowEnv
                        { _shChain = cid
                        , _shLegacy = sqlitePactStore legacy
                        , _shSplit = splitPactStore (SplitEnv cid (sqlitePactStore hot) cc counters)
                        , _shLogger = shadowLogger
                        , _shDifferences = diffs
                        , _shBroken = broken
                        , _shClock = getMonotonicTime
                        , _shLogState = logState
                        , _shOnBroken = persistBroken brokenFile
                        }
                    stopped = do
                        n <- readIORef diffs
                        b <- readIORef broken
                        logJson Info $ object
                            [ "ShadowStopped" .= object
                                [ "chain" .= chainIdToText cid
                                , "differences" .= n
                                , "broken" .= b
                                ]
                            ]
                k ChainStorage
                    { _cstSql = legacy
                    , _cstStore = ChainStoreShadow env
                    , _cstFreezer = if wasBroken
                        then Nothing
                        else Just (freezer cs cc hot counters (Just <$> readIORef diffs))
                    , _cstDrain = drain
                    }
                    `finally` stopped
  where
    cfg = _pstHistory st
    layout = _pstLayout st
    logg = logFunctionText logger
    logJson level v = logg level (T.decodeUtf8 (BL.toStrict (A.encode v)))

    -- every new connection to the SQLite files opens under the rename lock
    withHot = bracket
        (withSqliteDirLock layout $ \dir -> startSqliteDb cid logger dir False)
        stopSqliteDb

    -- Shadow difference records must not block the Pact thread when the
    -- node's log queue is full: they go through the node's logger with the
    -- discard policy.
    shadowLogger = forwardingLogger (set L.setLoggerPolicy L.LogPolicyDiscard logger)

    freezer cs cc hot counters diffs = ChainFreezer
        { _cfChain = cid
        , _cfStorage = st
        , _cfCold = cc
        , _cfColdStore = cs
        , _cfHot = hot
        , _cfCounters = counters
        , _cfShadowDifferences = diffs
        , _cfLogger = forwardingLogger logger
        }

    -- A failed write is logged at Error and not rethrown: the chain runs on
    -- the legacy store alone either way, but the next start compares it
    -- again.
    persistBroken f = do
        let tmp = f <.> "tmp"
        r <- try $ do
            BL.writeFile tmp $ A.encode $ object
                [ "chain" .= chainIdToText cid
                , "message" .= ("the split side of this chain broke in shadow mode; remove sqlite-split and pactCold to compare again" :: Text)
                ]
            renameFile tmp f
        case r of
            Right () -> return ()
            Left (e :: IOException) -> logg Error $ T.unwords
                [ "Shadow mode: could not write the broken flag of chain", chainIdToText cid
                , "to", T.pack f <> ":", T.pack (displayException e) <> "."
                , "The chain runs on the legacy store alone; the next start compares it again."
                ]

-- | The start checks of a chain in split mode.
splitStartChecks :: ChainId -> SQLiteEnv -> ColdChain -> IO ()
splitStartChecks cid hot cc = withSqliteErrorDetail hot $ do
    let store = sqlitePactStore hot
    -- A new hot file (a fresh node or test directory) has no schema until the
    -- checkpointer creates it: it is empty, and holds no frontier block.
    hasHistory <- sqliteHasBlockHistory hot
    tip <- if hasHistory then _psGetLatestBlock store else return Nothing
    _ccVerified cc >>= mapM_ (\f -> do
        ok <- if hasHistory then _psLookupBlock store (_frHeight f, _frHash f) else return False
        unless ok $ throwIO $ SplitStartRefused $ T.unwords
            [ "Refusing to start chain", chainIdToText cid, "with --pact-history-store split:"
            , "the verified frontier of the cold store, height", sshow (_frHeight f) <> ", block"
            , sshow (_frHash f) <> ", is not in the BlockHistory of the SQLite file (its tip is"
            , tipText tip <> "). The SQLite file and the cold store do not belong together."
            , "Restore a whole snapshot (block database, SQLite and cold store together),"
            , "or replay Pact from the chain database."
            ])
    _ccWritten cc >>= mapM_ (\f ->
        unless (maybe False ((>= _frHeight f) . fst) tip) $ throwIO $ SplitStartRefused $ T.unwords
            [ "Refusing to start chain", chainIdToText cid, "with --pact-history-store split:"
            , "the SQLite tip,", tipText tip <> ", is below the written frontier of the cold store,"
            , "height", sshow (_frHeight f) <> ", block", sshow (_frHash f) <> "."
            , "Restore a whole snapshot (block database, SQLite and cold store together),"
            , "or replay Pact from the chain database."
            ])
  where
    tipText = \case
        Nothing -> "empty"
        Just (h, b) -> "height " <> sshow h <> ", block " <> sshow b

-- | Runs a chain's freezer ('runFreezer') next to an action, in split and
-- shadow when '_phFreezerEnabled'; otherwise just the action. Use it only
-- after the checkpointer exists ('withCheckpointerResources'). The freezer
-- does nothing until the action runs the start action it is given: the Pact
-- service runs it after the startup rewind ('initializeLatestBlock'), so that
-- the freezer cannot advance the frontier from the stale SQLite tip above the
-- rewind target. The freezer stops when the action ends; starting it twice is
-- harmless.
--
withChainFreezer
    :: ChainStorage
    -> (forall a. IO a -> IO a)
        -- ^ runs an action under @cpModuleCacheVar@
    -> STM Bool
        -- ^ a block waits for validation ('pactQueueBlockWaiting')
    -> IO (Maybe (Time Micros))
        -- ^ the creation time of the latest committed block
    -> (IO () -> IO b)
        -- ^ the action, given the start action of the freezer
    -> IO b
withChainFreezer cst underLock blockWaiting latestTime inner = case _cstFreezer cst of
    Just cf | _phFreezerEnabled (_pstHistory (_cfStorage cf)) -> do
        let st = _cfStorage cf
            cfg = _pstHistory st
        window <- newTrimDutyWindow
        stats <- newIORef emptyFreezerStats
        let tg = TrimGateEnv
                { _tgBlockWaiting = blockWaiting
                , _tgLatestBlockTime = latestTime
                , _tgConfig = cfg
                , _tgWindow = window
                , _tgClock = realGateClock
                }
            env = FreezerEnv
                { _feChain = _cfChain cf
                , _feCold = _cfCold cf
                , _feLayout = _pstLayout st
                , _feConfig = cfg
                , _feWithPactDb = \act -> underLock (act (_cfHot cf))
                , _feBeforeTrimChunk = trimGate tg
                , _feAfterTrimChunk = recordTrimChunk tg
                , _fePollMicros = _pstFreezerPollMicros st
                , _feSplitCounters = Just (_cfCounters cf)
                , _feCacheStats = coldCacheStats (_cfColdStore cf)
                , _feShadowDifferences = _cfShadowDifferences cf
                , _feTrimWindow = defaultTrimWindow
                , _feLogger = _cfLogger cf
                , _feHooks = noHooks
                , _feStats = stats
                }
        started <- newEmptyMVar
        withAsync (readMVar started >> runFreezer env (_pstFreezerSem st)) $ \a -> do
            writeIORef (_cstDrain cst) (Just (cancel a >> drainFreezer env))
            inner (void $ tryPutMVar started ())
                `finally` writeIORef (_cstDrain cst) Nothing
    _ -> inner (return ())

-- | Stops the chain's freezer and runs it to the end (@--only-sync-pact@).
-- 'False' when no freezer runs.
--
drainChainFreezer :: ChainStorage -> IO Bool
drainChainFreezer cst = readIORef (_cstDrain cst) >>= \case
    Nothing -> return False
    Just d -> True <$ d
