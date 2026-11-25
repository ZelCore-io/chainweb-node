{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.Layout
-- License: MIT
-- Stability: experimental
--
-- The directory layout of the Pact history under @<db-dir>/0@ (spec 6.2, 7.4,
-- controller's call 8):
--
-- * @sqlite@: the legacy SQLite files of all chains;
-- * @sqlite-split@: the same files once trimming may have started, with the
--   format marker @PACT-SPLIT-FORMAT.json@;
-- * @pactCold@: the cold store.
--
-- At the first trim on any chain the node writes the marker into @sqlite@ and
-- renames @sqlite@ to @sqlite-split@ ('ensureSplitDir'). A stock 3.2 binary
-- does not look in @sqlite-split@, and @--pact-history-store sqlite@ refuses
-- it, because trimmed files are not legacy files: @SYS:Pacts@ keeps no
-- anchors, so continuations of old defpacts would fail.
--
-- The rename is safe for open connections: they keep their file descriptors,
-- SQLite's locks are on the inode, and no new connection opens while the
-- rename runs ('withSqliteDirLock').
--
-- Directory states:
--
-- * __A__: @sqlite@ without a marker (or nothing yet);
-- * __B__: @sqlite@ with a marker: a crash inside the rename, nothing trimmed;
-- * __C__: @sqlite-split@ with a marker;
-- * __D__: @sqlite-split@ without a marker, which the protocol never leaves.
--
module Chainweb.Pact.Backend.Store.Layout
( -- * Layout
  PactLayout(..)
, resolvePactLayout
, currentSqliteDir
, withSqliteDirLock
, ensureSplitDir

  -- * Names
, sqliteDirName
, splitDirName
, coldDirName
, splitMarkerFileName

  -- * Marker
, SplitMarker(..)
, splitMarkerFormat
, splitMarkerVersion
, readSplitMarker
, writeSplitMarker

  -- * Errors
, PactLayoutError(..)
, pactLayoutErrorMessage
) where

import Control.Concurrent.MVar
import Control.Exception (Exception(..), bracket, throwIO)
import Control.Monad

import Data.Aeson
import qualified Data.ByteString.Lazy as BL
import Data.IORef
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock (getCurrentTime)
import Data.Time.Format.ISO8601 (iso8601Show)

import GHC.Generics (Generic)

import System.Directory
import System.FilePath
import System.LogLevel
import System.Posix.IO (OpenMode(..), closeFd, defaultFileFlags, openFd)
import System.Posix.Unistd (fileSynchronise)

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.Pact.Backend.Store.Config

-- -------------------------------------------------------------------------- --
-- Names

sqliteDirName, splitDirName, coldDirName, splitMarkerFileName :: FilePath
sqliteDirName = "sqlite"
splitDirName = "sqlite-split"
coldDirName = "pactCold"
splitMarkerFileName = "PACT-SPLIT-FORMAT.json"

-- -------------------------------------------------------------------------- --
-- Marker

-- | The format marker in @sqlite-split@. It records where the first trim of
-- the node happened.
--
data SplitMarker = SplitMarker
    { _smFormat :: !Text
        -- ^ always 'splitMarkerFormat'
    , _smVersion :: !Int
        -- ^ 'splitMarkerVersion'
    , _smFirstTrimChain :: !ChainId
    , _smFirstTrimHeight :: !BlockHeight
    , _smCreated :: !Text
        -- ^ ISO 8601
    } deriving (Show, Eq, Generic)

splitMarkerFormat :: Text
splitMarkerFormat = "chainweb-pact-split"

splitMarkerVersion :: Int
splitMarkerVersion = 1

instance ToJSON SplitMarker where
    toJSON m = object
        [ "format" .= _smFormat m
        , "version" .= _smVersion m
        , "firstTrimChain" .= _smFirstTrimChain m
        , "firstTrimHeight" .= _smFirstTrimHeight m
        , "created" .= _smCreated m
        ]

instance FromJSON SplitMarker where
    parseJSON = withObject "SplitMarker" $ \o -> SplitMarker
        <$> o .: "format"
        <*> o .: "version"
        <*> o .: "firstTrimChain"
        <*> o .: "firstTrimHeight"
        <*> o .: "created"

-- | Reads the marker of a directory: 'Nothing' if there is none. A marker
-- that does not parse, or has another format or version, is an
-- 'UnknownSplitMarker' error.
--
readSplitMarker :: FilePath -> IO (Maybe SplitMarker)
readSplitMarker dir = do
    let f = dir </> splitMarkerFileName
    doesFileExist f >>= \case
        False -> return Nothing
        True -> eitherDecode <$> BL.readFile f >>= \case
            Left e -> throwIO $ UnknownSplitMarker f (T.pack e)
            Right m
                | _smFormat m /= splitMarkerFormat || _smVersion m /= splitMarkerVersion ->
                    throwIO $ UnknownSplitMarker f
                        $ "format " <> T.pack (show (_smFormat m)) <> " version " <> T.pack (show (_smVersion m))
                | otherwise -> return (Just m)

-- | Writes the marker durably: a temporary file, @fsync@, rename, @fsync@ of
-- the directory.
--
writeSplitMarker :: FilePath -> ChainId -> BlockHeight -> IO SplitMarker
writeSplitMarker dir cid h = do
    now <- getCurrentTime
    let m = SplitMarker
            { _smFormat = splitMarkerFormat
            , _smVersion = splitMarkerVersion
            , _smFirstTrimChain = cid
            , _smFirstTrimHeight = h
            , _smCreated = T.pack (iso8601Show now)
            }
        f = dir </> splitMarkerFileName
        tmp = f <.> "tmp"
    BL.writeFile tmp (encode m)
    syncPath tmp
    renameFile tmp f
    syncPath dir
    return m

-- | @fsync@ of a file or a directory.
--
syncPath :: FilePath -> IO ()
syncPath p = bracket (openFd p ReadOnly defaultFileFlags) closeFd fileSynchronise

-- -------------------------------------------------------------------------- --
-- Errors

data PactLayoutError
    = TrimmedDirInSqliteMode FilePath
    | AmbiguousSqliteDirs FilePath FilePath
    | SplitDirWithoutMarker FilePath
    | MissingColdStore FilePath
    | ShadowNeedsLegacy FilePath
    | ResetRefused PactHistoryStore
    | BackupRefused PactHistoryStore
    | UnknownSplitMarker FilePath Text
        -- ^ a marker file that this binary does not understand
    deriving (Show, Eq)

instance Exception PactLayoutError where
    displayException = T.unpack . pactLayoutErrorMessage

-- | Each message names the directory and the way out (spec 7.4).
--
pactLayoutErrorMessage :: PactLayoutError -> Text
pactLayoutErrorMessage = \case
    TrimmedDirInSqliteMode d -> T.unwords
        [ "The Pact SQLite directory", p d, "holds trimmed Pact history (split format)."
        , "--pact-history-store sqlite cannot open it, because trimmed files are not legacy files."
        , "Start with --pact-history-store split, or restore a kept legacy copy into"
        , p (takeDirectory d </> sqliteDirName) <> ", rebuild legacy files with storage-export,"
        , "or replay Pact from the chain database."
        ]
    AmbiguousSqliteDirs legacy split -> T.unwords
        [ "Both", p legacy, "and", p split, "exist."
        , "With --pact-history-store split only", p split, "may exist;", p legacy
        , "is a legacy copy, for instance from shadow mode."
        , "Move", p legacy, "out of the database directory, or start with --pact-history-store shadow."
        ]
    SplitDirWithoutMarker d -> T.unwords
        [ "The directory", p d, "has no format marker", T.pack splitMarkerFileName <> "."
        , "This node never leaves it in that state, so it was not written by this node's split protocol."
        , "Move", p d, "out of the database directory and restore a consistent snapshot."
        ]
    MissingColdStore d -> T.unwords
        [ "The Pact SQLite files are trimmed, but the cold store", p d, "is missing,"
        , "so the history below the frontier is lost."
        , "Restore a whole snapshot (block database, SQLite and cold store together),"
        , "or remove the trimmed directory and replay Pact from the chain database."
        ]
    ShadowNeedsLegacy d -> T.unwords
        [ "--pact-history-store shadow needs the untouched legacy SQLite directory", p d <> ","
        , "but it is missing or carries the split format marker."
        , "Start with --pact-history-store split, or restore a legacy copy into", p d <> "."
        ]
    ResetRefused m -> T.unwords
        [ "--reset-chain-databases is refused with --pact-history-store", pactHistoryStoreToText m <> ":"
        , "the SQLite files and the cold store must be removed together."
        , "Remove the directories sqlite, sqlite-split and pactCold under <db-dir>/0 by hand,"
        , "or reset with --pact-history-store sqlite."
        ]
    BackupRefused m -> T.unwords
        [ "The backup API is refused with --pact-history-store", pactHistoryStoreToText m <> ":"
        , "a backup must cover the cold store too."
        , "Take whole snapshots of the database directory with the node stopped,"
        , "or run with --pact-history-store sqlite."
        ]
    UnknownSplitMarker f why -> T.unwords
        [ "The format marker", p f, "is not one this binary understands (" <> why <> ")."
        , "Run the binary that wrote it, or restore a consistent snapshot."
        ]
  where
    p = T.pack

-- -------------------------------------------------------------------------- --
-- Layout

data PactLayout = PactLayout
    { _plRoot :: !FilePath
        -- ^ @<db-dir>/0@
    , _plMode :: !PactHistoryStore
    , _plSqliteDir :: !(IORef FilePath)
        -- ^ the directory of the SQLite files the node runs on: @sqlite@,
        -- or @sqlite-split@ once it exists
    , _plLegacyDir :: !(Maybe FilePath)
        -- ^ shadow: @<root>/sqlite@
    , _plColdDir :: !(Maybe FilePath)
        -- ^ split, shadow: @<root>/pactCold@
    , _plRenameLock :: !(MVar ())
    }

-- | The directory of the SQLite files the node runs on.
--
currentSqliteDir :: PactLayout -> IO FilePath
currentSqliteDir = readIORef . _plSqliteDir

-- | Runs an action with the current SQLite directory while no rename can
-- happen. Every new connection to the SQLite files opens through it.
--
withSqliteDirLock :: PactLayout -> (FilePath -> IO a) -> IO a
withSqliteDirLock l act = withMVar (_plRenameLock l) $ \() -> currentSqliteDir l >>= act

-- | The directory state under the root.
--
data DirState = DirState
    { _dsSqlite :: !Bool
    , _dsSqliteMarker :: !Bool
    , _dsSplit :: !Bool
    , _dsSplitMarker :: !Bool
    , _dsCold :: !Bool
    }

readDirState :: FilePath -> IO DirState
readDirState root = do
    hasSqlite <- doesDirectoryExist sqliteDir
    hasSplit <- doesDirectoryExist splitDir
    sqliteMarker <- if hasSqlite then hasMarker sqliteDir else return False
    splitMarker <- if hasSplit then hasMarker splitDir else return False
    hasCold <- doesDirectoryExist (root </> coldDirName)
    return $ DirState hasSqlite sqliteMarker hasSplit splitMarker hasCold
  where
    sqliteDir = root </> sqliteDirName
    splitDir = root </> splitDirName
    hasMarker d = (/= Nothing) <$> readSplitMarker d

-- | Resolves the layout under @root@ (@<db-dir>/0@) for a mode, following the
-- table of Task 20. It may finish an interrupted rename (state B in split
-- mode), remove a stale marker (state B in sqlite mode), or create the split
-- side and the cold directory (shadow mode). It runs before any connection
-- opens. Throws 'PactLayoutError'.
--
resolvePactLayout
    :: Logger l
    => l
    -> PactHistoryStore
    -> Bool
        -- ^ reset (@--reset-chain-databases@)
    -> FilePath
    -> IO PactLayout
resolvePactLayout logger mode reset root = do
    when (reset && mode /= HistorySqlite) $ throwIO $ ResetRefused mode
    st <- readDirState root
    when (_dsSplit st && not (_dsSplitMarker st)) $
        throwIO $ SplitDirWithoutMarker splitDir
    when (_dsSqliteMarker st && _dsSplit st) $
        throwIO $ AmbiguousSqliteDirs sqliteDir splitDir
    lock <- newMVar ()
    let layout dir legacy cold = do
            ref <- newIORef dir
            return $ PactLayout
                { _plRoot = root
                , _plMode = mode
                , _plSqliteDir = ref
                , _plLegacyDir = legacy
                , _plColdDir = cold
                , _plRenameLock = lock
                }
    case mode of
        HistorySqlite -> do
            when (_dsSplit st && not (_dsSqlite st)) $
                throwIO $ TrimmedDirInSqliteMode splitDir
            when (_dsSplit st) $ logg Warn $ T.unwords
                [ "Using", T.pack sqliteDir <> "; the split directory", T.pack splitDir
                , "is left over (for instance from shadow mode) and is not used."
                ]
            when (_dsSqliteMarker st) $ do
                removeFile (sqliteDir </> splitMarkerFileName)
                syncPath sqliteDir
                logg Warn $ "Removed the split format marker from " <> T.pack sqliteDir
                    <> ": the rename to sqlite-split had not finished, nothing was trimmed."
            when (_dsCold st) $ logg Warn $ T.unwords
                [ "The cold store", T.pack coldDir, "is unused with --pact-history-store sqlite;"
                , "delete it to roll back."
                ]
            layout sqliteDir Nothing Nothing
        HistorySplit
            | _dsSplit st && _dsSqlite st ->
                throwIO $ AmbiguousSqliteDirs sqliteDir splitDir
            | _dsSplit st -> do
                unless (_dsCold st) $ throwIO $ MissingColdStore coldDir
                layout splitDir Nothing (Just coldDir)
            | _dsSqliteMarker st -> do
                renameDirectory sqliteDir splitDir
                syncPath root
                logg Info $ "Finished the rename of " <> T.pack sqliteDir <> " to "
                    <> T.pack splitDir <> " that a previous run had started."
                layout splitDir Nothing (Just coldDir)
            | otherwise ->
                layout sqliteDir Nothing (Just coldDir)
        HistoryShadow
            | _dsSqliteMarker st || not (_dsSqlite st) && _dsSplit st ->
                throwIO $ ShadowNeedsLegacy sqliteDir
            | otherwise -> do
                createDirectoryIfMissing True sqliteDir
                unless (_dsSplit st) $ do
                    createDirectory splitDir
                    void $ writeSplitMarker splitDir (unsafeChainId 0) 0
                    syncPath root
                    logg Info $ "Created the split side " <> T.pack splitDir <> " for shadow mode."
                unless (_dsCold st) $ do
                    createDirectory coldDir
                    syncPath root
                layout splitDir (Just sqliteDir) (Just coldDir)
  where
    sqliteDir = root </> sqliteDirName
    splitDir = root </> splitDirName
    coldDir = root </> coldDirName
    logg = logFunctionText logger

-- | Makes sure the SQLite files live in @sqlite-split@ with a marker. Called
-- before every trim; trimming starts only after it returns. The first call on
-- the node renames @sqlite@ (the protocol below), later calls return at once.
--
-- Under the rename lock: write the marker into @sqlite@ (temporary file,
-- @fsync@, rename, @fsync@ of the directory), rename @sqlite@ to
-- @sqlite-split@, @fsync@ the root, then point '_plSqliteDir' at the new
-- path. Open connections keep their file descriptors.
--
ensureSplitDir :: Logger l => l -> PactLayout -> ChainId -> BlockHeight -> IO ()
ensureSplitDir logger l cid h = do
    when (_plMode l == HistorySqlite) $
        ioError $ userError "ensureSplitDir: called with --pact-history-store sqlite"
    cur <- currentSqliteDir l
    unless (takeFileName cur == splitDirName) $
        modifyMVar_ (_plRenameLock l) $ \() -> do
            dir <- currentSqliteDir l
            unless (takeFileName dir == splitDirName) $ do
                let new = _plRoot l </> splitDirName
                m <- writeSplitMarker dir cid h
                renameDirectory dir new
                syncPath (_plRoot l)
                writeIORef (_plSqliteDir l) new
                logFunctionText logger Info $ "First trim (chain " <> chainIdToText cid
                    <> ", height " <> T.pack (show h) <> "): renamed " <> T.pack dir
                    <> " to " <> T.pack new <> " with format marker created "
                    <> _smCreated m
