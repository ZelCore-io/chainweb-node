{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.StorageTools.Migrate
--
-- Offline, in-place conversions of the RocksDB database of a node:
--
-- * 'migrateTrees' deletes the Merkle-tree tables of the payload store
--   ('treeNamespaces'), which nothing reads: SPV proofs rebuild the trees
--   from the payloads.
-- * 'migratePayloads' rewrites the payload tables ('payloadNamespaces') with
--   the zstd-dict compression of @--payload-compression zstd-dict@.
--
-- Each step works on one namespace at a time and records the namespaces that
-- are done in a progress file in the database directory (see 'Progress'). A
-- run that is interrupted resumes with the first namespace that is not done;
-- the work on a namespace is idempotent, so a namespace that was interrupted
-- is simply done again. No namespace outside of the two lists is deleted or
-- written.
--
-- The compaction of a namespace covers every live file whose key range
-- overlaps the namespace, not only the keys of the namespace (see
-- 'compactionRange'): a file can span a namespace without holding any of its
-- keys, for instance a file with keys of @BlockOutputs2@ and @BlockPayload2@
-- spans the empty old-layout @BlockPayload@, and RocksDB's range compaction
-- does not rewrite such a file. Keys of a neighbouring namespace may
-- therefore be rewritten as well, with the same compression.
--
-- A namespace is recorded as done only after its work was verified (see
-- 'compactRangeChecked' and 'verifyNamespace'): the compaction did not raise
-- RocksDB's count of background errors, and afterwards a tree namespace has
-- no entries and none of the files that overlapped it before the compaction
-- is live, and every live file that overlaps a payload namespace is ZSTD with
-- a dictionary. If the check fails, the namespace is compacted again, at most
-- 'compactionAttempts' times in all. If the work on a namespace fails, the
-- step stops with 'NamespaceFailed', which names the namespace, and the
-- namespace is not recorded, so that a rerun retries it.
--
-- The database is opened read-write, which takes the RocksDB @LOCK@ file. A
-- process that has the database open read-write (a node, another migration)
-- holds that lock, so the steps fail before they change anything while it
-- runs. A process that opens the database read-only takes no lock and is not
-- detected: for instance a node with @--read-only-replay@ or
-- @--only-sync-pact@ that does not prune the chain database, the source of
-- @compact@, or @cw-range-db diff@. The operator must stop every process that
-- opens the database, read-only ones included, before a step runs.
--
-- A step whose namespaces are all done does not open the database: a rerun
-- after completion changes nothing, not even the files. The progress file is
-- tied to the database by RocksDB's @IDENTITY@, so a database that replaced
-- another in place does not inherit its progress.
--
-- After the migration, the node must run with @--store-merkle-trees false@
-- and @--payload-compression zstd-dict@. With the defaults (@true@ and
-- @snappy@) it writes the trees and Snappy files again, while the progress
-- file still says that the steps are done.
--
module Chainweb.StorageTools.Migrate
( -- * Steps
  MigrateStep(..)
, migrateStepName
, MigrateOpts(..)
, defaultMigrateOpts
, migrateTrees
, migratePayloads
, migrate
, runMigrate
, MigrateException(..)

  -- * Command line
, MigrateCommand(..)
, migrateParserInfo
, migrateCommandParser

  -- * Compaction and checks
, compactionRange
, compactionAttempts
, compactRawRangeForced
, compactRawRangeWith
, compactRangeChecked
, verifyNamespace

  -- * Namespaces
, treeNamespaces
, payloadNamespaces
, stepNamespaces

  -- * Live files
, LiveFile(..)
, liveFileRanges
, liveFilePath
, namespaceBounds
, overlapsNamespace

  -- * Progress
, Progress(..)
, progressFile
, progressKey
, readProgress
, writeProgress
, dbIdentity
, checkedProgress
) where

import Control.Concurrent (threadDelay)
import Control.Exception
import Control.Monad

import Data.Aeson hiding (Error)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Options.Applicative

import Foreign.C
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr
import Foreign.Storable (peek)

import qualified GHC.Foreign as GHC
import qualified GHC.IO.Encoding as GHC

import System.Clock
import System.Directory (doesFileExist, renameFile)
import System.Exit (ExitCode(..))
import System.FilePath ((</>), takeDirectory)
import System.LogLevel
import System.Posix.IO (OpenMode(..), closeFd, defaultFileFlags, openFd)
import System.Posix.Unistd (fileSynchronise)

import Text.Read (readMaybe)

-- internal modules

import qualified Database.RocksDB.Internal as R

import Chainweb.Chainweb.Configuration
    (PayloadCompression(..), payloadCompressionReader, payloadCompressionToText, payloadCompressionTuning)
import Chainweb.Logger
import Chainweb.Storage.Table
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning
import Chainweb.StorageTools.Http (pausePollMicros)
import Chainweb.StorageTools.RangeDb (rawTable)
import Chainweb.Utils (sshow)

-- -------------------------------------------------------------------------- --
-- Namespaces

-- | The Merkle-tree tables of the payload store, old and new layout
-- (@Chainweb.Payload.PayloadStore.RocksDB@).
--
treeNamespaces :: [B.ByteString]
treeNamespaces = ["TransactionTree","TransactionTree2","OutputTree","OutputTree2"]

-- | The payload tables of the payload store, old and new layout, without the
-- Merkle trees (@Chainweb.Payload.PayloadStore.RocksDB@).
--
payloadNamespaces :: [B.ByteString]
payloadNamespaces = ["BlockPayloadIndex","BlockPayload","BlockPayload2","BlockTransactions","BlockTransactions2","BlockOutputs","BlockOutputs2"]

-- -------------------------------------------------------------------------- --
-- Live files

-- | A live SST file of a database and the range of the user keys in it.
--
data LiveFile = LiveFile
    { _lfName :: !FilePath
        -- ^ relative to the database directory and starting with @/@, as
        -- RocksDB reports it, for instance @/000012.sst@
    , _lfLevel :: !Int
    , _lfSmallestKey :: !B.ByteString
    , _lfLargestKey :: !B.ByteString
        -- ^ inclusive; the end of a range tombstone counts as a key
    }
    deriving (Show, Eq)

-- | The path of a live file in the database directory.
--
liveFilePath :: FilePath -> LiveFile -> FilePath
liveFilePath db f = db </> dropWhile (== '/') (_lfName f)

-- | The live SST files of an open database, with their key ranges.
--
liveFileRanges :: RocksDb -> IO [LiveFile]
liveFileRanges rdb =
    bracket (c_rocksdb_livefiles dbPtr) c_rocksdb_livefiles_destroy $ \lf -> do
        n <- c_rocksdb_livefiles_count lf
        forM [0 .. n - 1] $ \i -> LiveFile
            <$> (GHC.peekCString GHC.utf8 =<< c_rocksdb_livefiles_name lf i)
            <*> (fromIntegral <$> c_rocksdb_livefiles_level lf i)
            <*> key c_rocksdb_livefiles_smallestkey lf i
            <*> key c_rocksdb_livefiles_largestkey lf i
  where
    R.DB dbPtr = _rocksDbHandle rdb
    -- the key is owned by the live files object, so it is copied
    key f lf i = alloca $ \sizePtr -> do
        p <- f lf i sizePtr
        size <- peek sizePtr
        B.packCStringLen (p, fromIntegral size)

-- | The raw keys @[lower, upper)@ of a table namespace, for instance
-- @"TransactionTree"@ or @"BlockHeader/0/header"@. These are the bounds of
-- 'rawTable' (@newTable@ and its @$@ and @%@ bounds in
-- "Chainweb.Storage.Table.RocksDB").
--
namespaceBounds :: RocksDb -> B.ByteString -> (B.ByteString, B.ByteString)
namespaceBounds rdb ns = (prefix <> "$", prefix <> "%")
  where
    prefix = _rocksDbNamespace rdb <> "-" <> ns

-- | Whether the key range of a live file overlaps the keys of a namespace.
--
overlapsNamespace :: RocksDb -> B.ByteString -> LiveFile -> Bool
overlapsNamespace rdb ns f = _lfSmallestKey f < upper && _lfLargestKey f >= lower
  where
    (lower, upper) = namespaceBounds rdb ns

-- Foreign imports of the vendored RocksDB 8.3.2 C API (@c.h@) that
-- "Chainweb.Storage.Table.RocksDB.Tuning" does not import. The other
-- @rocksdb_livefiles_*@ calls come from there.

-- | @int rocksdb_livefiles_level(const rocksdb_livefiles_t*, int index);@
foreign import ccall safe "rocksdb_livefiles_level"
    c_rocksdb_livefiles_level
        :: Ptr LiveFiles -> CInt -> IO CInt

-- | @const char* rocksdb_livefiles_smallestkey(const rocksdb_livefiles_t*,
-- int index, size_t* size);@
--
-- The key is owned by the live files object.
foreign import ccall safe "rocksdb_livefiles_smallestkey"
    c_rocksdb_livefiles_smallestkey
        :: Ptr LiveFiles -> CInt -> Ptr CSize -> IO CString

-- | @const char* rocksdb_livefiles_largestkey(const rocksdb_livefiles_t*,
-- int index, size_t* size);@
--
-- The key is owned by the live files object.
foreign import ccall safe "rocksdb_livefiles_largestkey"
    c_rocksdb_livefiles_largestkey
        :: Ptr LiveFiles -> CInt -> Ptr CSize -> IO CString

-- -------------------------------------------------------------------------- --
-- Steps

data MigrateStep
    = MigrateTrees
        -- ^ delete the Merkle-tree tables
    | MigratePayloads
        -- ^ rewrite the payload tables with zstd-dict
    deriving (Show, Eq, Ord, Enum, Bounded)

migrateStepName :: MigrateStep -> T.Text
migrateStepName MigrateTrees = "trees"
migrateStepName MigratePayloads = "payloads"

-- | The namespaces of a step, in the order in which they are processed.
--
stepNamespaces :: MigrateStep -> [B.ByteString]
stepNamespaces MigrateTrees = treeNamespaces
stepNamespaces MigratePayloads = payloadNamespaces

data MigrateOpts = MigrateOpts
    { _moStopAfter :: !(Maybe Int)
        -- ^ test hook: return after this many namespaces were done in this
        -- run, as if the process had been stopped.
    , _moSkipWork :: ![B.ByteString]
        -- ^ test hook: the work on these namespaces is skipped, as if it had
        -- failed without an error. The check of the namespace must then
        -- fail.
    , _moSkipCompaction :: ![B.ByteString]
        -- ^ test hook: the compaction of these namespaces is skipped, as if
        -- it had done nothing without an error; for @trees@, the delete and
        -- the flush still happen. The check of the namespace must then fail.
    , _moPayloadCompression :: !PayloadCompression
        -- ^ @trees@: the compression of the node, with which the compaction
        -- rewrites the files that hold tree entries and other entries. Once
        -- a payload namespace is done, @trees@ uses zstd-dict.
    , _moPauseFile :: !(Maybe FilePath)
        -- ^ no namespace is started while this file exists
    }
    deriving (Show, Eq)

-- | The defaults of the command line: no test hook, the compression of
-- chainweb-node 3.2.1 (Snappy), no pause file.
--
defaultMigrateOpts :: MigrateOpts
defaultMigrateOpts = MigrateOpts
    { _moStopAfter = Nothing
    , _moSkipWork = []
    , _moSkipCompaction = []
    , _moPayloadCompression = PayloadSnappy
    , _moPauseFile = Nothing
    }

-- | The work on a namespace failed, or its check failed. The namespace is not
-- marked as done, so that a rerun retries it.
--
data MigrateException = NamespaceFailed !MigrateStep !B.ByteString !T.Text
    deriving (Eq)

instance Show MigrateException where
    show (NamespaceFailed step ns reason) = "storage-migrate " <> T.unpack (migrateStepName step)
        <> ": namespace " <> B8.unpack ns <> " failed and is not marked as done (a rerun retries it): "
        <> T.unpack reason

instance Exception MigrateException

-- | A failed check of 'compactNamespaceChecked' or 'verifyNamespace'.
--
newtype CheckFailed = CheckFailed T.Text
    deriving (Show)

instance Exception CheckFailed

-- -------------------------------------------------------------------------- --
-- Progress

-- | The namespaces that are done, as 'progressKey's, for instance
-- @trees:TransactionTree@, and the database they were done on. Stored as JSON
-- in 'progressFile'.
--
data Progress = Progress
    { _progressIdentity :: !T.Text
        -- ^ the content of the @IDENTITY@ file of the database (see
        -- 'dbIdentity')
    , _progressDone :: !(S.Set T.Text)
    }
    deriving (Show, Eq)

instance ToJSON Progress where
    toJSON (Progress i d) = object ["identity" .= i, "done" .= S.toAscList d]

instance FromJSON Progress where
    parseJSON = withObject "Progress" $ \o -> Progress
        <$> o .: "identity"
        <*> (S.fromList <$> o .: "done")

-- | @<db>/storage-migrate.progress.json@. RocksDB ignores files whose names it
-- does not use.
--
progressFile :: FilePath -> FilePath
progressFile db = db </> "storage-migrate.progress.json"

progressKey :: MigrateStep -> B.ByteString -> T.Text
progressKey step ns = migrateStepName step <> ":" <> T.decodeUtf8 ns

-- | The progress file of a database, if there is one. A file that does not
-- parse is an error.
--
readProgress :: FilePath -> IO (Maybe Progress)
readProgress db = doesFileExist f >>= \case
    False -> return Nothing
    True -> eitherDecodeStrict' <$> B.readFile f >>= \case
        Right p -> return (Just p)
        Left e -> throwIO $ userError $ "storage-migrate: cannot parse " <> f <> ": " <> e
  where
    f = progressFile db

-- | The content of the @IDENTITY@ file of a database, without surrounding
-- white space: a unique id that RocksDB writes when it creates the database
-- (and when it opens one that has no such file).
--
dbIdentity :: FilePath -> IO T.Text
dbIdentity db = doesFileExist f >>= \case
    False -> throwIO $ userError $ "storage-migrate: no IDENTITY file in " <> db
        <> ", so the progress file " <> progressFile db <> " cannot be checked against the database."
        <> " Delete the progress file to migrate this database from the start."
    True -> T.strip . T.decodeLatin1 <$> B.readFile f
  where
    f = db </> "IDENTITY"

-- | The namespaces that are done on a database. No progress file means that
-- none is done. A progress file of another database, which has another
-- @IDENTITY@, is refused.
--
checkedProgress :: FilePath -> IO (S.Set T.Text)
checkedProgress db = readProgress db >>= \case
    Nothing -> return mempty
    Just (Progress i done) -> do
        i' <- dbIdentity db
        unless (i == i') $ throwIO $ userError $ "storage-migrate: " <> progressFile db
            <> " is the progress of the database with IDENTITY " <> show i
            <> ", but the database in " <> db <> " has IDENTITY " <> show i'
            <> ". The database was replaced. Delete the progress file to migrate this database from the start."
        return done

-- | Replaces the progress file atomically: the new content goes to a
-- temporary file that is synced to disk and then renamed over the progress
-- file; the directory is synced as well, so that the rename is durable.
--
writeProgress :: FilePath -> Progress -> IO ()
writeProgress db p = do
    BL.writeFile tmp (encode p)
    fsyncPath tmp
    renameFile tmp f
    fsyncPath (takeDirectory f)
  where
    f = progressFile db
    tmp = f <> ".tmp"

fsyncPath :: FilePath -> IO ()
fsyncPath path = bracket (openFd path ReadOnly defaultFileFlags) closeFd fileSynchronise

-- -------------------------------------------------------------------------- --
-- Migration

-- | Deletes the namespaces of 'treeNamespaces', one after the other: a range
-- delete of the namespace, a flush that puts the range tombstone into an SST
-- file, and a forced compaction of the files that overlap the namespace (see
-- 'compactionRange'), which drops the entries and the tombstone from the
-- files. The namespace is marked as done once the compaction succeeded, the
-- namespace has no entries, and none of the files that overlapped the
-- namespace before the compaction is live. The tombstone alone already hides
-- the entries; the last condition shows that the compaction rewrote the
-- files.
--
-- The compaction writes its files with the compression that
-- '_moPayloadCompression' names (@--payload-compression@, Snappy by
-- default, as the node), or with zstd-dict once any payload namespace is done
-- (see 'migratePayloads').
--
migrateTrees :: Logger l => l -> FilePath -> MigrateOpts -> IO ()
migrateTrees l db = migrate l db MigrateTrees

-- | Rewrites the namespaces of 'payloadNamespaces', one after the other, with
-- a forced compaction of the files that overlap the namespace (see
-- 'compactionRange') under 'payloadZstdTuning', so that every such file is
-- written with zstd and a dictionary. The namespace is marked as done once
-- the compaction succeeded and every live file that overlaps the namespace is
-- ZSTD with a dictionary. Keys and values do not change.
--
migratePayloads :: Logger l => l -> FilePath -> MigrateOpts -> IO ()
migratePayloads l db = migrate l db MigratePayloads

migrate :: Logger l => l -> FilePath -> MigrateStep -> MigrateOpts -> IO ()
migrate l db step opts = do
    isDb <- doesFileExist (db </> "CURRENT")
    unless isDb $ throwIO $ userError $ "storage-migrate: not a RocksDB database: " <> db
    done0 <- checkedProgress db
    if null (todoOf done0)
    then
        -- the database is not opened, so that it does not change at all
        logg Info $ "storage-migrate " <> stepName <> ": nothing to do: "
            <> T.pack (progressFile db) <> " lists every namespace of the step as done, so the database"
            <> " is not opened. To redo a step, delete that file; both steps then start from the beginning."
    else do
        logg Info $ "storage-migrate " <> stepName <> ": opening " <> T.pack db
            <> " read-write. This fails if a process has it open read-write (a node, another"
            <> " storage-migrate), which holds the RocksDB LOCK. A process that opens it read-only is"
            <> " not detected: stop it first."
        when (step == MigrateTrees) $
            logg Info $ "storage-migrate trees: rewritten files are written with " <> stepCompression done0
        withRocksDbTuned db modernDefaultOptions (stepTuning done0) $ \rdb -> do
            ident <- dbIdentity db
            -- again, under the lock
            done1 <- checkedProgress db
            let todo = todoOf done1
                todo' = maybe id take (_moStopAfter opts) todo
            forM_ (stepNamespaces step) $ \ns ->
                when (progressKey step ns `S.member` done1) $
                    logg Info $ "storage-migrate " <> stepName <> ": " <> nsText ns <> ": done before"
            unless (null todo') $ flushRocksDb rdb
            foldM_ (\done ns -> do
                waitWhilePaused
                t0 <- getTime Monotonic
                logg Info $ "storage-migrate " <> stepName <> ": " <> nsText ns <> ": start"
                runNamespace rdb ns
                let done' = S.insert (progressKey step ns) done
                writeProgress db (Progress ident done')
                t1 <- getTime Monotonic
                logg Info $ "storage-migrate " <> stepName <> ": " <> nsText ns <> ": done in "
                    <> sshow (toNanoSecs (diffTimeSpec t1 t0) `div` 1_000_000) <> " ms"
                return done'
                ) done1 todo'
            when (length todo' < length todo) $
                logg Info $ "storage-migrate " <> stepName <> ": stopped after "
                    <> sshow (length todo') <> " namespaces"
  where
    logg = logFunctionText l
    stepName = migrateStepName step
    nsText = T.decodeUtf8

    todoOf done =
        [ns | ns <- stepNamespaces step, progressKey step ns `S.notMember` done]

    stepCompression done = case step of
        MigratePayloads -> payloadCompressionToText PayloadZstdDict
        MigrateTrees
            | any (\ns -> progressKey MigratePayloads ns `S.member` done) payloadNamespaces ->
                payloadCompressionToText PayloadZstdDict <> " (a payload namespace is done)"
            | otherwise -> payloadCompressionToText (_moPayloadCompression opts)

    stepTuning done = case step of
        MigratePayloads -> payloadZstdTuning
        MigrateTrees
            | any (\ns -> progressKey MigratePayloads ns `S.member` done) payloadNamespaces ->
                payloadZstdTuning
            | otherwise -> payloadCompressionTuning (_moPayloadCompression opts)

    waitWhilePaused = forM_ (_moPauseFile opts) $ \f -> do
        paused <- doesFileExist f
        when paused $ do
            logg Info $ "storage-migrate " <> stepName <> ": paused while " <> T.pack f <> " exists"
            let loop = doesFileExist f >>= \p -> when p (threadDelay pausePollMicros >> loop)
            loop
            logg Info $ "storage-migrate " <> stepName <> ": resumed"

    -- the work on a namespace and its check; any failure is a
    -- 'NamespaceFailed', which leaves the namespace out of the progress file
    runNamespace rdb ns = migrateNamespace rdb ns `catch` \e -> if
        | Just (_ :: SomeAsyncException) <- fromException e -> throwIO e
        | Just (nf :: MigrateException) <- fromException e -> throwIO nf
        | Just (CheckFailed reason) <- fromException e -> throwIO $ NamespaceFailed step ns reason
        | otherwise -> throwIO $ NamespaceFailed step ns (T.pack $ displayPlain e)

    migrateNamespace rdb ns = do
        if skipWork
        then logg Warn $ nsPrefix ns <> "test hook: the work is skipped"
        else when (step == MigrateTrees) $ do
            deleteRangeRocksDb (rawTable rdb ns) (Nothing, Nothing)
            flushRocksDb rdb
        compactAndVerify 1
      where
        skipWork = ns `elem` _moSkipWork opts
        skipCompaction = skipWork || ns `elem` _moSkipCompaction opts

        -- a failed check compacts again, a failed compaction does not
        compactAndVerify :: Int -> IO ()
        compactAndVerify i = do
            before <- filter (overlapsNamespace rdb ns) <$> liveFileRanges rdb
            let range = compactionRange rdb ns before
            if
                | skipWork -> return ()
                | skipCompaction -> logg Warn $ nsPrefix ns <> "test hook: the compaction is skipped"
                | otherwise -> do
                    when (range /= namespaceBounds rdb ns) $
                        logg Info $ nsPrefix ns <> sshow (length before)
                            <> " live files overlap the namespace; the compaction covers their keys, from "
                            <> sshow (fst range) <> " to " <> sshow (snd range)
                    compactRangeChecked rdb range
            try (verifyNamespace rdb db step ns before) >>= \case
                Right () -> return ()
                Left (CheckFailed reason)
                    | i < compactionAttempts -> do
                        logg Warn $ nsPrefix ns <> "the check failed after compaction " <> sshow i
                            <> " of at most " <> sshow compactionAttempts <> ", compacting again: " <> reason
                        compactAndVerify (i + 1)
                    | otherwise -> throwIO $ CheckFailed $
                        reason <> " (after " <> sshow i <> " compactions of the namespace)"

    nsPrefix ns = "storage-migrate " <> stepName <> ": " <> nsText ns <> ": "

-- | 'migrate' for a command line: a failure is logged at level 'Error' and
-- gives @ExitFailure 1@.
--
runMigrate :: Logger l => l -> FilePath -> MigrateStep -> MigrateOpts -> IO ExitCode
runMigrate l db step opts = try (migrate l db step opts) >>= \case
    Right () -> return ExitSuccess
    Left (e :: SomeException)
        | Just (_ :: SomeAsyncException) <- fromException e -> throwIO e
        | otherwise -> do
            logFunctionText l Error $ T.pack $ displayPlain e
            return $ ExitFailure 1

-- | 'displayException' of the exception itself, without the context (a
-- backtrace) that GHC adds to a 'SomeException' when it is thrown.
--
displayPlain :: SomeException -> String
displayPlain (SomeException e) = displayException e

-- -------------------------------------------------------------------------- --
-- Command line

data MigrateCommand = MigrateCommand
    { _mcStep :: !MigrateStep
    , _mcDatabaseDirectory :: !FilePath
        -- ^ the @--database-directory@ of the node, or the RocksDB directory
    , _mcOpts :: !MigrateOpts
    }
    deriving (Show, Eq)

migrateParserInfo :: ParserInfo MigrateCommand
migrateParserInfo = info (migrateCommandParser <**> helper)
    ( fullDesc
    <> progDesc "Offline, in-place conversions of the RocksDB database of a stopped node"
    <> footer
        ( "Stop every process that opens the database first, read-only ones included."
        <> " A process that has it open read-write holds the RocksDB LOCK, and a step then fails"
        <> " without a change; a process that opens it read-only is not detected."
        <> " Progress is kept in storage-migrate.progress.json in the RocksDB directory, so an"
        <> " interrupted step resumes, and a step that is done does nothing; delete that file to redo the steps."
        <> " After the migration, run the node with --store-merkle-trees false and"
        <> " --payload-compression zstd-dict: with the node's defaults (true and snappy) it writes"
        <> " the trees and Snappy files again."
        )
    )

migrateCommandParser :: Parser MigrateCommand
migrateCommandParser = hsubparser $ mconcat
    [ cmd MigrateTrees
        ( "delete the Merkle-tree tables (TransactionTree, TransactionTree2, OutputTree,"
        <> " OutputTree2). Run it first: payloads then has less to rewrite. Afterwards run the node with"
        <> " --store-merkle-trees false, or it writes the trees again."
        )
        compressionOption
    , cmd MigratePayloads
        ( "rewrite the payload tables with zstd-dict (as --payload-compression zstd-dict writes"
        <> " them). Afterwards run the node with --payload-compression zstd-dict, or it writes Snappy"
        <> " files again."
        )
        (pure PayloadZstdDict)
    ]
  where
    cmd step desc compression = command (T.unpack $ migrateStepName step) $ info
        (MigrateCommand step <$> dirOption <*> (opts <$> compression <*> pauseOption))
        (progDesc desc)

    opts c pause = defaultMigrateOpts
        { _moPayloadCompression = c
        , _moPauseFile = pause
        }

    dirOption = strOption
        ( long "database-directory"
        <> metavar "DIR"
        <> help "the --database-directory of the node (the RocksDB database is DIR/0/rocksDb), or the RocksDB directory itself"
        )

    compressionOption = option payloadCompressionReader
        ( long "payload-compression"
        <> metavar "snappy|zstd-dict"
        <> value (_moPayloadCompression defaultMigrateOpts)
        <> showDefaultWith (T.unpack . payloadCompressionToText)
        <> help
            ( "compression of the files that the compaction rewrites; use the value the node runs with."
            <> " Once the payloads step has done a namespace, zstd-dict is used whatever this says."
            )
        )

    pauseOption = optional $ strOption
        ( long "pause-file"
        <> metavar "FILE"
        <> help "no namespace is started while this file exists; the namespace in progress is finished first"
        )

-- -------------------------------------------------------------------------- --
-- Compaction and checks

-- | The raw keys @[begin, end]@, both inclusive, that the compaction of a
-- namespace covers: the bounds of the namespace ('namespaceBounds') and the
-- key range of every live file that overlaps it ('overlapsNamespace').
--
-- RocksDB's range compaction rewrites a level only if the level has a key in
-- the range. A file whose key range spans the namespace but holds none of its
-- keys (for instance a file with keys of @BlockOutputs2@ and @BlockPayload2@,
-- around the empty @BlockPayload@) is then not rewritten, while the check of
-- the namespace sees it. With the key ranges of the files, the range holds
-- the smallest key of each such file, so its level is compacted, and the
-- file with it.
--
compactionRange :: RocksDb -> B.ByteString -> [LiveFile] -> (B.ByteString, B.ByteString)
compactionRange rdb ns files =
    ( minimum (lower : (_lfSmallestKey <$> overlapping))
    , maximum (upper : (_lfLargestKey <$> overlapping))
    )
  where
    (lower, upper) = namespaceBounds rdb ns
    overlapping = filter (overlapsNamespace rdb ns) files

-- | How often a namespace is compacted at most: once, and twice more if its
-- check fails after a compaction. Then the namespace fails.
--
compactionAttempts :: Int
compactionAttempts = 3

-- | A forced compaction (@BottommostLevelCompaction::kForce@) of the raw keys
-- @[begin, end]@, both inclusive, of any namespaces. Every file in the range
-- is rewritten with the current options. 'compactRangeForced' takes the keys
-- of a single table instead.
--
compactRawRangeForced :: RocksDb -> (B.ByteString, B.ByteString) -> IO ()
compactRawRangeForced = compactRawRangeWith (\_ -> return ())

-- | 'compactRawRangeForced' with more compaction options, which the first
-- argument sets.
--
compactRawRangeWith :: (Ptr CompactOptions -> IO ()) -> RocksDb -> (B.ByteString, B.ByteString) -> IO ()
compactRawRangeWith setOptions rdb (begin, end) =
    bracket c_rocksdb_compactoptions_create c_rocksdb_compactoptions_destroy $ \copts -> do
        c_rocksdb_compactoptions_set_bottommost_level_compaction copts bottommostLevelForce
        setOptions copts
        B.useAsCStringLen begin $ \(beginPtr, beginLen) ->
            B.useAsCStringLen end $ \(endPtr, endLen) ->
                c_rocksdb_compact_range_opt dbPtr copts
                    beginPtr (fromIntegral beginLen)
                    endPtr (fromIntegral endLen)
  where
    R.DB dbPtr = _rocksDbHandle rdb
    -- BottommostLevelCompaction::kForce
    bottommostLevelForce = 2

-- | 'compactRawRangeForced', which throws if the compaction failed.
--
-- @rocksdb_compact_range_opt@ discards the status of the compaction. RocksDB
-- 8.3.2 counts every failed compaction, manual ones included, in the property
-- @rocksdb.background-errors@ (@DBImpl::BackgroundCallCompaction@), so the
-- property is read before and after the compaction.
--
compactRangeChecked :: RocksDb -> (B.ByteString, B.ByteString) -> IO ()
compactRangeChecked rdb range = do
    e0 <- backgroundErrors rdb
    compactRawRangeForced rdb range
    e1 <- backgroundErrors rdb
    when (e1 > e0) $ throwIO $ CheckFailed $
        "the compaction failed: rocksdb.background-errors went from " <> sshow e0 <> " to " <> sshow e1
        <> "; the LOG file of the database has the error"

backgroundErrors :: RocksDb -> IO Integer
backgroundErrors rdb = rocksDbProperty rdb "rocksdb.background-errors" >>= \case
    Just v | Just n <- readMaybe (B8.unpack v) -> return n
    v -> throwIO $ CheckFailed $ "cannot read rocksdb.background-errors: " <> sshow v

-- | The check of a namespace after its compaction, before it is marked as
-- done. The last argument is the live files that overlapped the namespace
-- before the compaction.
--
-- * @trees@: the namespace has no entries, and none of the files that
--   overlapped it before the compaction is live. The range tombstone alone
--   empties the namespace; the files show that the compaction rewrote them.
--   A file that the compaction wrote can still overlap the namespace: it
--   holds keys on both sides of it, as files are cut by size, not by
--   namespace.
-- * @payloads@: every live SST file whose key range overlaps the namespace is
--   ZSTD with the dictionary of 'payloadZstdTuning'.
--
-- The compression of the files is read from their table properties
-- ('sstCompressionInfo', which opens a read-only instance of the database).
-- An automatic compaction can replace live files while they are read; the
-- check is then repeated, up to five times.
--
verifyNamespace :: RocksDb -> FilePath -> MigrateStep -> B.ByteString -> [LiveFile] -> IO ()
verifyNamespace rdb _ MigrateTrees ns before = do
    e <- withTableIterator (rawTable rdb ns) iterEntry
    forM_ e $ \(Entry k _) -> throwIO $ CheckFailed $
        "the namespace still has entries after the delete and the compaction, for instance the key " <> sshow k
    live <- S.fromList . fmap _lfName <$> liveFileRanges rdb
    let stale = [_lfName f | f <- before, _lfName f `S.member` live]
    unless (null stale) $ throwIO $ CheckFailed $
        "live files that overlapped the namespace before the compaction are still live, so the compaction"
        <> " did not rewrite them: " <> sshow stale
verifyNamespace rdb db MigratePayloads ns _ = go (1 :: Int)
  where
    attempts = 5
    go i = do
        files <- filter (overlapsNamespace rdb ns) <$> liveFileRanges rdb
        unless (null files) $ try @IOException (sstCompressionInfo db) >>= \case
            Left e
                | i < attempts -> retry i
                | otherwise -> throwIO $ CheckFailed $
                    "cannot read the compression of the live files: " <> T.pack (displayException e)
            Right props -> do
                let found = [(f, M.lookup (liveFilePath db f) props) | f <- files]
                    notZstd = [(_lfName f, c) | (f, Just (c, o)) <- found, c /= "ZSTD" || not (dictOption `B.isInfixOf` o)]
                    missing = [_lfName f | (f, Nothing) <- found]
                if
                    | not (null notZstd) -> throwIO $ CheckFailed $
                        "live files of the namespace are not ZSTD with a dictionary (" <> T.decodeUtf8 dictOption
                        <> ") after the compaction: " <> sshow notZstd
                    | not (null missing) && i < attempts -> retry i
                    | not (null missing) -> throwIO $ CheckFailed $
                        "the live files changed while their compression was read: " <> sshow missing
                    | otherwise -> return ()
    retry i = threadDelay 1_000_000 >> go (i + 1)

-- | The dictionary size of 'payloadZstdTuning' as it appears in the property
-- @rocksdb.compression_options@ of a file, for instance
-- @max_dict_bytes=65536;@.
--
dictOption :: B.ByteString
dictOption = case _tuningCompression payloadZstdTuning of
    Zstd _ dictBytes _ -> "max_dict_bytes=" <> B8.pack (show dictBytes) <> ";"
    KeepBindingCompression -> error "Chainweb.StorageTools.Migrate.dictOption: payloadZstdTuning is not zstd"
