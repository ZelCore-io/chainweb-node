{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

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
-- The database is opened read-write, which takes the RocksDB @LOCK@ file. A
-- running node holds that lock, so the steps fail before they change
-- anything if a node (or another migration) uses the database. A step whose
-- namespaces are all done does not open the database: a rerun after
-- completion changes nothing, not even the files.
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
) where

import Control.Exception (bracket, throwIO)
import Control.Monad

import Data.Aeson
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Foreign.C
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr
import Foreign.Storable (peek)

import qualified GHC.Foreign as GHC
import qualified GHC.IO.Encoding as GHC

import System.Clock
import System.Directory (doesFileExist, renameFile)
import System.FilePath ((</>), takeDirectory)
import System.LogLevel
import System.Posix.IO (OpenMode(..), closeFd, defaultFileFlags, openFd)
import System.Posix.Unistd (fileSynchronise)

-- internal modules

import qualified Database.RocksDB.Internal as R

import Chainweb.Logger
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning
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

newtype MigrateOpts = MigrateOpts
    { _moStopAfter :: Maybe Int
        -- ^ test hook: return after this many namespaces were done in this
        -- run, as if the process had been stopped.
    }
    deriving (Show, Eq)

defaultMigrateOpts :: MigrateOpts
defaultMigrateOpts = MigrateOpts Nothing

-- -------------------------------------------------------------------------- --
-- Progress

-- | The namespaces that are done, as 'progressKey's, for instance
-- @trees:TransactionTree@. Stored as JSON in 'progressFile'.
--
newtype Progress = Progress { _progressDone :: S.Set T.Text }
    deriving (Show, Eq)

instance ToJSON Progress where
    toJSON (Progress d) = object ["done" .= S.toAscList d]

instance FromJSON Progress where
    parseJSON = withObject "Progress" $ \o -> Progress . S.fromList <$> o .: "done"

-- | @<db>/storage-migrate.progress.json@. RocksDB ignores files whose names it
-- does not use.
--
progressFile :: FilePath -> FilePath
progressFile db = db </> "storage-migrate.progress.json"

progressKey :: MigrateStep -> B.ByteString -> T.Text
progressKey step ns = migrateStepName step <> ":" <> T.decodeUtf8 ns

-- | The progress of a database. No progress file means no progress; a file
-- that does not parse is an error.
--
readProgress :: FilePath -> IO Progress
readProgress db = doesFileExist f >>= \case
    False -> return $ Progress mempty
    True -> eitherDecodeStrict' <$> B.readFile f >>= \case
        Right p -> return p
        Left e -> throwIO $ userError $ "storage-migrate: cannot parse " <> f <> ": " <> e
  where
    f = progressFile db

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
-- file, and a forced compaction of the range, which drops the entries from
-- the files. Then the namespace is marked as done.
--
-- The compaction writes its files with the compression of the database: with
-- zstd-dict once any payload namespace is done (see 'migratePayloads'),
-- otherwise with the options of chainweb-node 3.2.1 (Snappy).
--
migrateTrees :: Logger l => l -> FilePath -> MigrateOpts -> IO ()
migrateTrees l db = migrate l db MigrateTrees

-- | Rewrites the namespaces of 'payloadNamespaces', one after the other, with
-- a forced compaction of the range under 'payloadZstdTuning', so that every
-- file of the range is written with zstd and a dictionary. Then the namespace
-- is marked as done. Keys and values do not change.
--
migratePayloads :: Logger l => l -> FilePath -> MigrateOpts -> IO ()
migratePayloads l db = migrate l db MigratePayloads

migrate :: Logger l => l -> FilePath -> MigrateStep -> MigrateOpts -> IO ()
migrate l db step opts = do
    isDb <- doesFileExist (db </> "CURRENT")
    unless isDb $ throwIO $ userError $ "storage-migrate: not a RocksDB database: " <> db
    p0 <- readProgress db
    if null (todoOf p0)
    then
        -- the database is not opened, so that it does not change at all
        logg Info $ "storage-migrate " <> stepName <> ": nothing to do, all namespaces are done"
    else do
        logg Info $ "storage-migrate " <> stepName <> ": opening " <> T.pack db
            <> " (fails if a node uses the database)"
        withRocksDbTuned db modernDefaultOptions (stepTuning p0) $ \rdb -> do
            -- again, under the lock
            p1@(Progress done1) <- readProgress db
            let todo = todoOf p1
                todo' = maybe id take (_moStopAfter opts) todo
            forM_ (stepNamespaces step) $ \ns ->
                when (progressKey step ns `S.member` done1) $
                    logg Info $ "storage-migrate " <> stepName <> ": " <> nsText ns <> ": done before"
            unless (null todo') $ flushRocksDb rdb
            foldM_ (\done ns -> do
                t0 <- getTime Monotonic
                logg Info $ "storage-migrate " <> stepName <> ": " <> nsText ns <> ": start"
                migrateNamespace rdb ns
                let done' = S.insert (progressKey step ns) done
                writeProgress db (Progress done')
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

    todoOf (Progress done) =
        [ns | ns <- stepNamespaces step, progressKey step ns `S.notMember` done]

    stepTuning (Progress done) = case step of
        MigratePayloads -> payloadZstdTuning
        MigrateTrees
            | any (\ns -> progressKey MigratePayloads ns `S.member` done) payloadNamespaces ->
                payloadZstdTuning
            | otherwise -> defaultTuning

    migrateNamespace rdb ns = do
        let tbl = rawTable rdb ns
        when (step == MigrateTrees) $ do
            deleteRangeRocksDb tbl (Nothing, Nothing)
            flushRocksDb rdb
        compactRangeForced tbl (Nothing, Nothing)
