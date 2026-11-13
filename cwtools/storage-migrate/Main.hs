{-# LANGUAGE LambdaCase #-}

-- |
-- Module: Main
--
-- Offline, in-place conversions of the RocksDB database of a stopped node:
--
-- @
-- storage-migrate trees --database-directory DIR [--payload-compression snappy|zstd-dict] [--pause-file FILE]
-- storage-migrate payloads --database-directory DIR [--pause-file FILE]
-- @
--
-- @trees@ deletes the Merkle-tree tables, @payloads@ rewrites the payload
-- tables with zstd-dict. Run @trees@ first: @payloads@ then has less to
-- rewrite. Both resume where an interrupted run stopped, from
-- @storage-migrate.progress.json@ in the RocksDB directory.
--
-- A process that has the database open read-write (a node, another
-- @storage-migrate@) holds the RocksDB @LOCK@, and a step then fails without
-- a change. A process that opens the database read-only is not detected: the
-- operator must stop every process that opens the database, read-only ones
-- included, before a step runs.
--
-- See "Chainweb.StorageTools.Migrate".
--
module Main (main) where

import qualified Data.Text.IO as T

import Options.Applicative

import System.Directory (doesFileExist)
import System.Exit
import System.FilePath ((</>))
import System.IO
import System.LogLevel

-- internal modules

import Chainweb.Logger
import Chainweb.StorageTools.Migrate

-- | The RocksDB directory: @DIR@ if it is a RocksDB database, else
-- @DIR/0/rocksDb@, the layout of the node.
--
rocksDbDir :: FilePath -> IO FilePath
rocksDbDir dir = isDb dir >>= \case
    True -> return dir
    False -> isDb nodeDir >>= \case
        True -> return nodeDir
        False -> die $ "storage-migrate: no RocksDB database in " <> dir <> " or " <> nodeDir
  where
    nodeDir = dir </> "0" </> "rocksDb"
    isDb d = doesFileExist (d </> "CURRENT")

main :: IO ()
main = do
    hSetBuffering stderr LineBuffering
    MigrateCommand step dir opts <- execParser migrateParserInfo
    db <- rocksDbDir dir
    exitWith =<< runMigrate logger db step opts
  where
    logger = genericLogger Info (T.hPutStrLn stderr)
