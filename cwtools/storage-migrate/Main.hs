{-# LANGUAGE LambdaCase #-}

-- |
-- Module: Main
--
-- Offline, in-place conversions of the RocksDB database of a stopped node:
--
-- @
-- storage-migrate trees --database-directory DIR
-- storage-migrate payloads --database-directory DIR
-- @
--
-- @trees@ deletes the Merkle-tree tables, @payloads@ rewrites the payload
-- tables with zstd-dict. Run @trees@ first: @payloads@ then has less to
-- rewrite. Both resume where an interrupted run stopped, from
-- @storage-migrate.progress.json@ in the RocksDB directory. They fail without
-- a change while a node uses the database.
--
module Main (main) where

import qualified Data.Text as T
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

data Command = Command MigrateStep FilePath

commands :: Parser Command
commands = hsubparser $ mconcat
    [ cmd MigrateTrees "delete the Merkle-tree tables (TransactionTree, TransactionTree2, OutputTree, OutputTree2)"
    , cmd MigratePayloads "rewrite the payload tables with zstd-dict (as --payload-compression zstd-dict writes them)"
    ]
  where
    cmd step desc = command (T.unpack $ migrateStepName step) $
        info (Command step <$> dirOption) (progDesc desc)
    dirOption = strOption
        ( long "database-directory"
        <> metavar "DIR"
        <> help "the --database-directory of the node (the RocksDB database is DIR/0/rocksDb), or the RocksDB directory itself"
        )

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
    Command step dir <- execParser $ info (commands <**> helper)
        ( fullDesc
        <> progDesc "Offline, in-place conversions of the RocksDB database of a stopped node"
        )
    db <- rocksDbDir dir
    exitWith =<< runMigrate logger db step defaultMigrateOpts
  where
    logger = genericLogger Info (T.hPutStrLn stderr)
