{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GADTs #-}


{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE DataKinds #-}

-- |
-- Module: Chainweb.Pact.ChainwebPactDb
-- Copyright: Copyright © 2018 - 2020 Kadena LLC.
-- License: MIT
-- Maintainer: Emmanuel Denloye-Ito <emmanuel@kadena.io>
-- Stability: experimental
--
-- SQLite interaction utilities.

module Chainweb.Pact.Backend.Utils
  ( -- * General utils
    open2
  , chainDbFileName
    -- * Shared Pact database interactions
  , tbl
    -- * Savepoints
  , SavepointName(..)
  -- * SQLite conversions and assertions
  , toUtf8
  , fromUtf8
  , asStringUtf8
  , convSavepointName
  , expectSingleRowCol
  , expectSingle
  -- * SQLite runners
  , withSqliteDb
  , startSqliteDb
  , stopSqliteDb
  , withSQLiteConnection
  , openSQLiteConnection
  , closeSQLiteConnection
  , withTempSQLiteConnection
  , withInMemSQLiteConnection
  -- * SQLite Pragmas
  , chainwebPragmas
  ) where

import Control.Monad
import Control.Monad.Catch

import Data.Bits
import Data.Foldable
import Data.String
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Database.SQLite3.Direct as SQ3

import Prelude hiding (log)

import System.Directory
import System.FilePath
import System.LogLevel

-- pact

import qualified Pact.Types.SQLite as Pact4
import Pact.Types.Util (AsString(..))


-- chainweb

import Chainweb.Logger
import Chainweb.Pact.Backend.SQLite.DirectV2

import Chainweb.Pact.Types
import Chainweb.Version
import Chainweb.Utils
import Database.SQLite3.Direct hiding (open2)
import GHC.Stack (HasCallStack)
import qualified Data.ByteString.Char8 as B8
import Chainweb.Pact.Backend.Types
import Data.Text (Text)

-- -------------------------------------------------------------------------- --
-- SQ3.Utf8 Encodings

toUtf8 :: Text -> SQ3.Utf8
toUtf8 = SQ3.Utf8 . T.encodeUtf8
{-# INLINE toUtf8 #-}

fromUtf8 :: SQ3.Utf8 -> Text
fromUtf8 (SQ3.Utf8 bytes) = T.decodeUtf8 bytes
{-# INLINE fromUtf8 #-}

asStringUtf8 :: AsString a => a -> SQ3.Utf8
asStringUtf8 = toUtf8 . asString
{-# INLINE asStringUtf8 #-}


-- -------------------------------------------------------------------------- --
--

convSavepointName :: SavepointName -> SQ3.Utf8
convSavepointName = toUtf8 . toText

data SavepointName = BatchSavepoint | DbTransaction | PreBlock
  deriving (Eq, Ord, Enum, Bounded)

instance Show SavepointName where
    show = T.unpack . toText

instance HasTextRepresentation SavepointName where
    toText BatchSavepoint = "batch"
    toText DbTransaction = "db-transaction"
    toText PreBlock = "preblock"
    {-# INLINE toText #-}

    fromText "batch" = pure BatchSavepoint
    fromText "db-transaction" = pure DbTransaction
    fromText "preblock" = pure PreBlock
    fromText t = throwM $ TextFormatException
        $ "failed to decode SavepointName " <> t
        <> ". Valid names are " <> T.intercalate ", " (toText @SavepointName <$> [minBound .. maxBound])
    {-# INLINE fromText #-}

expectSingleRowCol :: Show a => String -> [[a]] -> IO a
expectSingleRowCol _ [[s]] = return s
expectSingleRowCol s v =
  internalError $
  "expectSingleRowCol: "
  <> asString s <>
  " expected single row and column result, got: "
  <> asString (show v)

expectSingle :: Show a => String -> [a] -> IO a
expectSingle _ [s] = return s
expectSingle desc v =
  internalError $
  "Expected single-" <> asString (show desc) <> " result, got: " <>
  asString (show v)

chainwebPragmas :: [Pact4.Pragma]
chainwebPragmas =
  [ "synchronous = NORMAL"
  , "journal_mode = WAL"
  , "locking_mode = NORMAL"
      -- changed from locking_mode = EXCLUSIVE to allow backups to run concurrently
      -- with Pact service operation. the effect of this change is twofold:
      --   - now file locks are grabbed at the beginning of each transaction; with
      --     EXCLUSIVE, file locks are never let go until the entire connection closes.
      --     (see https://web.archive.org/web/20220222231602/https://sqlite.org/pragma.html#pragma_locking_mode)
      --   - now we can query the database while another connection is open,
      --     taking full advantage of WAL mode.
      --     (see https://web.archive.org/web/20220226212219/https://sqlite.org/wal.html#sometimes_queries_return_sqlite_busy_in_wal_mode)
  , "temp_store = MEMORY"
  , "auto_vacuum = NONE"
  , "page_size = 1024"
  ]

withSqliteDb
    :: Logger logger
    => ChainId
    -> logger
    -> FilePath
    -> Bool
    -> (SQLiteEnv -> IO a)
    -> IO a
withSqliteDb cid logger dbDir resetDb = bracket
    (startSqliteDb cid logger dbDir resetDb)
    stopSqliteDb

startSqliteDb
    :: Logger logger
    => ChainId
    -> logger
    -> FilePath
    -> Bool
    -> IO SQLiteEnv
startSqliteDb cid logger dbDir doResetDb = do
    when doResetDb resetDb
    createDirectoryIfMissing True dbDir
    logFunctionText logger Debug $ "opening sqlitedb named " <> T.pack sqliteFile
    openSQLiteConnection sqliteFile chainwebPragmas
  where
    resetDb = removeDirectoryRecursive dbDir
    sqliteFile = dbDir </> chainDbFileName cid

chainDbFileName :: ChainId -> FilePath
chainDbFileName cid = fold
    [ "pact-v1-chain-"
    , T.unpack (chainIdToText cid)
    , ".sqlite"
    ]

stopSqliteDb :: SQLiteEnv -> IO ()
stopSqliteDb = closeSQLiteConnection

withSQLiteConnection :: String -> [Pact4.Pragma] -> (SQLiteEnv -> IO c) -> IO c
withSQLiteConnection file ps =
    bracket (openSQLiteConnection file ps) closeSQLiteConnection

openSQLiteConnection :: String -> [Pact4.Pragma] -> IO SQLiteEnv
openSQLiteConnection file ps = open2 file >>= \case
    Left (err, msg) ->
      internalError $
      "withSQLiteConnection: Can't open db with "
      <> asString (show err) <> ": " <> asString (show msg)
    Right r -> do
      Pact4.runPragmas r ps
      return r

closeSQLiteConnection :: SQLiteEnv -> IO ()
closeSQLiteConnection c = void $ close_v2 c

-- passing the empty string as filename causes sqlite to use a temporary file
-- that is deleted when the connection is closed. In practice, unless the database becomes
-- very large, the database will reside memory and no data will be written to disk.
--
-- Cf. https://www.sqlite.org/inmemorydb.html
--
withTempSQLiteConnection :: [Pact4.Pragma] -> (SQLiteEnv -> IO c) -> IO c
withTempSQLiteConnection = withSQLiteConnection ""

-- Using the special file name @:memory:@ causes sqlite to create a temporary in-memory
-- database.
--
-- Cf. https://www.sqlite.org/inmemorydb.html
--
withInMemSQLiteConnection :: [Pact4.Pragma] -> (SQLiteEnv -> IO c) -> IO c
withInMemSQLiteConnection = withSQLiteConnection ":memory:"

open2 :: String -> IO (Either (SQ3.Error, SQ3.Utf8) SQ3.Database)
open2 file = open_v2
    (fromString file)
    (collapseFlags [sqlite_open_readwrite , sqlite_open_create , sqlite_open_fullmutex])
    Nothing -- Nothing corresponds to the nullPtr

collapseFlags :: [SQLiteFlag] -> SQLiteFlag
collapseFlags xs =
    if Prelude.null xs then error "collapseFlags: You must pass a non-empty list"
    else Prelude.foldr1 (.|.) xs

sqlite_open_readwrite, sqlite_open_create, sqlite_open_fullmutex :: SQLiteFlag
sqlite_open_readwrite = 0x00000002
sqlite_open_create = 0x00000004
sqlite_open_fullmutex = 0x00010000

tbl :: HasCallStack => Utf8 -> Utf8
tbl t@(Utf8 b)
    | B8.elem ']' b = error $ "Chainweb.Pact4.Backend.ChainwebPactDb: Code invariant violation. Illegal SQL table name " <> sshow b <> ". Please report this as a bug."
    | otherwise = "[" <> t <> "]"
