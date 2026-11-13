{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Test.Pact.Store.SQLite
--
-- Unit tests of the SQLite 'PactStore'.
--
module Chainweb.Test.Pact.Store.SQLite (tests) where

import Control.Exception (ErrorCall(..), SomeException, throwIO, try)
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.List (intersperse, sort)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Database.SQLite3.Direct as SQ3
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource (runResourceT)
import Test.Tasty
import Test.Tasty.HUnit

import qualified Pact.Types.SQLite as P4SQL

import Chainweb.BlockHash
import Chainweb.BlockHeight
import Chainweb.Pact.Backend.Store.SQLite
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils (chainwebPragmas, withSQLiteConnection, withTempSQLiteConnection)
import Chainweb.Test.Utils (withTempDir)
import Chainweb.Utils.Serialization (runGetEitherS)

tests :: TestTree
tests = testGroup "PactStore SQLite"
    [ testCase "readNewest: newest version, and newest below a bound" $ withStore $ \_ ps -> do
        commit ps 1 ["t"] [("t", [("a", 1, "v1"), ("a", 2, "v2"), ("b", 3, "v3"), ("a", 5, "v5")])] []
        _psReadNewest ps "t" "a" Nothing >>= (@?= Right (Just "v5"))
        _psReadNewest ps "t" "a" (Just 5) >>= (@?= Right (Just "v2"))
        _psReadNewest ps "t" "a" (Just 2) >>= (@?= Right (Just "v1"))
        _psReadNewest ps "t" "a" (Just 1) >>= (@?= Right Nothing)
        _psReadNewest ps "t" "zz" Nothing >>= (@?= Right Nothing)

    , testCase "readNewest on a missing table returns Left ErrorError" $ withStore $ \_ ps -> do
        _psReadNewest ps "nope" "a" Nothing >>= (@?= Left SQ3.ErrorError)
        _psReadNewest ps "nope" "a" (Just 3) >>= (@?= Left SQ3.ErrorError)

    , testCase "distinctKeys KeysOrderedByKey is ascending bytewise; KeysAsStored is the set SQLite returns" $ withStore $ \sql ps -> do
        commit ps 1 ["t"] [("t", [("b", 1, "x"), ("a", 2, "x"), ("C", 3, "x"), ("a", 4, "x"), ("k10", 5, "x"), ("k9", 6, "x")])] []
        _psDistinctKeys ps KeysOrderedByKey "t" (Just 7) >>= (@?= Right ["C", "a", "b", "k10", "k9"])
        _psDistinctKeys ps KeysOrderedByKey "t" (Just 3) >>= (@?= Right ["a", "b"])
        raw <- (\rs -> [ k | [P4SQL.SText (SQ3.Utf8 k)] <- rs ]) <$> P4SQL.qry_ sql "SELECT DISTINCT rowkey FROM [t]" [P4SQL.RText]
        Right asStored <- _psDistinctKeys ps KeysAsStored "t" Nothing
        asStored @?= raw
        sort asStored @?= ["C", "a", "b", "k10", "k9"]
        Right asStoredBelow3 <- _psDistinctKeys ps KeysAsStored "t" (Just 3)
        sort asStoredBelow3 @?= ["a", "b"]

    , testCase "commitBlock: rowids ascend in _bcRows order; each table's mutation row right after its rows; index and history rows written" $ withStore $ \sql ps -> do
        commit ps 1 ["t1", "t2"] [] ["h1"]
        query sql 3 "SELECT rowid, tablename, createBlockheight FROM VersionedTableCreation ORDER BY rowid"
            >>= (@?= [["1", "t1", "1"], ["2", "t2", "1"]])
        -- record the order of every insert from here on
        forM_
            [ "CREATE TABLE log (seq INTEGER PRIMARY KEY, what TEXT)"
            , "CREATE TRIGGER log_t1 AFTER INSERT ON [t1] BEGIN INSERT INTO log (what) VALUES ('t1 ' || NEW.rowkey); END"
            , "CREATE TRIGGER log_t2 AFTER INSERT ON [t2] BEGIN INSERT INTO log (what) VALUES ('t2 ' || NEW.rowkey); END"
            , "CREATE TRIGGER log_mut AFTER INSERT ON VersionedTableMutation BEGIN INSERT INTO log (what) VALUES ('mutation ' || NEW.tablename); END"
            , "CREATE TRIGGER log_idx AFTER INSERT ON TransactionIndex BEGIN INSERT INTO log (what) VALUES ('index ' || CAST(NEW.txhash AS TEXT)); END"
            , "CREATE TRIGGER log_hist AFTER INSERT ON BlockHistory BEGIN INSERT INTO log (what) VALUES ('history ' || NEW.blockheight); END"
            ] $ P4SQL.exec_ sql
        commit ps 2 []
            [ ("t2", [("y", 3, "y3"), ("x", 2, "x2"), ("y", 4, "y4")])
            , ("t1", [("b", 5, "b5"), ("a", 2, "a2")])
            ]
            ["h2a", "h2b"]
        query sql 1 "SELECT what FROM log ORDER BY seq" >>= (@?= fmap pure
            [ "t2 y", "t2 x", "t2 y", "mutation t2"
            , "t1 b", "t1 a", "mutation t1"
            , "index h2a", "index h2b"
            , "history 2"
            ])
        query sql 4 "SELECT rowid, rowkey, txid, rowdata FROM [t2] ORDER BY rowid"
            >>= (@?= [["1", "y", "3", "y3"], ["2", "x", "2", "x2"], ["3", "y", "4", "y4"]])
        query sql 3 "SELECT rowid, tablename, blockheight FROM VersionedTableMutation ORDER BY rowid"
            >>= (@?= [["1", "t2", "2"], ["2", "t1", "2"]])
        query sql 2 "SELECT CAST(txhash AS TEXT), blockheight FROM TransactionIndex ORDER BY rowid"
            >>= (@?= [["h1", "1"], ["h2a", "2"], ["h2b", "2"]])
        query sql 3 "SELECT blockheight, hex(hash), endingtxid FROM BlockHistory ORDER BY rowid"
            >>= (@?= [["1", hashHex 1, "11"], ["2", hashHex 2, "12"]])

    , testCase "commitBlock with a duplicate TransactionIndex hash returns Left, throws nothing" $ withStore $ \_ ps -> do
        commit ps 1 [] [] ["h"]
        r <- try @SomeException $ _psCommitBlock ps (blockCommit 2 [] [] ["h"])
        case r of
            Right (Left SQ3.ErrorConstraint) -> return ()
            Right other -> assertFailure $ "expected Left ErrorConstraint, got " <> show other
            Left e -> assertFailure $ "threw " <> show e

    , testCase "rowsAtTxId returns rows in rowid order" $ withStore $ \_ ps -> do
        commit ps 1 ["t"] [("t", [("b", 7, "x"), ("a", 7, "y"), ("c", 8, "z"), ("d", 7, "w")])] []
        _psRowsAtTxId ps "t" 7 >>= (@?= Right [("b", "x"), ("a", "y"), ("d", "w")])
        _psRowsAtTxId ps "t" 9 >>= (@?= Right [])

    , testCase "rowsInTxIdRange: from inclusive, to exclusive; readPrev and readHistorical: newest below the bound" $ withStore $ \_ ps -> do
        commit ps 1 ["t"] [("t", [("a", 1, "a1"), ("a", 2, "a2"), ("b", 3, "b3"), ("a", 4, "a4")])] []
        Right r <- _psRowsInTxIdRange ps "t" 2 4
        sort r @?= [(2, "a", "a2"), (3, "b", "b3")]
        _psRowsInTxIdRange ps "t" 5 9 >>= (@?= Right [])
        _psReadPrev ps "t" "a" 4 >>= (@?= Right (Just "a2"))
        _psReadPrev ps "t" "a" 5 >>= (@?= Right (Just "a4"))
        _psReadPrev ps "t" "a" 1 >>= (@?= Right Nothing)
        _psReadHistorical ps "t" "a" 4 >>= (@?= Right (Just ("a", "a2")))
        _psReadHistorical ps "t" "b" 4 >>= (@?= Right (Just ("b", "b3")))
        _psReadHistorical ps "t" "b" 3 >>= (@?= Right Nothing)
        _psReadPrev ps "nope" "a" 4 >>= (@?= Left SQ3.ErrorError)

    , testCase "withBatch rolls back on exception; withReadSnapshot never persists" $ withStore $ \sql ps -> do
        r <- try @ErrorCall $ _psWithBatch ps $ do
            commit ps 1 ["t"] [("t", [("a", 1, "x")])] ["h"]
            throwIO (ErrorCall "abort") :: IO ()
        r @?= Left (ErrorCall "abort")
        _psGetLatestBlock ps >>= (@?= Nothing)
        _psTableInCatalog ps False "t" >>= (@?= Right False)
        seen <- _psWithReadSnapshot ps $ do
            commit ps 1 ["t"] [("t", [("a", 1, "x")])] ["h"]
            _psReadNewest ps "t" "a" Nothing
        seen @?= Right (Just "x")
        _psGetLatestBlock ps >>= (@?= Nothing)
        query sql 1 "SELECT COUNT(*) FROM TransactionIndex" >>= (@?= [["0"]])
        _psWithBatch ps $ commit ps 1 ["t"] [("t", [("a", 1, "x")])] ["h"]
        _psGetLatestBlock ps >>= (@?= Just (1, mkHash 1))
        _psTableInCatalog ps False "t" >>= (@?= Right True)
        _psTableInCatalog ps False "T" >>= (@?= Right False)
        _psTableInCatalog ps True "T" >>= (@?= Right True)
        _psTableExistsAt ps "T" 2 >>= (@?= Right True)
        _psTableExistsAt ps "t" 1 >>= (@?= Right False)

    , testCase "backupTo writes a file whose per-table dumps equal the source" $ withStore $ \sql ps ->
        runResourceT $ withTempDir "pact-store-backup" >>= \dir -> liftIO $ do
            commit ps 1 ["t1"] [("t1", [("a", 1, "x")])] ["h1"]
            commit ps 2 ["t2"] [("t2", [("b", 2, "y")]), ("t1", [("a", 2, "z")])] ["h2"]
            let file = dir <> "/backup.sqlite"
            _psBackupTo ps file
            source <- dumpAll sql
            copy <- withSQLiteConnection file chainwebPragmas dumpAll
            assertBool "the source has user tables" (length source > 9)
            copy @?= source
    ]

-- -------------------------------------------------------------------------- --
-- Utils

withStore :: (SQLiteEnv -> PactStore -> IO a) -> IO a
withStore k = withTempSQLiteConnection chainwebPragmas $ \sql -> do
    let ps = sqlitePactStore sql
    _psInitSchema ps
    k sql ps

mkHash :: Int -> BlockHash
mkHash n = either error id $ runGetEitherS decodeBlockHash (BS.replicate 32 (fromIntegral n))

hashHex :: Int -> Text
hashHex n = T.replicate 32 ("0" <> T.pack (show n)) -- n < 10

blockCommit
    :: BlockHeight -> [Text] -> [(Text, [(ByteString, Int, ByteString)])] -> [ByteString]
    -> BlockCommit
blockCommit h newTables rows txs = BlockCommit
    { _bcHeight = h
    , _bcHash = mkHash (fromIntegral h)
    , _bcEndTxId = fromIntegral (10 + h)
    , _bcNewTables = newTables
    , _bcRows = [ (t, [ (k, fromIntegral i, v) | (k, i, v) <- rs ]) | (t, rs) <- rows ]
    , _bcTxHashes = txs
    }

commit
    :: PactStore -> BlockHeight -> [Text] -> [(Text, [(ByteString, Int, ByteString)])] -> [ByteString]
    -> IO ()
commit ps h newTables rows txs =
    _psCommitBlock ps (blockCommit h newTables rows txs) >>= either (fail . show) return

-- | Run a query of @n@ columns, all rendered as text.
query :: SQLiteEnv -> Int -> SQ3.Utf8 -> IO [[Text]]
query sql n q = do
    rows <- P4SQL.qry_ sql q (replicate n P4SQL.RText)
    return $ fmap (fmap (\case P4SQL.SText (SQ3.Utf8 t) -> T.decodeUtf8 t; s -> error (show s))) rows

-- | Every table, every row, every column quoted, in rowid order.
dumpAll :: SQLiteEnv -> IO [(Text, [[Text]])]
dumpAll sql = do
    tables <- (\rs -> [ t | [P4SQL.SText t] <- rs ]) <$> P4SQL.qry_ sql
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name" [P4SQL.RText]
    forM tables $ \t@(SQ3.Utf8 tb) -> do
        cols <- (\rs -> [ c | [_, P4SQL.SText c] <- rs ]) <$> P4SQL.qry_ sql ("PRAGMA table_info('" <> t <> "')") [P4SQL.RInt, P4SQL.RText]
        rows <- P4SQL.qry_ sql
            ("SELECT " <> mconcat (intersperse ", " ("quote(rowid)" : fmap (\c -> "quote(" <> c <> ")") cols))
                <> " FROM [" <> t <> "] ORDER BY rowid")
            (P4SQL.RText <$ ("rowid" : cols))
        return (T.decodeUtf8 tb, fmap (fmap (\case P4SQL.SText (SQ3.Utf8 v) -> T.decodeUtf8 v; s -> error (show s))) rows)
