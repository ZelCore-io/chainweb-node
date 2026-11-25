{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.SQLite
-- License: See LICENSE file
--
-- The SQLite 'PactStore'. Every statement text is copied verbatim from the
-- 3.2.1 function that runs it, named next to each operation.
--
module Chainweb.Pact.Backend.Store.SQLite
    ( sqlitePactStore

      -- * Freezer (storage redesign P3, spec 6.2)
    , withSqliteReadOnly
    , openSqliteReadOnly
    , closeSqliteReadOnly
    , sqliteReadTransaction
    , sqliteTip
    , sqliteBlocks
    , sqliteBlockHashAt
    , sqliteBlockAtOrBelow
    , sqliteMutatedTables
    , sqliteMutatedTablesBetween
    , sqliteBlockRows
    , sqliteKeyRowCount
    , TrimRule(..)
    , sqliteTrimChunk
    , sqliteWithTrimSavepoint
    , sqliteExplainTrim
    ) where

import Control.Exception (SomeAsyncException, evaluate)
import Control.Lens (over, view, _Left)
import Control.Monad
import Control.Monad.Catch
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SB
import qualified Data.HashMap.Strict as HashMap
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Database.SQLite3.Direct (Utf8(..))
import qualified Database.SQLite3.Direct as SQ3
import GHC.Stack

import Chainweb.BlockHash
import Chainweb.BlockHeader
import Chainweb.BlockHeight
import Chainweb.Pact.Backend.SQLite.DirectV2 (SQLiteFlag(..), open_v2, close_v2)
import Chainweb.Pact.Backend.Store.Cold (StepRow(..))
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils
    (SavepointName(..), convSavepointName, expectSingle, tbl, toUtf8)
import Chainweb.Pact.Types (PactException(..), internalError)
import Chainweb.Utils (T2(..), T3(..), sshow, toText)
import Chainweb.Utils.Serialization (runGetEitherS, runPutS)
import Chainweb.Version

import qualified Pact.Types.Persistence as Pact4
import qualified Pact.Types.SQLite as Pact4

-- | The 'PactStore' of a SQLite database.
sqlitePactStore :: SQLiteEnv -> PactStore
sqlitePactStore db = PactStore
    { _psGetLatestBlock = getLatestBlock db
    , _psGetEarliestBlock = getEarliestBlock db
    , _psLookupBlock = lookupBlock db
    , _psGetBlockParent = \v cid -> getBlockParent v cid db
    , _psGetEndTxId = \msg -> getEndTxId msg db
    , _psGetEndTxId' = \msg -> getEndTxId' msg db
    , _psDoLookupSuccessful = doLookupSuccessful db
    , _psRewindDbTo = rewindDbTo db
    , _psBackupTo = backupTo db
    , _psInitSchema = initSchema db
    , _psReadNewest = readNewest db
    , _psRowsInTxIdRange = rowsInTxIdRange db
    , _psReadPrev = readPrev db
    , _psReadHistorical = readHistorical db
    , _psDistinctKeys = distinctKeys db
    , _psTxIdsAfter = txIdsAfter db
    , _psRowsAtTxId = rowsAtTxId db
    , _psTableExistsAt = tableExistsAt db
    , _psTableInCatalog = tableInCatalog db
    , _psCommitBlock = commitBlock db
    , _psWithBatch = withBatch db
    , _psWithReadSnapshot = withReadSnapshot db
    }

type TxId = Pact4.TxId

-- -------------------------------------------------------------------------- --
-- Statement helpers that return SQLite errors instead of throwing them
-- (copied from Chainweb.Pact5.Backend.ChainwebPactDb, over Pact 4's SType)

bindParams :: SQ3.Statement -> [Pact4.SType] -> ExceptT SQ3.Error IO ()
bindParams stmt as =
    forM_ (zip as [1..]) $ \(a,i) -> ExceptT $
      case a of
        Pact4.SInt n -> SQ3.bindInt64 stmt i n
        Pact4.SDouble n -> SQ3.bindDouble stmt i n
        Pact4.SText n -> SQ3.bindText stmt i n
        Pact4.SBlob n -> SQ3.bindBlob stmt i n

prepStmt :: HasCallStack => SQ3.Database -> SQ3.Utf8 -> ExceptT SQ3.Error IO SQ3.Statement
prepStmt c q = do
    r <- ExceptT $ SQ3.prepare c q
    case r of
        Nothing -> internalError "No SQL statements in prepared statement"
        Just s -> return s

qry :: SQ3.Database -> SQ3.Utf8 -> [Pact4.SType] -> [Pact4.RType] -> ExceptT SQ3.Error IO [[Pact4.SType]]
qry e q as rts = bracket (prepStmt e q) (ExceptT . SQ3.finalize) $ \stmt -> do
    bindParams stmt as
    reverse <$> stepStmt stmt rts

stepStmt :: SQ3.Statement -> [Pact4.RType] -> ExceptT SQ3.Error IO [[Pact4.SType]]
stepStmt stmt rts = do
    let acc rs SQ3.Done = return rs
        acc rs SQ3.Row = do
            as <- lift $ forM (zip rts [0..]) $ \(rt,ci) ->
                case rt of
                    Pact4.RInt -> Pact4.SInt <$> SQ3.columnInt64 stmt ci
                    Pact4.RDouble -> Pact4.SDouble <$> SQ3.columnDouble stmt ci
                    Pact4.RText -> Pact4.SText <$> SQ3.columnText stmt ci
                    Pact4.RBlob -> Pact4.SBlob <$> SQ3.columnBlob stmt ci
            sr <- ExceptT $ SQ3.step stmt
            acc (as:rs) sr
    sr <- ExceptT $ SQ3.step stmt
    acc [] sr

exec_ :: SQ3.Database -> SQ3.Utf8 -> ExceptT SQ3.Error IO ()
exec_ e q = ExceptT $ over _Left fst <$> SQ3.exec e q

txIdParam :: TxId -> Pact4.SType
txIdParam (Pact4.TxId t) = Pact4.SInt (fromIntegral t)

-- -------------------------------------------------------------------------- --
-- Row-level operations

-- | Pact 4 @doReadRow@ (bound: the rewound case) and Pact 5 @doReadRow@
-- (always bounded); the bounded texts are identical.
readNewest :: SQLiteEnv -> Text -> ByteString -> Maybe TxId -> IO (Either SQ3.Error (Maybe ByteString))
readNewest db tableName rowkey mlim = runExceptT $ do
    let blockLimitStmt = maybe "" (const " AND txid < ?") mlim
    let blockLimitParam = maybe [] (\txid -> [txIdParam txid]) mlim
    let queryStmt =
            "SELECT rowdata FROM " <> tbl (toUtf8 tableName) <> " WHERE rowkey = ?" <> blockLimitStmt
            <> " ORDER BY txid DESC LIMIT 1;"
    result <- qry db queryStmt ([Pact4.SText (Utf8 rowkey)] ++ blockLimitParam) [Pact4.RBlob]
    case result of
        [] -> return Nothing
        [[Pact4.SBlob a]] -> return (Just a)
        err -> internalError $
            "doReadRow: Expected (at most) a single result, but got: " <>
            T.pack (show err)

-- | @queryHistory@ of @getBlockHistory@.
rowsInTxIdRange :: SQLiteEnv -> Text -> TxId -> TxId -> IO (Either SQ3.Error [(TxId, ByteString, ByteString)])
rowsInTxIdRange db tableName s e = runExceptT $ do
    let sql = "SELECT txid, rowkey, rowdata FROM [" <> toUtf8 tableName <>
              "] WHERE txid >= ? AND txid < ?"
    r <- qry db sql
          [txIdParam s, txIdParam e]
          [Pact4.RInt, Pact4.RText, Pact4.RBlob]
    forM r $ \case
        [Pact4.SInt txid, Pact4.SText (Utf8 key), Pact4.SBlob value] ->
            return (fromIntegral txid, key, value)
        err -> internalError $
          "queryHistory: Expected single row with three columns as the \
          \result, got: " <> T.pack (show err)

-- | @queryPrev@ of @getBlockHistory@.
readPrev :: SQLiteEnv -> Text -> ByteString -> TxId -> IO (Either SQ3.Error (Maybe ByteString))
readPrev db tableName k s = runExceptT $ do
    let sql = "SELECT rowdata FROM [" <> toUtf8 tableName <>
              "] WHERE rowkey = ? AND txid < ? " <>
              "ORDER BY txid DESC LIMIT 1"
    r <- qry db sql
      [Pact4.SText (Utf8 k), txIdParam s]
      [Pact4.RBlob]
    case r of
      [] -> return Nothing
      [[Pact4.SBlob value]] -> return (Just value)
      _ -> internalError $ "queryPrev: expected 0 or 1 rows, got: " <> T.pack (show r)

-- | @queryHistoryLookup@ of @lookupHistorical@.
readHistorical :: SQLiteEnv -> Text -> ByteString -> TxId -> IO (Either SQ3.Error (Maybe (ByteString, ByteString)))
readHistorical db tableName k e = runExceptT $ do
    let sql = "SELECT rowKey, rowdata FROM [" <> toUtf8 tableName <>
              "] WHERE txid < ? AND rowkey = ? ORDER BY txid DESC LIMIT 1;"
    r <- qry db sql
      [txIdParam e, Pact4.SText (Utf8 k)]
      [Pact4.RText, Pact4.RBlob]
    case r of
      [[Pact4.SText (Utf8 key), Pact4.SBlob value]] -> return (Just (key, value))
      [] -> pure Nothing
      _ -> internalError $ "lookupHistorical: expected single-row result, got " <> sshow r

-- | Pact 4 @doKeys@ ('KeysAsStored') and Pact 5 @doKeys@ ('KeysOrderedByKey').
distinctKeys :: SQLiteEnv -> KeysQuery -> Text -> IO (Either SQ3.Error [ByteString])
distinctKeys db q tn = runExceptT $ do
    ks <- case q of
        KeysAsStored mlim -> do
            let blockLimitStmt = maybe "" (const " WHERE txid < ?;") mlim
            let blockLimitParam = maybe [] (\txid -> [txIdParam txid]) mlim
            qry db ("SELECT DISTINCT rowkey FROM " <> tbl (toUtf8 tn) <> blockLimitStmt) blockLimitParam [Pact4.RText]
        KeysOrderedByKey txid ->
            qry db
                ("SELECT DISTINCT rowkey FROM " <> tbl (toUtf8 tn) <> "WHERE txid < ? ORDER BY rowkey;")
                [txIdParam txid] [Pact4.RText]
    forM ks $ \case
        [Pact4.SText (Utf8 k)] -> return k
        _ -> internalError "doKeys: The impossible happened."

-- | Pact 4 @doTxIds@.
txIdsAfter :: SQLiteEnv -> Text -> TxId -> IO (Either SQ3.Error [TxId])
txIdsAfter db tn tid = runExceptT $ do
    rows <- qry db stmt [txIdParam tid] [Pact4.RInt]
    forM rows $ \case
        [Pact4.SInt tid'] -> return $ Pact4.TxId (fromIntegral tid')
        _ -> internalError "doTxIds: the impossible happened"
  where
    stmt = "SELECT DISTINCT txid FROM " <> tbl (toUtf8 tn) <> " WHERE txid > ?"

-- | @readFromDb@ of Pact 4 @doGetTxLog@.
rowsAtTxId :: SQLiteEnv -> Text -> TxId -> IO (Either SQ3.Error [(ByteString, ByteString)])
rowsAtTxId db tableName txid = runExceptT $ do
    rows <- qry db stmt [txIdParam txid] [Pact4.RText, Pact4.RBlob]
    forM rows $ \case
        [Pact4.SText (Utf8 key), Pact4.SBlob value] -> return (key, value)
        err -> internalError $
          "readHistoryResult: Expected single row with two columns as the \
          \result, got: " <> T.pack (show err)
  where
    stmt = "SELECT rowkey, rowdata FROM " <> tbl (toUtf8 tableName) <> " WHERE txid = ?"

-- | Pact 4 @tableExistsInDbAtHeight@ and Pact 5 @checkTableExistsInDb@.
tableExistsAt :: SQLiteEnv -> Text -> BlockHeight -> IO (Either SQ3.Error Bool)
tableExistsAt db tableName bh = runExceptT $ do
    let tableExistsStmt =
          -- table names are compared case-insensitively: lower(...) on both sides
          "SELECT tablename FROM VersionedTableCreation WHERE createBlockheight < ? AND lower(tablename) = lower(?)"
    qry db tableExistsStmt [Pact4.SInt $ max 0 (fromIntegral bh), Pact4.SText (toUtf8 tableName)] [Pact4.RText] >>= \case
        [] -> return False
        _ -> return True

-- | @inDb@ of Pact 4 @doCreateUserTable@: true when exactly one table
-- matches. With an exact match the matched name equals the argument, so this
-- is the 3.2.1 test @lcTables || rname == t@.
tableInCatalog :: SQLiteEnv -> Bool -> Text -> IO (Either SQ3.Error Bool)
tableInCatalog db lcTables t = runExceptT $ do
    r <- qry db (tableLookupStmt lcTables) [Pact4.SText (toUtf8 t)] [Pact4.RText]
    case r of
        [[Pact4.SText _]] -> return True
        _ -> return False
  where
    tableLookupStmt False =
      "SELECT name FROM sqlite_master WHERE type='table' and name=?;"
    tableLookupStmt True =
      "SELECT name FROM sqlite_master WHERE type='table' and lower(name)=lower(?);"

-- -------------------------------------------------------------------------- --
-- Statement helpers of 'commitBlock': Pact 5's @exec_@, @exec'@ and
-- @execMulti@ (the same SQLite calls in the same order, so the same
-- 'SQ3.Error'), with the error tagged by the statement and the failing call

commitExec_ :: SQ3.Database -> CommitStatement -> SQ3.Utf8 -> ExceptT CommitError IO ()
commitExec_ e s q = ExceptT $ over _Left (\(err, msg) -> CommitError s (CommitExec msg) err) <$> SQ3.exec e q

commitPrep :: SQ3.Database -> CommitStatement -> SQ3.Utf8 -> ExceptT CommitError IO SQ3.Statement
commitPrep e s q = withExceptT (CommitError s CommitPrepare) (prepStmt e q)

commitBind :: CommitStatement -> SQ3.Statement -> [Pact4.SType] -> ExceptT CommitError IO ()
commitBind s stmt as = withExceptT (CommitError s CommitBind) (bindParams stmt as)

commitCall :: CommitStatement -> CommitPhase -> IO (Either SQ3.Error a) -> ExceptT CommitError IO a
commitCall s p act = ExceptT $ over _Left (CommitError s p) <$> act

-- | Pact 5's @exec'@: an error of the finalize wins over the step's, as there.
commitExec' :: SQ3.Database -> CommitStatement -> SQ3.Utf8 -> [Pact4.SType] -> ExceptT CommitError IO ()
commitExec' e s q as =
    bracket (commitPrep e s q) (commitCall s CommitFinalize . SQ3.finalize) $ \stmt -> do
        commitBind s stmt as
        void $ commitCall s CommitStep (SQ3.step stmt)

-- | Pact 5's @execMulti@: the result of the finalize is ignored, as there.
commitExecMulti :: SQ3.Database -> CommitStatement -> SQ3.Utf8 -> [[Pact4.SType]] -> ExceptT CommitError IO ()
commitExecMulti c s q rows = bracket (commitPrep c s q) (lift . SQ3.finalize) $ \stmt -> do
    forM_ rows $ \row -> do
        commitCall s CommitReset (SQ3.reset stmt)
        lift $ SQ3.clearBindings stmt
        commitBind s stmt row
        commitCall s CommitStep (SQ3.step stmt)

-- | @commitBlockStateToDatabase@ of Pact 4 and Pact 5, which run the same
-- statements in the same order. The two @TransactionIndex@ texts differ only
-- in whitespace; this is Pact 5's.
commitBlock :: SQLiteEnv -> BlockCommit -> IO (Either CommitError ())
commitBlock db bc = runExceptT $ do
    mapM_ (createUserTable . toUtf8) (_bcNewTables bc)
    mapM_ (\(t, rows) -> writeTable (toUtf8 t) (prepRow <$> rows)) (_bcRows bc)
    indexPendingPactTransactions
    blockHistoryInsert (_bcEndTxId bc)
  where
    bh = _bcHeight bc
    hsh = _bcHash bc

    prepRow (rowkey, txid, rowdata) =
        [ Pact4.SText (Utf8 rowkey)
        , txIdParam txid
        , Pact4.SBlob rowdata
        ]

    writeTable tableName writes = do
        commitExecMulti db CommitRows q writes
        markTableMutation tableName bh
      where
        q = "INSERT OR REPLACE INTO " <> tbl tableName <> "(rowkey,txid,rowdata) VALUES(?,?,?)"

    -- Mark the table as being mutated during this block, so that we know
    -- to delete from it if we rewind past this block.
    markTableMutation tablename blockheight =
        commitExec' db CommitTableMutation mutq [Pact4.SText tablename, Pact4.SInt (fromIntegral blockheight)]
      where
        mutq = "INSERT OR IGNORE INTO VersionedTableMutation VALUES (?,?);"

    -- Record a block as being in the history of the checkpointer.
    blockHistoryInsert t =
        commitExec' db CommitBlockHistory stmt
            [ Pact4.SInt (fromIntegral bh)
            , Pact4.SBlob (runPutS (encodeBlockHash hsh))
            , txIdParam t
            ]
      where
        stmt = "INSERT INTO BlockHistory ('blockheight','hash','endingtxid') VALUES (?,?,?);"

    createUserTable tablename = do
        let (createtablestmt, indexcreationstmt) = versionedTableStmts tablename
        commitExec_ db CommitCreateTable createtablestmt
        commitExec_ db CommitCreateIndex indexcreationstmt
        markTableCreation tablename

    -- Mark the table as being created during this block, so that we know
    -- to drop it if we rewind past this block.
    markTableCreation tablename =
        commitExec' db CommitTableCreation insertstmt insertargs
      where
        insertstmt = "INSERT OR IGNORE INTO VersionedTableCreation VALUES (?,?)"
        insertargs = [Pact4.SText tablename, Pact4.SInt (fromIntegral bh)]

    -- Commit the index of pending successful transactions to the database
    indexPendingPactTransactions = do
        let rows = map toRow (_bcTxHashes bc)
        let q = "INSERT INTO TransactionIndex (txhash, blockheight) VALUES (?, ?)"
        commitExecMulti db CommitTxIndex q rows
      where
        toRow b = [Pact4.SBlob b, Pact4.SInt (fromIntegral bh)]

createVersionedTable :: SQLiteEnv -> SQ3.Utf8 -> ExceptT SQ3.Error IO ()
createVersionedTable db tablename = do
    exec_ db createtablestmt
    exec_ db indexcreationstmt
  where
    (createtablestmt, indexcreationstmt) = versionedTableStmts tablename

-- | The @CREATE TABLE@ and @CREATE INDEX@ statements of a versioned table.
versionedTableStmts :: SQ3.Utf8 -> (SQ3.Utf8, SQ3.Utf8)
versionedTableStmts tablename = (createtablestmt, indexcreationstmt)
  where
    ixName = tablename <> "_ix"
    createtablestmt =
        "CREATE TABLE IF NOT EXISTS " <> tbl tablename <> " \
            \ (rowkey TEXT\
            \, txid UNSIGNED BIGINT NOT NULL\
            \, rowdata BLOB NOT NULL\
            \, UNIQUE (rowkey, txid));"
    indexcreationstmt =
        "CREATE INDEX IF NOT EXISTS " <> tbl ixName <> " ON " <> tbl tablename <> "(txid DESC);"

-- -------------------------------------------------------------------------- --
-- Transactions

beginSavepoint :: SQLiteEnv -> SavepointName -> IO ()
beginSavepoint db name =
  Pact4.exec_ db $ "SAVEPOINT [" <> convSavepointName name <> "];"

commitSavepoint :: SQLiteEnv -> SavepointName -> IO ()
commitSavepoint db name =
  Pact4.exec_ db $ "RELEASE SAVEPOINT [" <> convSavepointName name <> "];"

rollbackSavepoint :: SQLiteEnv -> SavepointName -> IO ()
rollbackSavepoint db name =
  Pact4.exec_ db $ "ROLLBACK TRANSACTION TO SAVEPOINT [" <> convSavepointName name <> "];"

abortSavepoint :: SQLiteEnv -> SavepointName -> IO ()
abortSavepoint db name = do
  rollbackSavepoint db name
  commitSavepoint db name

-- | Moved from "Chainweb.Pact.Backend.Utils" with the savepoint statements it
-- runs. Used by 'initSchema'.
withSavepoint
    :: SQLiteEnv
    -> SavepointName
    -> IO a
    -> IO a
withSavepoint db name action = mask $ \resetMask -> do
    beginSavepoint db name
    go resetMask `catches` handlers
  where
    go resetMask = do
        r <- resetMask action `onException` abortSavepoint db name
        commitSavepoint db name
        evaluate r
    throwErr s = internalError $ "withSavepoint (" <> toText name <> "): " <> s
    handlers = [ Handler $ \(e :: PactException) -> throwErr (sshow e)
               , Handler $ \(e :: SomeAsyncException) -> throwM e
               , Handler $ \(e :: SomeException) -> throwErr ("non-pact exception: " <> sshow e)
               ]

-- | The savepoint of @restoreAndSave@.
withBatch :: SQLiteEnv -> IO a -> IO a
withBatch db act = fmap fst $ generalBracket
    (beginSavepoint db BatchSavepoint)
    (\_ -> \case
        ExitCaseSuccess {} -> commitSavepoint db BatchSavepoint
        _ -> abortSavepoint db BatchSavepoint
    ) $ \_ -> act

-- | The savepoint of @readFrom@.
withReadSnapshot :: SQLiteEnv -> IO a -> IO a
withReadSnapshot db act = bracket
    (beginSavepoint db BatchSavepoint)
    (\_ -> abortSavepoint db BatchSavepoint) $ \() -> act

-- -------------------------------------------------------------------------- --
-- Whole operations (Chainweb.Pact.PactService.Checkpointer.Internal,
-- Chainweb.Pact.Backend.Utils, Chainweb.Pact5.Backend.ChainwebPactDb,
-- Chainweb.Backup)

getEarliestBlock :: HasCallStack => SQLiteEnv -> IO (Maybe (BlockHeight, BlockHash))
getEarliestBlock db = do
  r <- Pact4.qry_ db qtext [Pact4.RInt, Pact4.RBlob] >>= mapM go
  case r of
    [] -> return Nothing
    (!o:_) -> return (Just o)
  where
    qtext = "SELECT blockheight, hash FROM BlockHistory ORDER BY blockheight ASC LIMIT 1"

    go [Pact4.SInt hgt, Pact4.SBlob blob] =
        let hash = either error id $ runGetEitherS decodeBlockHash blob
        in return (fromIntegral hgt, hash)
    go _ = fail "Chainweb.Pact.Backend.RelationalCheckpointer.doGetEarliest: impossible. This is a bug in chainweb-node."

getLatestBlock :: HasCallStack => SQLiteEnv -> IO (Maybe (BlockHeight, BlockHash))
getLatestBlock db = do
  r <- Pact4.qry_ db qtext [Pact4.RInt, Pact4.RBlob] >>= mapM go
  case r of
    [] -> return Nothing
    (!o:_) -> return (Just o)
  where
    qtext = "SELECT blockheight, hash FROM BlockHistory ORDER BY blockheight DESC LIMIT 1"

    go [Pact4.SInt hgt, Pact4.SBlob blob] =
        let hash = either error id $ runGetEitherS decodeBlockHash blob
        in return (fromIntegral hgt, hash)
    go _ = fail "Chainweb.Pact.Backend.RelationalCheckpointer.getLatest: impossible. This is a bug in chainweb-node."

lookupBlock :: SQLiteEnv -> (BlockHeight, BlockHash) -> IO Bool
lookupBlock db (bheight, bhash) = do
    r <- Pact4.qry db qtext [Pact4.SInt $ fromIntegral bheight, Pact4.SBlob (runPutS (encodeBlockHash bhash))]
                      [Pact4.RInt]
    expectSingle "row" r >>= \case
        [Pact4.SInt n] -> return $! n == 1
        _ -> internalError "lookupBlock: output type mismatch"
  where
    qtext = "SELECT COUNT(*) FROM BlockHistory WHERE blockheight = ? AND hash = ?;"

getBlockParent :: ChainwebVersion -> ChainId -> SQLiteEnv -> (BlockHeight, BlockHash) -> IO (Maybe BlockHash)
getBlockParent v cid db (bh, hash)
    | bh == genesisHeight v cid = return Nothing
    | otherwise = do
        blockFound <- lookupBlock db (bh, hash)
        if not blockFound
          then return Nothing
          else do
            r <- Pact4.qry db qtext [Pact4.SInt (fromIntegral (pred bh))] [Pact4.RBlob]
            case r of
              [[Pact4.SBlob blob]] ->
                either (internalError . T.pack) (return . return) $! runGetEitherS decodeBlockHash blob
              [] -> internalError "getBlockParent: block was found but its parent couldn't be found"
              _ -> error "getBlockParent: output type mismatch"
  where
    qtext = "SELECT hash FROM BlockHistory WHERE blockheight = ?"

doLookupSuccessful
    :: SQLiteEnv -> BlockHeight -> V.Vector SB.ShortByteString
    -> IO (HashMap.HashMap SB.ShortByteString (T2 BlockHeight BlockHash))
doLookupSuccessful db curHeight hashes = do
  fmap buildResultMap $ do -- swizzle results of query into a HashMap
      let
        hss = V.toList hashes
        params = BS.intercalate "," (map (const "?") hss)
        qtext = Utf8 $ BS.intercalate " "
            [ "SELECT blockheight, hash, txhash"
            , "FROM TransactionIndex"
            , "INNER JOIN BlockHistory USING (blockheight)"
            , "WHERE txhash IN (" <> params <> ")" <> " AND blockheight < ?;"
            ]
        qvals
          -- match query params above. first, hashes
          = map (\h -> Pact4.SBlob $ SB.fromShort h) hss
          -- then, the block height; we don't want to see txs from the
          -- current block in the db, because they'd show up in pending data
          ++ [Pact4.SInt $ fromIntegral curHeight]

      Pact4.qry db qtext qvals [Pact4.RInt, Pact4.RBlob, Pact4.RBlob] >>= mapM go
  where
    buildResultMap :: [T3 SB.ShortByteString BlockHeight BlockHash] -> HashMap.HashMap SB.ShortByteString (T2 BlockHeight BlockHash)
    buildResultMap xs = HashMap.fromList $
      map (\(T3 txhash blockheight blockhash) -> (txhash, T2 blockheight blockhash)) xs

    go :: [Pact4.SType] -> IO (T3 SB.ShortByteString BlockHeight BlockHash)
    go (Pact4.SInt blockheight:Pact4.SBlob blockhash:Pact4.SBlob txhash:_) = do
        !blockhash' <- either fail return $ runGetEitherS decodeBlockHash blockhash
        let !txhash' = SB.toShort txhash
        return $! T3 txhash' (fromIntegral blockheight) blockhash'
    go _ = fail "impossible"

getEndTxId :: Text -> SQLiteEnv -> Maybe ParentHeader -> IO (Historical TxId)
getEndTxId msg sql pc = case pc of
    Nothing -> return (Historical 0)
    Just (ParentHeader ph) -> getEndTxId' msg sql (view blockHeight ph) (view blockHash ph)

getEndTxId' :: Text -> SQLiteEnv -> BlockHeight -> BlockHash -> IO (Historical TxId)
getEndTxId' msg sql bh bhsh = do
    r <- Pact4.qry sql
      "SELECT endingtxid FROM BlockHistory WHERE blockheight = ? and hash = ?;"
      [ Pact4.SInt $ fromIntegral bh
      , Pact4.SBlob $ runPutS (encodeBlockHash bhsh)
      ]
      [Pact4.RInt]
    case r of
      [[Pact4.SInt tid]] -> return $ Historical (Pact4.TxId (fromIntegral tid))
      [] -> return NoHistory
      _ -> internalError $ msg <> ".getEndTxId: expected single-row int result, got " <> sshow r

-- | Delete any state from the database newer than the input parent header.
-- Returns the ending txid of the input parent header.
rewindDbTo
    :: SQLiteEnv
    -> Maybe ParentHeader
    -> IO TxId
rewindDbTo db Nothing = do
  rewindDbToGenesis db
  return 0
rewindDbTo db mh@(Just (ParentHeader ph)) = do
    !historicalEndingTxId <- getEndTxId "rewindDbToBlock" db mh
    endingTxId <- case historicalEndingTxId of
      NoHistory ->
        throwM
          $ BlockHeaderLookupFailure
          $ "rewindDbTo.getEndTxId: not in db: "
          <> sshow ph
      Historical endingTxId ->
        return endingTxId
    rewindDbToBlock db (view blockHeight ph) endingTxId
    return endingTxId

-- rewind before genesis, delete all user tables and all rows in all tables
rewindDbToGenesis
  :: SQLiteEnv
  -> IO ()
rewindDbToGenesis db = do
    Pact4.exec_ db "DELETE FROM BlockHistory;"
    Pact4.exec_ db "DELETE FROM [SYS:KeySets];"
    Pact4.exec_ db "DELETE FROM [SYS:Modules];"
    Pact4.exec_ db "DELETE FROM [SYS:Namespaces];"
    Pact4.exec_ db "DELETE FROM [SYS:Pacts];"
    Pact4.exec_ db "DELETE FROM [SYS:ModuleSources];"
    tblNames <- Pact4.qry_ db "SELECT tablename FROM VersionedTableCreation;" [Pact4.RText]
    forM_ tblNames $ \t -> case t of
      [Pact4.SText tn] -> Pact4.exec_ db ("DROP TABLE [" <> tn <> "];")
      _ -> internalError "Something went wrong when resetting tables."
    Pact4.exec_ db "DELETE FROM VersionedTableCreation;"
    Pact4.exec_ db "DELETE FROM VersionedTableMutation;"
    Pact4.exec_ db "DELETE FROM TransactionIndex;"

-- | Rewind the database to a particular block, given the end tx id of that
-- block.
rewindDbToBlock
  :: SQLiteEnv
  -> BlockHeight
  -> TxId
  -> IO ()
rewindDbToBlock db bh endingTxId = do
    tableMaintenanceRowsVersionedSystemTables
    droppedtbls <- dropTablesAtRewind
    vacuumTablesAtRewind droppedtbls
    deleteHistory
    clearTxIndex
  where
    dropTablesAtRewind :: IO (HashSet BS.ByteString)
    dropTablesAtRewind = do
        toDropTblNames <- Pact4.qry db findTablesToDropStmt
                          [Pact4.SInt (fromIntegral bh)] [Pact4.RText]
        tbls <- fmap HashSet.fromList . forM toDropTblNames $ \case
            [Pact4.SText tblname@(Utf8 tn)] -> do
                Pact4.exec_ db $ "DROP TABLE IF EXISTS " <> tbl tblname
                return tn
            _ -> internalError rewindmsg
        Pact4.exec' db
            "DELETE FROM VersionedTableCreation WHERE createBlockheight > ?"
            [Pact4.SInt (fromIntegral bh)]
        return tbls
    findTablesToDropStmt =
      "SELECT tablename FROM VersionedTableCreation WHERE createBlockheight > ?;"
    rewindmsg =
      "rewindBlock: dropTablesAtRewind: Couldn't resolve the name of the table to drop."

    deleteHistory :: IO ()
    deleteHistory =
        Pact4.exec' db "DELETE FROM BlockHistory WHERE blockheight > ?"
              [Pact4.SInt (fromIntegral bh)]

    vacuumTablesAtRewind :: HashSet BS.ByteString -> IO ()
    vacuumTablesAtRewind droppedtbls = do
        let processMutatedTables ms = fmap HashSet.fromList . forM ms $ \case
              [Pact4.SText (Utf8 tn)] -> return tn
              _ -> internalError "rewindBlock: vacuumTablesAtRewind: Couldn't resolve the name \
                                 \of the table to possibly vacuum."
        mutatedTables <- Pact4.qry db
            "SELECT DISTINCT tablename FROM VersionedTableMutation WHERE blockheight > ?;"
          [Pact4.SInt (fromIntegral bh)]
          [Pact4.RText]
          >>= processMutatedTables
        let toVacuumTblNames = HashSet.difference mutatedTables droppedtbls
        forM_ toVacuumTblNames $ \tblname ->
            Pact4.exec' db ("DELETE FROM " <> tbl (Utf8 tblname) <> " WHERE txid >= ?")
                  [Pact4.SInt $! fromIntegral endingTxId]
        Pact4.exec' db "DELETE FROM VersionedTableMutation WHERE blockheight > ?;"
              [Pact4.SInt (fromIntegral bh)]

    tableMaintenanceRowsVersionedSystemTables :: IO ()
    tableMaintenanceRowsVersionedSystemTables = do
        Pact4.exec' db "DELETE FROM [SYS:KeySets] WHERE txid >= ?" tx
        Pact4.exec' db "DELETE FROM [SYS:Modules] WHERE txid >= ?" tx
        Pact4.exec' db "DELETE FROM [SYS:Namespaces] WHERE txid >= ?" tx
        Pact4.exec' db "DELETE FROM [SYS:Pacts] WHERE txid >= ?" tx
        Pact4.exec' db "DELETE FROM [SYS:ModuleSources] WHERE txid >= ?" tx
      where
        tx = [Pact4.SInt $! fromIntegral endingTxId]

    -- | Delete all future transactions from the index
    clearTxIndex :: IO ()
    clearTxIndex =
        Pact4.exec' db "DELETE FROM TransactionIndex WHERE blockheight > ?;"
              [ Pact4.SInt (fromIntegral bh) ]

-- | @doBackup@ of "Chainweb.Backup", for this database.
backupTo :: SQLiteEnv -> FilePath -> IO ()
backupTo db path =
    void $ Pact4.qry db
        ("VACUUM main INTO ?")
        [Pact4.SText $ fromString path]
        []

-- | The error of a failed schema creation. It displays like Pact 5's
-- @InternalDbException@, which 3.2.1's @initSchema@ throws.
data StoreDbException = StoreDbException CallStack Text

instance Show StoreDbException where
    show = displayException

instance Exception StoreDbException where
    displayException (StoreDbException stack text) =
        T.unpack text <> "\n\n" <>
        prettyCallStack stack

-- | Pact 5's @internalDbError@ of 3.2.1, throwing 'StoreDbException'.
internalDbError :: HasCallStack => MonadThrow m => Text -> m a
internalDbError = throwM . StoreDbException callStack

-- | Pact 5's @throwOnDbError@ of 3.2.1. Top level with 'HasCallStack', like
-- 3.2.1's pair, so that a failure shows the @internalDbError@ and
-- @throwOnDbError@ frames that 3.2.1 showed.
throwOnDbError :: (HasCallStack, MonadThrow m) => ExceptT SQ3.Error m a -> m a
throwOnDbError act = runExceptT act >>= either (internalDbError . sshow) return

-- | Pact 5 @initSchema@: create all tables that exist pre-genesis.
initSchema :: SQLiteEnv -> IO ()
initSchema sql =
    withSavepoint sql DbTransaction $ throwOnDbError $ do
        createBlockHistoryTable
        createTableCreationTable
        createTableMutationTable
        createTransactionIndexTable
        create "SYS:KeySets"
        create "SYS:Modules"
        create "SYS:Namespaces"
        create "SYS:Pacts"
        create "SYS:ModuleSources"
  where
    create tablename = do
      createVersionedTable sql tablename

    createBlockHistoryTable :: ExceptT SQ3.Error IO ()
    createBlockHistoryTable =
      exec_ sql
        "CREATE TABLE IF NOT EXISTS BlockHistory \
        \(blockheight UNSIGNED BIGINT NOT NULL,\
        \ hash BLOB NOT NULL,\
        \ endingtxid UNSIGNED BIGINT NOT NULL, \
        \ CONSTRAINT blockHashConstraint UNIQUE (blockheight));"

    createTableCreationTable :: ExceptT SQ3.Error IO ()
    createTableCreationTable =
      exec_ sql
        "CREATE TABLE IF NOT EXISTS VersionedTableCreation\
        \(tablename TEXT NOT NULL\
        \, createBlockheight UNSIGNED BIGINT NOT NULL\
        \, CONSTRAINT creation_unique UNIQUE(createBlockheight, tablename));"

    createTableMutationTable :: ExceptT SQ3.Error IO ()
    createTableMutationTable =
      exec_ sql
        "CREATE TABLE IF NOT EXISTS VersionedTableMutation\
         \(tablename TEXT NOT NULL\
         \, blockheight UNSIGNED BIGINT NOT NULL\
         \, CONSTRAINT mutation_unique UNIQUE(blockheight, tablename));"

    createTransactionIndexTable :: ExceptT SQ3.Error IO ()
    createTransactionIndexTable = do
      exec_ sql
        "CREATE TABLE IF NOT EXISTS TransactionIndex \
         \ (txhash BLOB NOT NULL, \
         \ blockheight UNSIGNED BIGINT NOT NULL, \
         \ CONSTRAINT transactionIndexConstraint UNIQUE(txhash));"
      exec_ sql
        "CREATE INDEX IF NOT EXISTS \
         \ transactionIndexByBH ON TransactionIndex(blockheight)";

-- -------------------------------------------------------------------------- --
-- Freezer (storage redesign P3, spec 6.2)
--
-- The freezer's statements. Reads run on a read-only connection of their
-- own; the trim statements run on the Pact connection.

freezerQry :: SQLiteEnv -> SQ3.Utf8 -> [Pact4.SType] -> [Pact4.RType] -> IO [[Pact4.SType]]
freezerQry db q as rts = throwOnDbError (qry db q as rts)

-- | A read-only connection (@SQLITE_OPEN_READONLY@) with a busy timeout of
-- 10 s, as @cw-measure cold@ opens it.
withSqliteReadOnly :: FilePath -> (SQLiteEnv -> IO a) -> IO a
withSqliteReadOnly path = bracket (openSqliteReadOnly path) closeSqliteReadOnly

-- | The open of 'withSqliteReadOnly', for callers that open under a lock
-- and use the connection outside it.
openSqliteReadOnly :: FilePath -> IO SQLiteEnv
openSqliteReadOnly path = open
  where
    sqliteOpenReadOnly = SQLiteFlag 0x00000001
    open = open_v2 (fromString path) sqliteOpenReadOnly Nothing >>= \case
        Left (err, msg) -> internalError $
            "withSqliteReadOnly: can't open " <> T.pack path <> ": " <> sshow err <> ": " <> sshow msg
        Right db -> do
            -- wait instead of failing with SQLITE_BUSY while another
            -- connection holds a lock
            Pact4.exec_ db "PRAGMA busy_timeout = 10000" `onException` close_v2 db
            return db

closeSqliteReadOnly :: SQLiteEnv -> IO ()
closeSqliteReadOnly = void . close_v2

-- | One read transaction: @BEGIN@, then @COMMIT@ (@ROLLBACK@ on an
-- exception).
sqliteReadTransaction :: SQLiteEnv -> IO a -> IO a
sqliteReadTransaction db act = mask $ \restore -> do
    Pact4.exec_ db "BEGIN"
    r <- restore act `onException` Pact4.exec_ db "ROLLBACK"
    Pact4.exec_ db "COMMIT"
    return r

blockRow :: [Pact4.SType] -> IO (BlockHeight, BlockHash, TxId)
blockRow = \case
    [Pact4.SInt h, Pact4.SBlob hash, Pact4.SInt e] -> do
        bh <- either (internalError . T.pack) return $ runGetEitherS decodeBlockHash hash
        return (fromIntegral h, bh, fromIntegral e)
    r -> internalError $ "freezer: bad BlockHistory row: " <> sshow r

-- | The newest @BlockHistory@ row.
sqliteTip :: SQLiteEnv -> IO (Maybe (BlockHeight, BlockHash, TxId))
sqliteTip db = freezerQry db
    "SELECT blockheight, hash, endingtxid FROM BlockHistory ORDER BY blockheight DESC LIMIT 1"
    [] [Pact4.RInt, Pact4.RBlob, Pact4.RInt]
    >>= traverse blockRow . take 1 >>= \case
        [] -> return Nothing
        (x : _) -> return (Just x)

-- | The @BlockHistory@ rows with @from < height <= to@, ascending;
-- @Nothing@ for @from@ starts at the first block.
sqliteBlocks :: SQLiteEnv -> Maybe BlockHeight -> BlockHeight -> IO [(BlockHeight, BlockHash, TxId)]
sqliteBlocks db from to = case from of
    Nothing -> freezerQry db
        "SELECT blockheight, hash, endingtxid FROM BlockHistory WHERE blockheight <= ? ORDER BY blockheight ASC"
        [height to] cols >>= traverse blockRow
    Just f -> freezerQry db
        "SELECT blockheight, hash, endingtxid FROM BlockHistory WHERE blockheight > ? AND blockheight <= ? ORDER BY blockheight ASC"
        [height f, height to] cols >>= traverse blockRow
  where
    cols = [Pact4.RInt, Pact4.RBlob, Pact4.RInt]

height :: BlockHeight -> Pact4.SType
height = Pact4.SInt . fromIntegral

-- | The block hash of @BlockHistory@ at a height.
sqliteBlockHashAt :: SQLiteEnv -> BlockHeight -> IO (Maybe BlockHash)
sqliteBlockHashAt db h = freezerQry db
    "SELECT blockheight, hash, endingtxid FROM BlockHistory WHERE blockheight = ?"
    [height h] [Pact4.RInt, Pact4.RBlob, Pact4.RInt] >>= \case
        [] -> return Nothing
        (r : _) -> (\(_, hash, _) -> Just hash) <$> blockRow r

-- | The newest height at or below @h@ whose @endingtxid@ is at or below the
-- txid: the last block before a txid range that ends in block @h@. A
-- backwards walk of the height index that stops at the first match.
sqliteBlockAtOrBelow :: SQLiteEnv -> BlockHeight -> TxId -> IO (Maybe BlockHeight)
sqliteBlockAtOrBelow db h txid = freezerQry db
    "SELECT blockheight FROM BlockHistory WHERE blockheight <= ? AND endingtxid <= ? ORDER BY blockheight DESC LIMIT 1"
    [height h, txIdParam txid] [Pact4.RInt] >>= \case
        [[Pact4.SInt x]] -> return (Just (fromIntegral x))
        _ -> return Nothing

-- | The tables of @VersionedTableMutation@ at one height, named as stored.
sqliteMutatedTables :: SQLiteEnv -> BlockHeight -> IO [Text]
sqliteMutatedTables db h = freezerQry db
    "SELECT tablename FROM VersionedTableMutation WHERE blockheight = ? ORDER BY rowid"
    [height h] [Pact4.RText] >>= traverse freezerTableName

-- | The distinct tables of @VersionedTableMutation@ with
-- @from < height <= to@ (@Nothing@: from the first block), named as stored.
sqliteMutatedTablesBetween :: SQLiteEnv -> Maybe BlockHeight -> BlockHeight -> IO [Text]
sqliteMutatedTablesBetween db from to = case from of
    Nothing -> freezerQry db
        "SELECT DISTINCT tablename FROM VersionedTableMutation WHERE blockheight <= ?"
        [height to] [Pact4.RText] >>= traverse freezerTableName
    Just f -> freezerQry db
        "SELECT DISTINCT tablename FROM VersionedTableMutation WHERE blockheight > ? AND blockheight <= ?"
        [height f, height to] [Pact4.RText] >>= traverse freezerTableName

freezerTableName :: [Pact4.SType] -> IO Text
freezerTableName = \case
    [Pact4.SText (Utf8 t)] -> either (internalError . sshow) return (T.decodeUtf8' t)
    r -> internalError $ "freezer: bad table name row: " <> sshow r

-- | The rows of a table with @from <= txid < to@, in rowid order (spec 6.2
-- step 1). Row keys and row data are the stored bytes.
sqliteBlockRows :: SQLiteEnv -> Text -> TxId -> TxId -> IO [StepRow]
sqliteBlockRows db t from to = do
    rows <- freezerQry db
        ("SELECT rowid, rowkey, txid, rowdata FROM " <> tbl (toUtf8 t) <> " WHERE txid >= ? AND txid < ? ORDER BY rowid")
        [txIdParam from, txIdParam to]
        [Pact4.RInt, Pact4.RText, Pact4.RInt, Pact4.RBlob]
    forM rows $ \case
        [_, Pact4.SText (Utf8 k), Pact4.SInt x, Pact4.SBlob v] -> return (StepRow k (fromIntegral x) v)
        r -> internalError $ "freezer: bad row of " <> t <> ": " <> sshow r

-- | The number of rows of a key with @from <= txid < to@.
sqliteKeyRowCount :: SQLiteEnv -> Text -> ByteString -> TxId -> TxId -> IO Int
sqliteKeyRowCount db t k from to = freezerQry db
    ("SELECT count(*) FROM " <> tbl (toUtf8 t) <> " WHERE rowkey = ? AND txid >= ? AND txid < ?")
    [Pact4.SText (Utf8 k), txIdParam from, txIdParam to] [Pact4.RInt] >>= \case
        [[Pact4.SInt n]] -> return (fromIntegral n)
        r -> internalError $ "freezer: bad count: " <> sshow r

-- | How a table is trimmed (spec 4.3, 6.2 step 5).
data TrimRule
    = KeepAnchors
    -- ^ delete a row with @txid < F@ when a newer row of the same key with
    -- @txid < F@ exists, oldest-first
    | DropAll
    -- ^ @SYS:Pacts@: delete every row with @txid < F@, oldest-first
    deriving (Eq, Show)

-- | The trim statements of one chunk: the window @[lo, hi)@ of txids of the
-- step, with @hi <= F@. The freezer walks a step's windows in ascending
-- order, and a chunk runs its statements in one savepoint, so the trim
-- deletes oldest-first: after each chunk a key's rows left in SQLite are a
-- newest-suffix of its legacy rows, and a SQLite hit at any bound, the tip
-- included, is the legacy answer (spec 4.3, 6.3). Deleting newest-first
-- would leave an older version without its newer one between chunks.
--
-- A window's statements look only at the window's rows, so each chunk costs
-- what its window holds and no chunk re-walks an earlier window.
--
-- * 'KeepAnchors': (a) the rows of a key written in the window that are
--   older than the window (the anchor of an earlier step, and anything an
--   earlier trim left); (b) the window's rows of which a newer row of the
--   same key below @F@ exists. The newest row below @F@ of every key stays.
-- * 'DropAll' (@SYS:Pacts@): every row below @hi@. Rows below @lo@ are gone
--   already when the windows run in order, so this walks the window only.
--
trimStatements :: Text -> TrimRule -> TxId -> TxId -> TxId -> [(SQ3.Utf8, [Pact4.SType])]
trimStatements t rule lo hi f = case rule of
    KeepAnchors ->
        [ ( "DELETE FROM " <> tn <> " WHERE rowid IN (SELECT o.rowid FROM "
                <> tn <> " AS n CROSS JOIN " <> tn <> " AS o"
                <> " WHERE n.txid >= ? AND n.txid < ? AND o.rowkey = n.rowkey AND o.txid < ?)"
          , [txIdParam lo, txIdParam hi, txIdParam lo]
          )
        , ( "DELETE FROM " <> tn <> " WHERE rowid IN (SELECT o.rowid FROM " <> tn <> " AS o"
                <> " WHERE o.txid >= ? AND o.txid < ? AND EXISTS (SELECT 1 FROM " <> tn <> " AS n"
                <> " WHERE n.rowkey = o.rowkey AND n.txid > o.txid AND n.txid < ?))"
          , [txIdParam lo, txIdParam hi, txIdParam f]
          )
        ]
    DropAll ->
        [ ( "DELETE FROM " <> tn <> " WHERE txid < ?"
          , [txIdParam hi]
          )
        ]
  where
    tn = tbl (toUtf8 t)

-- | Deletes the rows of one trim chunk, the window @[lo, hi)@ below @F@ (see
-- 'trimStatements'); returns the number of rows deleted.
sqliteTrimChunk :: SQLiteEnv -> Text -> TrimRule -> TxId -> TxId -> TxId -> IO Int
sqliteTrimChunk db t rule lo hi f =
    fmap sum $ forM (trimStatements t rule lo hi f) $ \(q, ps) -> do
        throwOnDbError $ bracket (prepStmt db q) (ExceptT . SQ3.finalize) $ \stmt -> do
            bindParams stmt ps
            void $ ExceptT (SQ3.step stmt)
        SQ3.changes db

-- | @EXPLAIN QUERY PLAN@ of a trim chunk's statements: the @detail@ column,
-- in order, statement after statement.
sqliteExplainTrim :: SQLiteEnv -> Text -> TrimRule -> IO [Text]
sqliteExplainTrim db t rule = fmap concat $ forM (trimStatements t rule 1 2 3) $ \(q, ps) -> do
    rows <- freezerQry db ("EXPLAIN QUERY PLAN " <> q) ps [Pact4.RInt, Pact4.RInt, Pact4.RInt, Pact4.RText]
    forM rows $ \case
        [_, _, _, Pact4.SText (Utf8 d)] -> return (T.decodeUtf8 d)
        r -> internalError $ "freezer: bad plan row: " <> sshow r

-- | Runs a trim chunk in the savepoint @trim@: released on success, rolled
-- back on any exception.
sqliteWithTrimSavepoint :: SQLiteEnv -> IO a -> IO a
sqliteWithTrimSavepoint db act = fmap fst $ generalBracket
    (Pact4.exec_ db "SAVEPOINT [trim];")
    (\_ -> \case
        ExitCaseSuccess {} -> Pact4.exec_ db "RELEASE SAVEPOINT [trim];"
        _ -> do
            Pact4.exec_ db "ROLLBACK TRANSACTION TO SAVEPOINT [trim];"
            Pact4.exec_ db "RELEASE SAVEPOINT [trim];"
    ) $ \_ -> act
