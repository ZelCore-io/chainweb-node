{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.Test.Pact.Store.Reference
--
-- Frozen copies of the 3.2.1 functions that the SQLite 'PactStore' replaced,
-- taken verbatim from @Chainweb.Pact.PactService.Checkpointer.Internal@ and
-- @Chainweb.Pact.Backend.Utils@ at 3.2.1. They are the oracle of the
-- differential tests in "Chainweb.Test.Pact.Store.SQLite". Do not change them.
--
module Chainweb.Test.Pact.Store.Reference
    ( getEarliestBlock
    , getLatestBlock
    , lookupBlock
    , getBlockParent
    , doLookupSuccessful
    , getEndTxId
    , getEndTxId'
    , rewindDbTo
    ) where

import Control.Lens (view)
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SB
import qualified Data.HashMap.Strict as HashMap
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import Database.SQLite3.Direct (Database, Utf8(..))
import GHC.Stack (HasCallStack)

import Pact.Types.SQLite
import qualified Pact.Types.Persistence as Pact4
import qualified Pact.Types.SQLite as Pact4

import Chainweb.BlockHash
import Chainweb.BlockHeader
import Chainweb.BlockHeight
import Chainweb.Pact.Backend.Types (Historical(..), SQLiteEnv)
import Chainweb.Pact.Backend.Utils (expectSingle, tbl)
import Chainweb.Pact.Types (PactException(..), internalError)
import Chainweb.Utils (T2(..), T3(..), sshow)
import Chainweb.Utils.Serialization (runGetEitherS, runPutS)
import Chainweb.Version

-- -------------------------------------------------------------------------- --
-- Chainweb.Pact.PactService.Checkpointer.Internal (3.2.1)

-- | Get the checkpointer's idea of the earliest block. The block height
-- is the height of the block of the block hash.
getEarliestBlock :: HasCallStack => SQLiteEnv -> IO (Maybe (BlockHeight, BlockHash))
getEarliestBlock db = do
  r <- qry_ db qtext [RInt, RBlob] >>= mapM go
  case r of
    [] -> return Nothing
    (!o:_) -> return (Just o)
  where
    qtext = "SELECT blockheight, hash FROM BlockHistory ORDER BY blockheight ASC LIMIT 1"

    go [SInt hgt, SBlob blob] =
        let hash = either error id $ runGetEitherS decodeBlockHash blob
        in return (fromIntegral hgt, hash)
    go _ = fail "Chainweb.Pact.Backend.RelationalCheckpointer.doGetEarliest: impossible. This is a bug in chainweb-node."

-- | Get the checkpointer's idea of the latest block. The block height is
-- is the height of the block of the block hash.
--
-- TODO: Under which circumstances does this return 'Nothing'?
getLatestBlock :: HasCallStack => SQLiteEnv -> IO (Maybe (BlockHeight, BlockHash))
getLatestBlock db = do
  r <- qry_ db qtext [RInt, RBlob] >>= mapM go
  case r of
    [] -> return Nothing
    (!o:_) -> return (Just o)
  where
    qtext = "SELECT blockheight, hash FROM BlockHistory ORDER BY blockheight DESC LIMIT 1"

    go [SInt hgt, SBlob blob] =
        let hash = either error id $ runGetEitherS decodeBlockHash blob
        in return (fromIntegral hgt, hash)
    go _ = fail "Chainweb.Pact.Backend.RelationalCheckpointer.getLatest: impossible. This is a bug in chainweb-node."

-- | Ask: is the checkpointer aware of the given block?
lookupBlock :: SQLiteEnv -> (BlockHeight, BlockHash) -> IO Bool
lookupBlock db (bheight, bhash) = do
    r <- qry db qtext [SInt $ fromIntegral bheight, SBlob (runPutS (encodeBlockHash bhash))]
                      [RInt]
    liftIO (expectSingle "row" r) >>= \case
        [SInt n] -> return $! n == 1
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
            r <- qry db qtext [SInt (fromIntegral (pred bh))] [RBlob]
            case r of
              [[SBlob blob]] ->
                either (internalError . T.pack) (return . return) $! runGetEitherS decodeBlockHash blob
              [] -> internalError "getBlockParent: block was found but its parent couldn't be found"
              _ -> error "getBlockParent: output type mismatch"
  where
    qtext = "SELECT hash FROM BlockHistory WHERE blockheight = ?"

-- -------------------------------------------------------------------------- --
-- Chainweb.Pact.Backend.Utils (3.2.1)

doLookupSuccessful :: Database -> BlockHeight -> V.Vector SB.ShortByteString -> IO (HashMap.HashMap SB.ShortByteString (T2 BlockHeight BlockHash))
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
    -- NOTE: it's useful to keep the types of 'go' and 'buildResultMap' in sync
    -- for readability but also to ensure the compiler and reader infer the
    -- right result types from the db query.

    buildResultMap :: [T3 SB.ShortByteString BlockHeight BlockHash] -> HashMap.HashMap SB.ShortByteString (T2 BlockHeight BlockHash)
    buildResultMap xs = HashMap.fromList $
      map (\(T3 txhash blockheight blockhash) -> (txhash, T2 blockheight blockhash)) xs

    go :: [Pact4.SType] -> IO (T3 SB.ShortByteString BlockHeight BlockHash)
    go (Pact4.SInt blockheight:Pact4.SBlob blockhash:Pact4.SBlob txhash:_) = do
        !blockhash' <- either fail return $ runGetEitherS decodeBlockHash blockhash
        let !txhash' = SB.toShort txhash
        return $! T3 txhash' (fromIntegral blockheight) blockhash'
    go _ = fail "impossible"

getEndTxId :: Text -> SQLiteEnv -> Maybe ParentHeader -> IO (Historical Pact4.TxId)
getEndTxId msg sql pc = case pc of
    Nothing -> return (Historical 0)
    Just (ParentHeader ph) -> getEndTxId' msg sql (view blockHeight ph) (view blockHash ph)

getEndTxId' :: Text -> SQLiteEnv -> BlockHeight -> BlockHash -> IO (Historical Pact4.TxId)
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
    -> IO Pact4.TxId
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
  :: Database
  -> BlockHeight
  -> Pact4.TxId
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
