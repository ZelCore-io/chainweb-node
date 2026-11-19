{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE BlockArguments #-}

-- TODO pact5: fix the orphan PactDbFor instance
{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

-- | The database operations that manipulate and read the Pact state.

-- Note: [Pact Transactions]

-- What is a "pact transaction"? There are three levels of transactions going on:
-- +-------------------------------------------------------------------------------------------------------------------------+
-- | Block                                                                                                                   |
-- |                                                                                                                         |
-- | +-----------------+ +---------------------------------------------+   +-----------------------------------------------+ |
-- | | Apply Coinbase  | | Apply Command                               |   | Apply Command                                 | |
-- | | |               | |+------------+ +------------+ +------------+ |   |+------------+ +------------+ +------------+   | |
-- | | |               | || Buy        | | Run        | | Redeem     | |   || Buy        | | Run        | | Redeem     |   | |
-- | | v               | || Gas        | | Payload    | | Gas        | |   || Gas        | | Payload    | | Gas        |   | |
-- | | Pact tx         | || |          | | |          | | |          | |   || |          | | |          | | |          |   | |
-- | | (begin-tx)      | || v          | | v          | | v          | +-->|| v          | | v          | | v          |   | |
-- | +v----------------+ || Pact tx    | | Pact tx    | | Pact tx    | |   || Pact tx    | | Pact tx    | | Pact tx    |   | |
-- |  Pact5Db tx         || (begin-tx) | | (begin-tx) | | (begin-tx) | |   || (begin-tx) | | (begin-tx) | | (begin-tx) |   | |
-- |                     ||            | |            | |            | |   ||            | |            | |            |   | |
-- |                     |+------------+ +------------+ +------------+ |   |+------------+ +------------+ +------------+   | |
-- |                     |                                             |   |                                               | |
-- |                     +v--------------------------------------------+   +v----------------------------------------------+ |
-- |                      Pact5Db tx                                        Pact5Db tx                                       |
-- +v------------------------------------------------------------------------------------------------------------------------+
--  SQLite tx (withSavepoint)
--    (in some cases multiple blocks in tx)
--
--
-- Transactions must be nested in this way.
--
-- SQLite transaction ensures that the Pact5Db transaction
-- sees a consistent view of the database, especially if its
-- writes are committed later.
--
-- Pact5Db tx ensures that the Pact tx's writes
-- are recorded.
--
-- Pact tx ensures that failed transactions' writes are not recorded.


module Chainweb.Pact5.Backend.ChainwebPactDb
    ( chainwebPactBlockDb
    , Pact5Db(..)
    , BlockHandlerEnv(..)
    , blockHandlerStore
    , blockHandlerLogger
    , toTxLog
    , toPactTxLog
    , domainTableName
    , convRowKey
    , commitBlockStateToDatabase
    ) where

import Chainweb.BlockHash
import Chainweb.BlockHeight
import Chainweb.Logger
import Chainweb.Pact.Backend.InMemDb qualified as InMemDb
import Chainweb.Pact.Backend.Store.Types
    (BlockCommit(..), CommitError(..), KeysQuery(..), PactStore(..))
import Chainweb.Pact.Backend.Types
import Chainweb.Pact.Backend.Utils
import Chainweb.Utils (sshow, unsafeHead, T2)
import Chainweb.Version
import Chainweb.Version.Guards
import Control.Applicative
import Control.Concurrent.MVar
import Control.Exception.Safe
import Control.Lens
import Control.Monad
import Control.Monad.Except
import Control.Monad.Morph
import Control.Monad.Reader
import Control.Monad.State.Strict
import Control.Monad.Trans.Maybe
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as B8
import Data.ByteString.Short qualified as SB
import Data.DList (DList)
import Data.DList qualified as DL
import Data.Foldable
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HM
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet qualified as HashSet
import Data.Int
import Data.List(group, sort)
import Data.Map.Strict qualified as M
import Data.Maybe
import Data.Singletons (Dict(..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Vector (Vector)
import Database.SQLite3.Direct qualified as SQ3
import GHC.Stack
import Pact.Core.Builtin qualified as Pact
import Pact.Core.Command.Types (RequestKey (..))
import Pact.Core.Errors qualified as Pact
import Pact.Core.Evaluate qualified as Pact
import Pact.Core.Gas qualified as Pact
import Pact.Core.Guards qualified as Pact
import Pact.Core.Hash
import Pact.Core.Info qualified as Pact
import Pact.Core.Names qualified as Pact
import Pact.Core.Persistence (throwDbOpErrorGasM)
import Pact.Core.Persistence qualified as Pact
import Pact.Core.Serialise qualified as Pact
import Pact.Core.StableEncoding (encodeStable)
import Pact.Types.Persistence qualified as Pact4
import Prelude hiding (concat, log)

data InternalDbException = InternalDbException CallStack Text
instance Show InternalDbException where show = displayException
instance Exception InternalDbException where
    displayException (InternalDbException stack text) =
        T.unpack text <> "\n\n" <>
        prettyCallStack stack

internalDbError :: HasCallStack => MonadThrow m => Text -> m a
internalDbError = throwM . InternalDbException callStack

throwOnDbError :: (HasCallStack, MonadThrow m) => ExceptT SQ3.Error m a -> m a
throwOnDbError act = runExceptT act >>= either (internalDbError . sshow) return

data BlockHandlerEnv logger = BlockHandlerEnv
    { _blockHandlerStore :: !PactStore
    , _blockHandlerLogger :: !logger
    , _blockHandlerVersion :: !ChainwebVersion
    , _blockHandlerBlockHeight :: !BlockHeight
    , _blockHandlerUpperBoundTxId :: !Pact.TxId
    , _blockHandlerChainId :: !ChainId
    , _blockHandlerMode :: !Pact.ExecutionMode
    , _blockHandlerAtTip :: Bool
    }

-- | The state used by database operations.
-- Includes both the state re: the whole block, and the state re: a transaction in progress.
data BlockState = BlockState
    { _bsBlockHandle :: !(BlockHandle Pact5)
    , _bsPendingTxWrites :: !(SQLitePendingData InMemDb.Store)
    , _bsPendingTxLog :: !(Maybe (DList (Pact.TxLog ByteString)))
    }

makeLenses ''BlockState
makeLenses ''BlockHandlerEnv

getPendingTxLogOrError :: Text -> BlockHandler logger (DList (Pact.TxLog ByteString))
getPendingTxLogOrError msg = do
    use bsPendingTxLog >>= \case
        Nothing -> liftGas $ Pact.throwDbOpErrorGasM (Pact.NotInTx msg)
        Just t -> return t

-- | The Pact 5 database as it's provided by the checkpointer.
data Pact5Db = Pact5Db
    { doPact5DbTransaction
        :: forall a
        . BlockHandle Pact5
        -> Maybe RequestKey
        -> (Pact.PactDb Pact.CoreBuiltin Pact.Info -> IO a)
        -> IO (a, BlockHandle Pact5)
        -- ^ Give this function a BlockHandle representing the state of a pending
        -- block and it will pass you a PactDb which contains the Pact state as of
        -- that point in the block. After you're done, it passes you back a
        -- BlockHandle representing the state of the block extended with any writes
        -- you made to the PactDb.
        -- Note also that this function handles registering
        -- transactions as completed, if you pass it a RequestKey.
    , lookupPactTransactions :: Vector RequestKey -> IO (HashMap RequestKey (T2 BlockHeight BlockHash))
        -- ^ Used to implement transaction polling.
    }

type instance PactDbFor logger Pact5 = Pact5Db

-- this monad allows access to the database environment "in" a particular
-- transaction, and allows charging gas for database operations.
newtype BlockHandler logger a = BlockHandler
    { runBlockHandler
        :: ReaderT (BlockHandlerEnv logger)
            (StateT BlockState (Pact.GasM Pact.CoreBuiltin Pact.Info)) a
    } deriving newtype
        ( Functor
        , Applicative
        , Monad
        , MonadState BlockState
        , MonadThrow
        , MonadIO
        , MonadReader (BlockHandlerEnv logger)
        )

domainTableName :: Pact.Domain k v b i -> SQ3.Utf8
domainTableName = toUtf8 . Pact.renderDomain

convKeySetName :: Pact.KeySetName -> Text
convKeySetName = Pact.renderKeySetName

convModuleName :: Pact.ModuleName -> Text
convModuleName = Pact.renderModuleName

convNamespaceName :: Pact.NamespaceName -> Text
convNamespaceName (Pact.NamespaceName name) = name

convRowKey :: Pact.RowKey -> Text
convRowKey (Pact.RowKey name) = name

-- to match legacy keys
convPactId :: Pact.DefPactId -> Text
convPactId pid = "PactId \"" <> Pact.renderDefPactId pid <> "\""

convHashedModuleName :: Pact.HashedModuleName -> Text
convHashedModuleName = Pact.renderHashedModuleName

liftGas :: Pact.GasM Pact.CoreBuiltin Pact.Info a -> BlockHandler logger a
liftGas g = BlockHandler (lift (lift g))

runOnBlockGassed
    :: BlockHandlerEnv logger -> MVar BlockState
    -> BlockHandler logger a
    -> Pact.GasM Pact.CoreBuiltin Pact.Info a
runOnBlockGassed env stateVar act = do
    ge <- ask
    r <- liftIO $ modifyMVar stateVar $ \s -> do
        r <- runExceptT (runReaderT (Pact.runGasM (runStateT (runReaderT (runBlockHandler act) env) s)) ge)
        let newState = either (\_ -> s) snd r
        return (newState, fmap fst r)
    liftEither r

chainwebPactBlockDb :: (Logger logger) => BlockHandlerEnv logger -> Pact5Db
chainwebPactBlockDb env = Pact5Db
    { doPact5DbTransaction = \blockHandle maybeRequestKey kont -> do
        stateVar <- newMVar $ BlockState blockHandle (_blockHandlePending blockHandle) Nothing
        let pactDb = Pact.PactDb
                { Pact._pdbPurity = Pact.PImpure
                , Pact._pdbRead = \d k -> runOnBlockGassed env stateVar $ doReadRow d k
                , Pact._pdbWrite = \wt d k v ->
                    runOnBlockGassed env stateVar $ doWriteRow wt d k v
                , Pact._pdbKeys = \d ->
                    runOnBlockGassed env stateVar $ doKeys d
                , Pact._pdbCreateUserTable = \tn ->
                    runOnBlockGassed env stateVar $ doCreateUserTable tn
                , Pact._pdbBeginTx = \m ->
                    runOnBlockGassed env stateVar $ doBegin m
                , Pact._pdbCommitTx =
                    runOnBlockGassed env stateVar doCommit
                , Pact._pdbRollbackTx =
                    runOnBlockGassed env stateVar doRollback
                }
        r <- kont pactDb
        finalState <- readMVar stateVar
        -- Register a successful transaction in the pending data for the block
        let registerRequestKey = case maybeRequestKey of
                Just requestKey -> HashSet.insert (SB.fromShort $ unHash $ unRequestKey requestKey)
                Nothing -> id
        let finalHandle =
                _bsBlockHandle finalState
                    & blockHandlePending . pendingSuccessfulTxs %~ registerRequestKey

        return (r, finalHandle)
    , lookupPactTransactions =
        fmap (HM.mapKeys (RequestKey . Hash)) .
        _psDoLookupSuccessful (_blockHandlerStore env) (_blockHandlerBlockHeight env) .
        fmap (unHash . unRequestKey)
    }

doReadRow
    :: forall k v logger
    -- ^ the highest block we should be reading writes from
    . Pact.Domain k v Pact.CoreBuiltin Pact.Info
    -> k
    -> BlockHandler logger (Maybe v)
doReadRow d k = do
    runMaybeT (MaybeT lookupInMem <|> MaybeT lookupInDb) >>= \case
        Nothing -> return Nothing
        Just (encodedValueLength, decodedValue) -> do
            case d of
                Pact.DModules -> do
                    BlockHandler $ lift $ lift
                        $ Pact.chargeGasM (Pact.GModuleOp (Pact.MOpLoadModule encodedValueLength))
                _ -> return ()
            bsPendingTxWrites . pendingWrites %=
                InMemDb.insert d k (InMemDb.ReadEntry encodedValueLength decodedValue)
            return (Just decodedValue)
    where
    codec :: BlockHandler logger (ByteString -> Maybe (Pact.Document v), Text)
    codec = do
        serialiser <- getSerialiser
        return $ case d of
            Pact.DKeySets ->
                (Pact._decodeKeySet serialiser, convKeySetName k)
            Pact.DModules ->
                (Pact._decodeModuleData serialiser, convModuleName k)
            Pact.DNamespaces ->
                (Pact._decodeNamespace serialiser, convNamespaceName k)
            Pact.DUserTables _ ->
                (Pact._decodeRowData serialiser, convRowKey k)
            Pact.DDefPacts ->
                (Pact._decodeDefPactExec serialiser, convPactId k)
            Pact.DModuleSource ->
                (Pact._decodeModuleCode serialiser, convHashedModuleName k)

    lookupInMem :: BlockHandler logger (Maybe (Int, v))
    lookupInMem = do
            store <- use (bsPendingTxWrites . pendingWrites)
            return $ InMemDb.lookup d k store <&> \case
                (InMemDb.ReadEntry len a) -> (len, a)
                (InMemDb.WriteEntry _ bs a) -> (BS.length bs, a)

    lookupInDb :: BlockHandler logger (Maybe (Int, v))
    lookupInDb = do
        case d of
            Pact.DUserTables pactTableName -> do
                -- if the table is pending creation, we also return Nothing
                fmap join $ withTableExistenceCheck pactTableName fetchRowFromDb
            _ -> throwOnDbError $ fetchRowFromDb
        where
        fetchRowFromDb :: ExceptT SQ3.Error (BlockHandler logger) (Maybe (Int, v))
        fetchRowFromDb = do
            (decodeValueDoc, encodedKey) <- lift codec
            let decodeValue = fmap (view Pact.document) . decodeValueDoc
            Pact.TxId txIdUpperBoundWord64 <- view blockHandlerUpperBoundTxId
            store <- view blockHandlerStore
            result <- ExceptT $ liftIO $
                _psReadNewest store (Pact.renderDomain d) (T.encodeUtf8 encodedKey)
                    (Just (Pact4.TxId txIdUpperBoundWord64))
            case result of
                Nothing -> return Nothing
                Just a -> return $ (BS.length a,) <$> decodeValue a

data TableStatus
    = TableCreationPending
    | TableExists
    | TableDoesNotExist

checkTableStatus :: Pact.TableName -> BlockHandler logger TableStatus
checkTableStatus tableName = do
    pds <- use bsPendingTxWrites
    if
        Pact.renderTableName tableName
        `HashSet.member`
        _pendingTableCreation pds
    then return TableCreationPending

    else if
        InMemDb.checkTableSeen tableName (_pendingWrites pds)
    then
        return TableExists

    else do
        exists <- checkTableExistsInDb
        when exists $
            bsPendingTxWrites . pendingWrites
                %= InMemDb.markTableSeen tableName
        return $
            if exists then TableExists else TableDoesNotExist

    where
    checkTableExistsInDb :: BlockHandler logger Bool
    checkTableExistsInDb = do
        bh <- view blockHandlerBlockHeight
        store <- view blockHandlerStore
        -- table names are case-insensitive
        liftIO $ throwOnDbError $ ExceptT $
            _psTableExistsAt store (Pact.renderTableName tableName) bh

-- we ideally produce `NoSuchTable` errors for accesses to user tables that
-- don't exist, so when doing such accesses, wrap them with
-- `withTableExistenceCheck`.  we cache knowledge of tables' existence in
-- `checkTableStatus`, too.  returns `Nothing` if the table is pending creation
-- in this block; usually, this means that we halt before accessing the db to
-- look in the table.
withTableExistenceCheck :: HasCallStack => Pact.TableName -> ExceptT SQ3.Error (BlockHandler logger) a -> BlockHandler logger (Maybe a)
withTableExistenceCheck tableName action = do
    atTip <- view blockHandlerAtTip
    if atTip
    -- at tip, speculatively execute the statement, and only check if the table
    -- was missing if the statement threw an error
    then runExceptT action >>= \case
        Left err@SQ3.ErrorError -> do
            tableStatus <- checkTableStatus tableName
            case tableStatus of
                TableDoesNotExist -> liftGas $ throwDbOpErrorGasM $ Pact.NoSuchTable tableName
                TableCreationPending -> return Nothing
                TableExists -> internalDbError (sshow err)
        Left err -> internalDbError (sshow err)
        Right result -> return (Just result)
    else do
        -- if we're rewound, we just check if the table exists first
        tableStatus <- checkTableStatus tableName
        case tableStatus of
            TableDoesNotExist -> liftGas $ throwDbOpErrorGasM $ Pact.NoSuchTable tableName
            TableCreationPending -> return Nothing
            TableExists -> throwOnDbError (Just <$> action)

latestTxId :: Lens' BlockState Pact.TxId
latestTxId = bsBlockHandle . blockHandleTxId . coerced

writeSys
    :: Pact.Domain k v Pact.CoreBuiltin Pact.Info
    -> k
    -> v
    -> BlockHandler logger ()
writeSys d k v = do
    txid <- use latestTxId
    serialiser <- getSerialiser
    let !(!encodedKey, !encodedValue) = case d of
            Pact.DKeySets -> (convKeySetName k, Pact._encodeKeySet serialiser v)
            Pact.DModules ->  (convModuleName k, Pact._encodeModuleData serialiser v)
            Pact.DNamespaces -> (convNamespaceName k, Pact._encodeNamespace serialiser v)
            Pact.DDefPacts -> (convPactId k, Pact._encodeDefPactExec serialiser v)
            Pact.DUserTables _ -> error "impossible"
            Pact.DModuleSource -> (convHashedModuleName k, Pact._encodeModuleCode serialiser v)
    recordPendingUpdate d k txid (encodedValue, v)
    recordTxLog d encodedKey encodedValue

recordPendingUpdate
    :: Pact.Domain k v Pact.CoreBuiltin Pact.Info
    -> k
    -> Pact.TxId
    -> (ByteString, v)
    -> BlockHandler logger ()
recordPendingUpdate d k txid (encodedValue, decodedValue) =
    bsPendingTxWrites . pendingWrites %=
        InMemDb.insert d k (InMemDb.WriteEntry txid encodedValue decodedValue)

writeUser
    :: Pact.WriteType
    -> Pact.TableName
    -> Pact.RowKey
    -> Pact.RowData
    -> BlockHandler logger ()
writeUser wt tableName k (Pact.RowData newRow) = do
    Pact.TxId txid <- use latestTxId
    maybeExistingValue <- doReadRow (Pact.DUserTables tableName) k
    checkInsertIsOK maybeExistingValue
    finalRow <- case maybeExistingValue of
        Nothing -> return $ Pact.RowData newRow
        Just (Pact.RowData oldRow) -> return $ Pact.RowData $ M.union newRow oldRow
    serialiser <- getSerialiser
    encodedFinalRow <- liftGas (Pact._encodeRowData serialiser finalRow)
    recordTxLog (Pact.DUserTables tableName) (convRowKey k) encodedFinalRow
    recordPendingUpdate (Pact.DUserTables tableName) k (Pact.TxId txid) (encodedFinalRow, finalRow)
    where

    -- only for user tables, we check first if the insertion is legal before doing it.
    checkInsertIsOK :: Maybe Pact.RowData -> BlockHandler logger ()
    checkInsertIsOK olds = do
        case (olds, wt) of
            (Nothing, Pact.Insert) -> return ()
            (Just _, Pact.Insert) -> liftGas $ Pact.throwDbOpErrorGasM (Pact.RowFoundError tableName k)
            (Nothing, Pact.Write) -> return ()
            (Just _, Pact.Write) -> return ()
            (Just _, Pact.Update) -> return ()
            (Nothing, Pact.Update) -> liftGas $ Pact.throwDbOpErrorGasM (Pact.NoRowFound tableName k)

doWriteRow
    -- ^ the highest block we should be reading writes from
    :: Pact.WriteType
    -> Pact.Domain k v Pact.CoreBuiltin Pact.Info
    -> k
    -> v
    -> BlockHandler logger ()
doWriteRow wt d k v = case d of
    Pact.DUserTables tableName -> writeUser wt tableName k v
    _ -> writeSys d k v

doKeys
    :: forall k v logger
    -- ^ the highest block we should be reading writes from
    . Pact.Domain k v Pact.CoreBuiltin Pact.Info
    -> BlockHandler logger [k]
doKeys d = do
    dbKeys <- getDbKeys
    mptx <- use bsPendingTxWrites

    let memKeys = collect mptx

    (parsedKeys, ordDict :: Dict (Ord k) ()) <- case d of
        Pact.DKeySets -> do
            let parsed = traverse Pact.parseAnyKeysetName dbKeys
            case parsed of
              Left msg -> internalDbError $ "doKeys.DKeySets: unexpected decoding " <> T.pack msg
              Right v -> pure (v, Dict ())
        Pact.DModules -> do
            let parsed = traverse Pact.parseModuleName dbKeys
            case parsed of
              Nothing -> internalDbError $ "doKeys.DModules: unexpected decoding"
              Just v -> pure (v, Dict ())
        Pact.DNamespaces -> pure (map Pact.NamespaceName dbKeys, Dict ())
        Pact.DDefPacts ->  pure (map Pact.DefPactId dbKeys, Dict ())
        Pact.DUserTables _ -> pure (map Pact.RowKey dbKeys, Dict ())
        Pact.DModuleSource -> do
            let parsed = map Pact.parseHashedModuleName dbKeys
            case sequence parsed of
              Just v -> pure (v, Dict ())
              Nothing -> internalDbError $ "doKeys.DModuleSources: unexpected decoding"
    case ordDict of
        Dict () -> do
            v <- view blockHandlerVersion
            cid <- view blockHandlerChainId
            bh <- view blockHandlerBlockHeight
            if chainweb230Pact v cid bh
            -- the read-cache contains duplicate keys that we need to remove.
            then return $ fmap (unsafeHead "doKeys") $ group $ sort (memKeys ++ parsedKeys)
            else return $ sort (memKeys ++ parsedKeys)

    where

    getDbKeys = do
            case d of
                Pact.DUserTables pactTableName -> do
                    fromMaybe [] <$> withTableExistenceCheck pactTableName fetchKeys
                _ -> throwOnDbError fetchKeys
        where
        fetchKeys :: ExceptT SQ3.Error (BlockHandler logger) [Text]
        fetchKeys = do
            Pact.TxId txIdUpperBoundWord64 <- view blockHandlerUpperBoundTxId
            store <- view blockHandlerStore
            ks <- ExceptT $ liftIO $
                _psDistinctKeys store (KeysOrderedByKey (Pact4.TxId txIdUpperBoundWord64)) (Pact.renderDomain d)
            return $ fromUtf8 . SQ3.Utf8 <$> ks

    collect p = InMemDb.keys d (_pendingWrites p)

recordTxLog
    :: Pact.Domain k v Pact.CoreBuiltin Pact.Info
    -> Text
    -> ByteString
    -> BlockHandler logger ()
recordTxLog d k v = recordTxLog' (Pact.renderDomain d) k v

recordTxLog' :: Text -> Text -> ByteString -> BlockHandler logger ()
recordTxLog' d k v = do
    bsPendingTxLog . _Just %= flip DL.snoc newLog
  where
    !newLog = Pact.TxLog d k v

recordTableCreationTxLog :: Pact.TableName -> BlockHandler logger ()
recordTableCreationTxLog tn = do
    recordTxLog' "SYS:usertables" (Pact._tableName tn) (encodeStable uti)
    where
    !uti = Pact.UserTableInfo (Pact._tableModuleName tn)

doCreateUserTable
    -- ^ the highest block we should be seeing tables from
    :: Pact.TableName
    -> BlockHandler logger ()
doCreateUserTable tableName = do
    -- first check if tablename already exists in pending queues
    checkTableStatus tableName >>= \case
        TableCreationPending ->
            liftGas $ Pact.throwDbOpErrorGasM $ Pact.TableAlreadyExists tableName
        TableExists ->
            liftGas $ Pact.throwDbOpErrorGasM $ Pact.TableAlreadyExists tableName
        TableDoesNotExist -> do
            bsPendingTxWrites . pendingTableCreation %=
                HashSet.insert (Pact.renderTableName tableName)
            recordTableCreationTxLog tableName

doRollback :: BlockHandler logger ()
doRollback = do
    blockWrites <- use (bsBlockHandle . blockHandlePending)
    bsPendingTxWrites .= blockWrites
    bsPendingTxLog .= Nothing

-- | Commit a Pact transaction
doCommit :: BlockHandler logger [Pact.TxLog B8.ByteString]
doCommit = view blockHandlerMode >>= \case
    m -> do
        txrs <- if m == Pact.Transactional
        then do
            modify' $ over latestTxId (\(Pact.TxId tid) -> Pact.TxId (succ tid))
            pendingTx <- use bsPendingTxWrites
            txLogs <- getPendingTxLogOrError "doCommit"

            -- merge pending tx into pending block data
            bsBlockHandle . blockHandlePending .= pendingTx
            bsPendingTxLog .= Nothing
            return txLogs
        else doRollback >> return mempty
        return $! DL.toList txrs

-- | Begin a Pact transaction. Note that we don't actually use the ExecutionMode anymore.
doBegin :: (Logger logger) => Pact.ExecutionMode -> BlockHandler logger (Maybe Pact.TxId)
doBegin _m = do
    use bsPendingTxLog >>= \case
        Just _ -> do
            txid <- use latestTxId
            liftGas $ Pact.throwDbOpErrorGasM (Pact.TxAlreadyBegun ("TxId " <> sshow (Pact._txId txid)))
        Nothing -> do
            bsPendingTxLog .= Just mempty
            Just <$> use latestTxId

toTxLog :: MonadThrow m => ChainwebVersion -> ChainId -> BlockHeight -> T.Text -> SQ3.Utf8 -> BS.ByteString -> m (Pact.TxLog Pact.RowData)
toTxLog version cid bh d key value = do
    let serialiser = pact5Serialiser version cid bh
    case fmap (view Pact.document) $ Pact._decodeRowData serialiser value of
        Nothing -> internalDbError $ "toTxLog: Unexpected value, unable to deserialize log: " <> sshow value
        Just v -> return $! Pact.TxLog d (fromUtf8 key) v

toPactTxLog :: Pact.TxLog Pact.RowData -> Pact4.TxLog Pact.RowData
toPactTxLog (Pact.TxLog d k v) = Pact4.TxLog d k v

-- | Write the block's pending state through the store, in 3.2.1's order: new
-- tables, then the system tables and the user tables that have writes, each
-- followed by its mutation row, then the transaction index, then the block
-- history row. A failure is rethrown through 'throwOnDbError', as in 3.2.1.
commitBlockStateToDatabase :: PactStore -> BlockHash -> BlockHeight -> BlockHandle Pact5 -> IO ()
commitBlockStateToDatabase store hsh bh blockHandle = throwOnDbError $ withExceptT _ceError $ ExceptT $
    _psCommitBlock store BlockCommit
        { _bcHeight = bh
        , _bcHash = hsh
        , _bcEndTxId = _blockHandleTxId blockHandle
        , _bcNewTables = toList (_pendingTableCreation pending)
        , _bcRows = tableWrites (_pendingWrites pending)
        , _bcTxHashes = toList (_pendingSuccessfulTxs pending)
        }
    where
    pending = _blockHandlePending blockHandle

    -- 3.2.1's backendWriteUpdateBatch: the system tables in this order, then
    -- the user tables; read entries are not written, and a table without
    -- writes gets neither an insert nor a mutation row.
    tableWrites :: InMemDb.Store -> [(Text, [(ByteString, Pact4.TxId, ByteString)])]
    tableWrites inMem = filter (not . null . snd) $
        [ (Pact.renderDomain Pact.DKeySets
          , mapMaybe (uncurry $ prepRow . convKeySetName) $ HashMap.toList (InMemDb.keySets inMem))
        , (Pact.renderDomain Pact.DModules
          , mapMaybe (uncurry $ prepRow . convModuleName) $ HashMap.toList (InMemDb.modules inMem))
        , (Pact.renderDomain Pact.DNamespaces
          , mapMaybe (uncurry $ prepRow . convNamespaceName) $ HashMap.toList (InMemDb.namespaces inMem))
        , (Pact.renderDomain Pact.DDefPacts
          , mapMaybe (uncurry $ prepRow . convPactId) $ HashMap.toList (InMemDb.defPacts inMem))
        , (Pact.renderDomain Pact.DModuleSource
          , mapMaybe (uncurry $ prepRow . convHashedModuleName) $ HashMap.toList (InMemDb.moduleSources inMem))
        ] <>
        [ (Pact.renderDomain (Pact.DUserTables tableName)
          , mapMaybe (uncurry $ prepRow . convRowKey) $ HashMap.toList tableContents)
        | (tableName, tableContents) <- itoList (InMemDb.userTables inMem)
        ]

    prepRow rowkey (InMemDb.WriteEntry (Pact.TxId txid) rowdataEncoded _) =
        Just (T.encodeUtf8 rowkey, Pact4.TxId txid, rowdataEncoded)
    prepRow _ InMemDb.ReadEntry {} = Nothing

getSerialiser :: BlockHandler logger (Pact.PactSerialise Pact.CoreBuiltin Pact.LineInfo)
getSerialiser = do
    version <- view blockHandlerVersion
    cid <- view blockHandlerChainId
    blockHeight <- view blockHandlerBlockHeight
    return $ pact5Serialiser version cid blockHeight
