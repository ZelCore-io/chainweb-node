{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.Cold
--
-- The cold store of the Pact history split (storage redesign, spec 4.3): one
-- RocksDB instance at @<db-dir>/0/pactCold@ for all chains, with the key
-- layouts of "Chainweb.Pact.Backend.Store.Cold.Key".
--
-- This module holds the store handle, the per-chain meta (written and
-- verified frontier), the table catalog, the key ids and the one-batch step
-- write. Each chain is reached through a 'ColdChain' record of operations, so
-- split routing and the freezer see nothing else.
--
-- A step is written as one atomic, synced batch: an @R@ entry per row, for
-- user tables a @T@ entry per txid and @K@ and @I@ entries for keys new to the
-- table, an @N@ entry for each table new to the chain, and @M@ with the new
-- written frontier. No @X@ (transaction index) entry is ever written.
--
module Chainweb.Pact.Backend.Store.Cold
( -- * Frontiers and meta
  Frontier(..)
, ColdMeta(..)
, emptyColdMeta
, encodeColdMeta
, decodeColdMeta
, frontierTxId

  -- * Table names
, foldTableName
, isSysTable
, frozenTable

  -- * Store
, ColdStore
, ColdOpenMode(..)
, ColdStoreException(..)
, withColdStore
, coldCacheStats
, coldStoreRocksDb
, coldRawGet
, coldRawKeys

  -- * Chains
, StepRow(..)
, ColdStep(..)
, ColdChain(..)
, openColdChain
, openColdChainWith
) where

import Control.Concurrent.MVar
import Control.Exception
import Control.Monad

import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import Data.IORef
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word

import qualified Database.RocksDB.Base as R
import qualified Database.RocksDB.Iterator as I
import qualified Database.RocksDB.ReadOptions as RO

import qualified Pact.Types.Persistence as Pact4

-- internal modules

import Chainweb.BlockHash
import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Pact.Backend.Store.Cold.Key
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning
import Chainweb.Utils.Serialization

-- -------------------------------------------------------------------------- --
-- Frontiers and meta

-- | The last frozen block of a chain: rows with @txid < _frEndTxId@ are
-- frozen.
--
data Frontier = Frontier
    { _frHeight :: !BlockHeight
    , _frHash :: !BlockHash
    , _frEndTxId :: !Pact4.TxId
    }
    deriving (Eq, Show)

-- | The @M c@ value of a chain.
--
-- '_cmTrimFrom' is the end txid of the verified frontier before the last
-- publish, so a restart re-trims only the last step.
--
data ColdMeta = ColdMeta
    { _cmWritten :: !(Maybe Frontier)
    , _cmVerified :: !(Maybe Frontier)
    , _cmTrimFrom :: !Pact4.TxId
    }
    deriving (Eq, Show)

-- | The meta of a chain that has no @M@ entry.
--
emptyColdMeta :: ColdMeta
emptyColdMeta = ColdMeta Nothing Nothing 0

txIdWord :: Pact4.TxId -> Word64
txIdWord (Pact4.TxId w) = w

-- | Version byte @1@; the written and the verified frontier, each as a tag
-- byte (@0@ none, @1@ present), then height (8 bytes, big-endian), the block
-- hash ('encodeBlockHash') and the end txid (8 bytes, big-endian); last the
-- trim-from txid (8 bytes, big-endian).
--
encodeColdMeta :: ColdMeta -> B.ByteString
encodeColdMeta (ColdMeta w v t) = runPutS $ do
    putWord8 1
    putFrontier w
    putFrontier v
    putWord64be (txIdWord t)
  where
    putFrontier Nothing = putWord8 0
    putFrontier (Just (Frontier h hash e)) = do
        putWord8 1
        putWord64be (getBlockHeight h)
        encodeBlockHash hash
        putWord64be (txIdWord e)

-- | The inverse of 'encodeColdMeta'. Refuses another version byte, an unknown
-- tag, a truncated value and trailing bytes.
--
decodeColdMeta :: B.ByteString -> Either String ColdMeta
decodeColdMeta = runGetEitherS $ do
    version <- getWord8
    unless (version == 1) $
        fail $ "Chainweb.Pact.Backend.Store.Cold.decodeColdMeta: unknown version " <> show version
    ColdMeta <$> getFrontier <*> getFrontier <*> (Pact4.TxId <$> getWord64be)
  where
    getFrontier = getWord8 >>= \case
        0 -> return Nothing
        1 -> fmap Just $ Frontier
            <$> (BlockHeight <$> getWord64be)
            <*> decodeBlockHash
            <*> (Pact4.TxId <$> getWord64be)
        tag -> fail $ "Chainweb.Pact.Backend.Store.Cold.decodeColdMeta: unknown frontier tag " <> show tag

-- | The end txid of a frontier; @0@ for none.
--
frontierTxId :: Maybe Frontier -> Pact4.TxId
frontierTxId = maybe 0 _frEndTxId

-- -------------------------------------------------------------------------- --
-- Table names

-- | SQLite folds ASCII identifiers only: @A@ to @Z@ become lower case, every
-- other character (non-ASCII included) is unchanged. @N@ keys and catalog
-- lookups use the folded name.
--
foldTableName :: Text -> Text
foldTableName = T.map $ \ch -> if ch >= 'A' && ch <= 'Z'
    then toEnum (fromEnum ch + 32)
    else ch

-- | Pact system tables (prefix @SYS:@). They get no by-txid lists and no key
-- ids.
--
isSysTable :: Text -> Bool
isSysTable = T.isPrefixOf "SYS:"

-- | Whether the freezer moves the history of a table to the cold store.
-- @SYS:Modules@ and @SYS:ModuleSources@ stay whole in SQLite. The freezer
-- only sees tables named in @VersionedTableMutation@, so @TransactionIndex@
-- and the bookkeeping tables never reach this function.
--
frozenTable :: Text -> Bool
frozenTable name = name /= "SYS:Modules" && name /= "SYS:ModuleSources"

-- -------------------------------------------------------------------------- --
-- Store

data ColdOpenMode = ColdReadWrite | ColdReadOnly
    deriving (Eq, Show)

newtype ColdStoreException = ColdStoreException Text
    deriving (Eq, Show)

instance Exception ColdStoreException

coldError :: Text -> IO a
coldError = throwIO . ColdStoreException . ("Chainweb.Pact.Backend.Store.Cold: " <>)

data ColdStore = ColdStore
    { _csDb :: !RocksDb
    , _csMode :: !ColdOpenMode
    , _csOpenChains :: !(MVar (S.Set ChainId))
        -- ^ chains with a 'ColdChain' in this process; a chain is opened once
    }

-- | Opens the cold store at the given directory with 'coldStoreTuning' and a
-- block cache of the given size in bytes. 'ColdReadWrite' creates the
-- database if it does not exist.
--
withColdStore :: FilePath -> ColdOpenMode -> Int -> (ColdStore -> IO a) -> IO a
withColdStore path mode cacheBytes act = do
    openChains <- newMVar mempty
    open path modernDefaultOptions tuning $ \db ->
        act (ColdStore db mode openChains)
  where
    tuning = coldStoreTuning { _tuningBlockCacheBytes = Just cacheBytes }
    open = case mode of
        ColdReadWrite -> withRocksDbTuned
        ColdReadOnly -> withReadOnlyRocksDbTuned

-- | Block cache hits and misses since the store was opened, from the RocksDB
-- statistics.
--
coldCacheStats :: ColdStore -> IO (Maybe (Word64, Word64))
coldCacheStats cs = rocksDbStatistics (_csDb cs) <&&> \stats -> (,)
    <$> statisticsTicker "rocksdb.block.cache.hit" stats
    <*> statisticsTicker "rocksdb.block.cache.miss" stats
  where
    m <&&> f = (f =<<) <$> m

-- | The underlying database, for tools and tests.
--
coldStoreRocksDb :: ColdStore -> RocksDb
coldStoreRocksDb = _csDb

-- | One raw key. For tools and tests.
--
coldRawGet :: ColdStore -> B.ByteString -> IO (Maybe B.ByteString)
coldRawGet cs = R.get (_rocksDbHandle (_csDb cs)) mempty

-- | Every key of the store, in order. Reads the whole store: for tests.
--
coldRawKeys :: ColdStore -> IO [B.ByteString]
coldRawKeys cs = withIterator (_csDb cs) $ \it -> do
    I.iterFirst it
    let go acc = I.iterKey it >>= \case
            Nothing -> return (reverse acc)
            Just k -> I.iterNext it >> go (k : acc)
    go []

withIterator :: RocksDb -> (I.Iterator -> IO a) -> IO a
withIterator db act =
    RO.withReadOptions mempty $ \ro -> I.withIter (_rocksDbHandle db) ro act

-- | The entries whose keys start with the prefix, in order.
--
scanPrefix :: RocksDb -> B.ByteString -> IO [(B.ByteString, B.ByteString)]
scanPrefix db prefix = withIterator db $ \it -> do
    I.iterSeek it prefix
    let go acc = I.iterEntry it >>= \case
            Just (k, v) | prefix `B.isPrefixOf` k -> I.iterNext it >> go ((k, v) : acc)
            _ -> return (reverse acc)
    go []

-- -------------------------------------------------------------------------- --
-- Steps

-- | One row version as SQLite stores it.
--
data StepRow = StepRow
    { _srRowKey :: !B.ByteString
    , _srTxId :: !Pact4.TxId
    , _srRowData :: !B.ByteString
    }
    deriving (Eq, Show)

-- | The rows of whole blocks up to and including the block of '_stpTo': per
-- table (named as stored), the rows in rowid order.
--
data ColdStep = ColdStep
    { _stpTo :: !Frontier
    , _stpTables :: ![(Text, [StepRow])]
    }
    deriving (Eq, Show)

-- -------------------------------------------------------------------------- --
-- Chains

-- | The cold store operations of one chain.
--
data ColdChain = ColdChain
    { _ccChain :: !ChainId
    , _ccMeta :: !(IO ColdMeta)
        -- ^ the meta as last written
    , _ccVerified :: !(IO (Maybe Frontier))
        -- ^ the verified frontier; read it once per operation
    , _ccWritten :: !(IO (Maybe Frontier))
    , _ccWriteStep :: !(ColdStep -> IO ())
        -- ^ writes a step as one synced batch, with the written frontier
        -- '_stpTo'; the verified frontier and '_cmTrimFrom' stay
    , _ccPublish :: !(IO ())
        -- ^ verified := written, '_cmTrimFrom' := the old verified end txid
        -- (synced); then the in-memory frontier moves and the new catalog
        -- entries become visible to readers. Does nothing when verified equals
        -- written.
    , _ccTableId :: !(Text -> IO (Maybe TableId))
        -- ^ the published catalog, looked up with the folded name
    }

-- | The writer state of a chain. It changes only after a batch is written.
--
data WriterState = WriterState
    { _wsMeta :: !ColdMeta
    , _wsTables :: !(HM.HashMap Text TableId)
        -- ^ folded name to table id, including unpublished entries
    , _wsNextTable :: !Word64
    , _wsNextKey :: !(M.Map TableId Word64)
        -- ^ the next key id of the tables seen so far
    , _wsUnpublished :: ![(Text, TableId)]
    }

openColdChain :: ColdStore -> ChainId -> IO ColdChain
openColdChain = openColdChainWith writeBatchSync

-- | 'openColdChain' with another batch writer, for fault injection in tests.
-- The writer must write the operations atomically and synced.
--
openColdChainWith
    :: (RocksDb -> [R.BatchOp] -> IO ())
    -> ColdStore
    -> ChainId
    -> IO ColdChain
openColdChainWith writer cs c = do
    modifyMVar_ (_csOpenChains cs) $ \open -> do
        when (S.member c open) $
            coldError $ "chain " <> chainIdToText c <> " is already open"
        return (S.insert c open)
    meta <- coldRawGet cs (metaKey c) >>= \case
        Nothing -> return emptyColdMeta
        Just bytes -> either
            (\e -> coldError $ "chain " <> chainIdToText c <> ": bad meta: " <> T.pack e)
            return
            (decodeColdMeta bytes)
    catalog <- readCatalog
    metaRef <- newIORef meta
    catalogRef <- newIORef catalog
    writerVar <- newMVar WriterState
        { _wsMeta = meta
        , _wsTables = catalog
        , _wsNextTable = maybe 0 ((+ 1) . tableIdWord) (maximumMay (HM.elems catalog))
        , _wsNextKey = mempty
        , _wsUnpublished = []
        }
    return ColdChain
        { _ccChain = c
        , _ccMeta = readIORef metaRef
        , _ccVerified = _cmVerified <$> readIORef metaRef
        , _ccWritten = _cmWritten <$> readIORef metaRef
        , _ccWriteStep = \s -> do
            checkWritable
            modifyMVar_ writerVar $ \ws -> do
                (ops, ws') <- buildStep ws s
                writer db ops
                writeIORef metaRef (_wsMeta ws')
                return ws'
        , _ccPublish = do
            checkWritable
            modifyMVar_ writerVar $ \ws -> do
                let m = _wsMeta ws
                if _cmWritten m == _cmVerified m
                then return ws
                else do
                    let m' = m
                            { _cmVerified = _cmWritten m
                            , _cmTrimFrom = frontierTxId (_cmVerified m)
                            }
                    writer db [R.Put (metaKey c) (encodeColdMeta m')]
                    writeIORef metaRef m'
                    atomicModifyIORef' catalogRef $ \cat ->
                        (foldl' (\acc (n, t) -> HM.insert n t acc) cat (_wsUnpublished ws), ())
                    return ws { _wsMeta = m', _wsUnpublished = [] }
        , _ccTableId = \name -> HM.lookup (foldTableName name) <$> readIORef catalogRef
        }
  where
    db = _csDb cs

    checkWritable = when (_csMode cs == ColdReadOnly) $
        coldError "the cold store is open read-only"

    maximumMay [] = Nothing
    maximumMay xs = Just (maximum xs)

    tableIdWord (TableId t) = fromIntegral t :: Word64

    -- the N entries of the chain
    readCatalog = do
        let prefix = tableIdKey c ""
        entries <- scanPrefix db prefix
        fmap HM.fromList $ forM entries $ \(k, v) -> do
            name <- either
                (\e -> coldError $ "bad table name in an N key: " <> T.pack (show e))
                return
                (T.decodeUtf8' (B.drop (B.length prefix) k))
            t <- maybe (coldError $ "bad table id of " <> name) return (be32 v)
            return (name, TableId t)

    -- the next key id of a table: one past the id of its last I entry
    nextKeyId ws t = case M.lookup t (_wsNextKey ws) of
        Just n -> return n
        Nothing -> withIterator db $ \it -> do
            let prefix = B.take 6 (idKey c t (KeyId 0))
            iterSeekForPrev it (idKey c t (KeyId maxBound))
            I.iterKey it >>= \case
                Just k | prefix `B.isPrefixOf` k -> case be32 (B.drop 6 k) of
                    Just i -> return (fromIntegral i + 1)
                    Nothing -> coldError $ "bad I key: " <> T.pack (show k)
                _ -> return 0

    lookupKeyId t k = coldRawGet cs (keyIdKey c t k) >>= \case
        Nothing -> return Nothing
        Just v -> maybe (coldError $ "bad key id value: " <> T.pack (show v)) (return . Just . KeyId) (be32 v)

    buildStep ws (ColdStep to tables) = do
        checkStep (_wsMeta ws) to tables
        (ws1, opsByTable) <- mapAccumM (buildTable to) ws tables
        let m' = (_wsMeta ws1) { _cmWritten = Just to }
            ops = concat opsByTable ++ [R.Put (metaKey c) (encodeColdMeta m')]
        return (ops, ws1 { _wsMeta = m' })

    checkStep meta to tables = do
        let lo = frontierTxId (_cmWritten meta)
            hi = _frEndTxId to
        forM_ (_cmWritten meta) $ \w ->
            unless (_frHeight to > _frHeight w && hi >= _frEndTxId w) $
                coldError $ "chain " <> chainIdToText c <> ": step to " <> T.pack (show to)
                    <> " is not after the written frontier " <> T.pack (show w)
        let names = foldTableName . fst <$> tables
        unless (S.size (S.fromList names) == length names) $
            coldError $ "a step names a table twice: " <> T.pack (show (fst <$> tables))
        forM_ tables $ \(name, rows) -> do
            unless (frozenTable name) $
                coldError $ "table " <> name <> " is not frozen"
            forM_ rows $ \r -> unless (_srTxId r >= lo && _srTxId r < hi) $
                coldError $ "table " <> name <> ": row txid " <> T.pack (show (_srTxId r))
                    <> " outside [" <> T.pack (show lo) <> ", " <> T.pack (show hi) <> ")"

    buildTable _to ws (name, rows) = do
        let folded = foldTableName name
        (t, ws1, tableOps) <- case HM.lookup folded (_wsTables ws) of
            Just t -> return (t, ws, [])
            Nothing -> do
                let n = _wsNextTable ws
                when (n > fromIntegral (maxBound :: Word32)) $
                    coldError "table ids exhausted"
                let t = TableId (fromIntegral n)
                return
                    ( t
                    , ws { _wsTables = HM.insert folded t (_wsTables ws)
                         , _wsNextTable = n + 1
                         , _wsUnpublished = (folded, t) : _wsUnpublished ws
                         }
                    , [R.Put (tableIdKey c folded) (word32BE (tableIdWord t))]
                    )
        let rowOps = [ R.Put (rowKey c t (_srRowKey r) (txIdWord (_srTxId r))) (_srRowData r) | r <- rows ]
        if isSysTable name
        then return (ws1, tableOps ++ rowOps)
        else do
            next0 <- nextKeyId ws1 t
            (next, _, keyOps, byTxId) <- foldM (keyStep t) (next0, HM.empty, [], M.empty) rows
            let txOps =
                    [ R.Put (txidKey c t txid) (encodeKeyIdList (reverse ids))
                    | (txid, ids) <- M.toAscList byTxId
                    ]
            return
                ( ws1 { _wsNextKey = M.insert t next (_wsNextKey ws1) }
                , tableOps ++ rowOps ++ reverse keyOps ++ txOps
                )

    -- assigns key ids in first-seen order and collects the T lists
    keyStep t (!next, !seen, keyOps, byTxId) r = do
        let k = _srRowKey r
        (kid, next', seen', keyOps') <- case HM.lookup k seen of
            Just kid -> return (kid, next, seen, keyOps)
            Nothing -> lookupKeyId t k >>= \case
                Just kid -> return (kid, next, HM.insert k kid seen, keyOps)
                Nothing -> do
                    when (next > fromIntegral (maxBound :: Word32)) $
                        coldError "key ids exhausted"
                    let kid = KeyId (fromIntegral next)
                    return
                        ( kid
                        , next + 1
                        , HM.insert k kid seen
                        , R.Put (idKey c t kid) k : R.Put (keyIdKey c t k) (word32BE next) : keyOps
                        )
        let byTxId' = M.insertWith (++) (txIdWord (_srTxId r)) [kid] byTxId
        return (next', seen', keyOps', byTxId')

-- -------------------------------------------------------------------------- --
-- Helpers

mapAccumM :: Monad m => (s -> a -> m (s, b)) -> s -> [a] -> m (s, [b])
mapAccumM f = go []
  where
    go acc s [] = return (s, reverse acc)
    go acc s (x : xs) = do
        (s', y) <- f s x
        go (y : acc) s' xs

-- | Four bytes, big-endian.
word32BE :: Word64 -> B.ByteString
word32BE = BL.toStrict . BB.toLazyByteString . BB.word32BE . fromIntegral

be32 :: B.ByteString -> Maybe Word32
be32 b
    | B.length b == 4 = Just $! B.foldl' (\a x -> a `shiftL` 8 .|. fromIntegral x) 0 b
    | otherwise = Nothing
