{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.StorageTools.Measure.Cold
--
-- Cold-store measurement (storage redesign, spec 4.3 and 11.3, P0).
--
-- 'buildColdFromSqlite' copies the history of one chain's Pact SQLite file
-- into a RocksDB instance with the cold-store layout and options of spec 4.3
-- and measures it. 'measureColdReads' times the cold point read (seek and one
-- step back) and checks every answer against SQLite. 'projectCold' turns the
-- measurements into the Pact-state total over all 20 chains and the verdict
-- between option R (the RocksDB cold store) and option S (segment files,
-- spec 9).
--
-- The legacy file is only read. Rows are copied as bytes and never decoded.
--
module Chainweb.StorageTools.Measure.Cold
(
-- * Building the cold store
  ColdBuildSpec(..)
, defaultBatchBytes
, TableSizes(..)
, ColdSizes(..)
, buildColdFromSqlite
, keptTables
, transactionIndexTable
, coldKeyspaces

-- * Derived sizes
, tableLogicalBytes
, coldLogicalBytes
, coldRatio
, chainProjectedBytes

-- * Cold reads
, ColdLatency(..)
, measureColdReads
, coldReadNewest
, scanPrefix

-- * Projection
, RorS(..)
, ProjectionInputs(..)
, Projection(..)
, projectCold
, specPactEstimateGB
, specMarginGB
, specChain0HotBytes
, chain0LogicalFromWalk
, latencyVerdict

-- * Rendering
, renderColdSizes
, renderColdLatency
, renderProjection
) where

import Control.Exception (bracket, evaluate)
import Control.Monad

import Data.Aeson
import qualified Data.Aeson.KeyMap as AKM
import Data.Aeson.Types (Parser, parseEither)
import qualified Data.ByteString as B
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BL
import Data.Char (toLower)
import qualified Data.HashMap.Strict as HM
import Data.IORef
import Data.List (sort)
import qualified Data.Map.Strict as M
import Data.Maybe
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word

import GHC.Clock (getMonotonicTimeNSec)
import GHC.Generics (Generic)

import System.Directory
import System.LogLevel (LogLevel(..))
import System.Random
import Text.Printf (printf)
import Text.Read (readMaybe)

import qualified Database.RocksDB.Base as R
import qualified Database.RocksDB.Iterator as I
import qualified Database.RocksDB.ReadOptions as R
import qualified Database.SQLite3 as SQ

-- internal modules

import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.Pact.Backend.Store.Cold.Key
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning

-- -------------------------------------------------------------------------- --
-- Building the cold store

-- | What 'buildColdFromSqlite' copies, and where to.
--
data ColdBuildSpec = ColdBuildSpec
    { _cbSqlite :: !FilePath
        -- ^ the legacy Pact SQLite file of the chain; opened read-only
    , _cbChain :: !ChainId
    , _cbTables :: !(Maybe [Text])
        -- ^ the tables to copy ('transactionIndexTable' included); 'Nothing'
        -- copies all versioned tables and the transaction index
    , _cbOut :: !FilePath
        -- ^ the RocksDB directory of the cold store; created
    , _cbBatchBytes :: !Int
        -- ^ upper bound of the key and value bytes of one write batch
    }
    deriving (Show, Eq)

-- | 64 MiB
--
defaultBatchBytes :: Int
defaultBatchBytes = 64 * 1024 * 1024

-- | Sizes of one table.
--
-- For a versioned table a version is one SQLite row, the key bytes are the
-- row key bytes plus 8 bytes of txid (the logical part of an @R@ key), and the
-- value bytes are the row bytes. For 'transactionIndexTable' a version is one
-- transaction, the key bytes are the hash bytes and the value bytes are the 8
-- bytes of the block height.
--
data TableSizes = TableSizes
    { _tsVersions :: !Word64
    , _tsDistinctKeys :: !Word64
    , _tsKeyBytes :: !Word64
    , _tsValueBytes :: !Word64
    , _tsSstBytes :: !Word64
        -- ^ SST bytes of the table's @R@ range (@X@ for the transaction
        -- index) after the forced compaction. An approximation by RocksDB at
        -- data-block granularity (32 KB); index, filter and metadata blocks
        -- are only in '_csTotalSst'.
    , _tsSqliteBytes :: !Word64
        -- ^ pages of the table and its indexes in the SQLite file (@dbstat@)
    }
    deriving (Show, Eq, Generic)

-- | The measurement of one chain.
--
data ColdSizes = ColdSizes
    { _csChain :: !Text
    , _csSqliteFileBytes :: !Word64
        -- ^ size of the legacy SQLite file today
    , _csTables :: !(M.Map Text TableSizes)
    , _csKeyspaceSst :: !(M.Map Text Word64)
        -- ^ SST bytes per keyspace (@R T K I N X@) after the forced
        -- compaction; approximate like '_tsSstBytes'
    , _csTotalSst :: !Word64
        -- ^ @rocksdb.total-sst-files-size@ after the forced compaction
    , _csHotKeptBytes :: !Word64
        -- ^ SQLite pages of the tables that stay whole in SQLite
        -- ('keptTables') and their indexes
    , _csHotAnchorBytes :: !Word64
        -- ^ the anchors that stay in SQLite: per copied versioned table except
        -- @SYS:Pacts@, its SQLite pages times distinct keys over versions
    }
    deriving (Show, Eq, Generic)

-- | Tables that stay whole in SQLite (spec 4.3, "Writes").
--
keptTables :: [Text]
keptTables = ["BlockHistory", "VersionedTableCreation", "VersionedTableMutation"]

-- | Copied to the @X@ keyspace.
--
transactionIndexTable :: Text
transactionIndexTable = "TransactionIndex"

-- | The keyspaces that the build writes.
--
coldKeyspaces :: [Char]
coldKeyspaces = "RTKINX"

isSysTable :: Text -> Bool
isSysTable = T.isPrefixOf "SYS:"

-- | Copies the history of one chain into a fresh cold store and measures it.
--
-- Every versioned table is read @ORDER BY rowid@. Each row becomes one @R@
-- entry with the row bytes verbatim. User tables (not @SYS:*@) also get
-- @K@ and @I@ entries for each row key, numbered in order of first appearance,
-- and one @T@ entry per txid that lists the key ids of that txid's rows in
-- ascending rowid order. The @T@ lists are buffered per block; the block
-- boundaries are the @endingtxid@s of @BlockHistory@. Each table gets an @N@
-- entry, with table ids in the order of the table names. The transaction
-- index becomes @X@ entries (hash to block height, 8 bytes big-endian).
--
-- A write batch holds at most '_cbBatchBytes' key and value bytes. At the end
-- the memtables are flushed and the whole database is compacted with the
-- bottommost level forced, so that every SST file is written with the cold
-- tuning. Fails if an SST file is then not zstd with the 64 KB dictionary.
--
buildColdFromSqlite :: Logger l => l -> ColdBuildSpec -> IO ColdSizes
buildColdFromSqlite logger spec = withSqliteReadOnly (_cbSqlite spec) $ \sq -> do
    allTables <- versionedTables sq
    hasTxIndex <- tableExists sq transactionIndexTable
    let available = allTables <> [ transactionIndexTable | hasTxIndex ]
    selected <- case _cbTables spec of
        Nothing -> return available
        Just names -> do
            forM_ names $ \n -> unless (n `elem` available) $
                ioError $ userError $ "table not found or not copyable: " <> T.unpack n
            return $ filter (`elem` names) available
    let tables = filter (/= transactionIndexTable) selected
        copyTxIndex = transactionIndexTable `elem` selected
    ends <- blockEnds sq
    fileBytes <- fromIntegral <$> getFileSize (_cbSqlite spec)
    exists <- doesPathExist (_cbOut spec)
    when exists $ ioError $ userError $ "output exists: " <> _cbOut spec
    sizes <- withRocksDbTuned (_cbOut spec) modernDefaultOptions coldStoreTuning $ \db -> do
        w <- newWriter (_rocksDbHandle db) (_cbBatchBytes spec)
        copied <- forM (zip [0 ..] tables) $ \(i, name) -> do
            let tid = TableId i
            putW w (tableIdKey c name) (word32 i)
            s <- copyTable w ends c tid name sq
            logFunctionText logger Info $ "copied " <> name <> ": " <> sshowT (_tsVersions s) <> " versions"
            return (name, tid, s)
        txIndex <- if copyTxIndex
            then do
                s <- copyTransactionIndex w c sq
                logFunctionText logger Info $ "copied " <> transactionIndexTable <> ": " <> sshowT (_tsVersions s) <> " entries"
                return [(transactionIndexTable, s)]
            else return []
        flushW w
        flushRocksDb db
        compactAllForced db

        -- sizes after compaction
        tableSizes <- forM copied $ \(name, TableId i, s) -> do
            sst <- approximateSize db (rangeKey 'R' c i) (rangeKey 'R' c (i + 1))
            sqb <- sqliteTableBytes sq name
            return (name, s { _tsSstBytes = sst, _tsSqliteBytes = sqb })
        txSizes <- forM txIndex $ \(name, s) -> do
            sst <- keyspaceBytes db 'X'
            sqb <- sqliteTableBytes sq name
            return (name, s { _tsSstBytes = sst, _tsSqliteBytes = sqb })
        keyspaces <- forM coldKeyspaces $ \ks -> (,) (T.singleton ks) <$> keyspaceBytes db ks
        total <- rocksDbProperty db "rocksdb.total-sst-files-size" >>= \case
            Just v | Just n <- readMaybe (T.unpack (T.decodeUtf8 v)) -> return n
            v -> ioError $ userError $ "rocksdb.total-sst-files-size: " <> show v
        kept <- sum <$> mapM (sqliteTableBytes sq) keptTables
        let anchors = sum
                [ anchorBytes s
                | (name, s) <- tableSizes
                , name /= "SYS:Pacts"
                ]
        return ColdSizes
            { _csChain = chainIdToText c
            , _csSqliteFileBytes = fileBytes
            , _csTables = M.fromList (tableSizes <> txSizes)
            , _csKeyspaceSst = M.fromList keyspaces
            , _csTotalSst = total
            , _csHotKeptBytes = kept
            , _csHotAnchorBytes = anchors
            }
    -- compactAllForced cannot report failure: check that every SST file was
    -- written with the cold tuning
    info <- sstCompressionInfo (_cbOut spec)
    let bad = M.filter
            (\(comp, opts) -> comp /= "ZSTD" || not ("max_dict_bytes=65536;" `B.isInfixOf` opts))
            info
    unless (M.null bad) $ ioError $ userError $
        "SST files not written with the cold tuning: " <> show bad
    return sizes
  where
    c = _cbChain spec
    anchorBytes s
        | _tsVersions s == 0 = 0
        | otherwise = round
            (fromIntegral (_tsSqliteBytes s) * fromIntegral (_tsDistinctKeys s)
                / fromIntegral (_tsVersions s) :: Double)

-- | @ch c id(4)@, the start of the range of a table in a keyspace.
--
rangeKey :: Char -> ChainId -> Word32 -> B.ByteString
rangeKey ch c i = BL.toStrict $ BB.toLazyByteString $
    BB.char7 ch <> BB.word8 (chainByte c) <> BB.word32BE i

keyspaceBytes :: RocksDb -> Char -> IO Word64
keyspaceBytes db ks = approximateSize db (B.singleton (c2w ks)) (B.singleton (c2w ks + 1))
  where
    c2w = fromIntegral . fromEnum

word32 :: Word32 -> B.ByteString
word32 = BL.toStrict . BB.toLazyByteString . BB.word32BE

word64 :: Word64 -> B.ByteString
word64 = BL.toStrict . BB.toLazyByteString . BB.word64BE

sshowT :: Show a => a -> Text
sshowT = T.pack . show

-- -------------------------------------------------------------------------- --
-- Copying

-- | Accumulates puts into write batches of at most the given number of key and
-- value bytes (a single larger entry gets a batch of its own).
--
data Writer = Writer
    { _wDb :: !R.DB
    , _wLimit :: !Int
    , _wOps :: !(IORef ([R.BatchOp], Int))
    }

newWriter :: R.DB -> Int -> IO Writer
newWriter db limit = do
    when (limit <= 0) $ ioError $ userError $ "invalid batch bytes: " <> show limit
    Writer db limit <$> newIORef ([], 0)

putW :: Writer -> B.ByteString -> B.ByteString -> IO ()
putW w k v = do
    let n = B.length k + B.length v
    (_, sz) <- readIORef (_wOps w)
    when (sz > 0 && sz + n > _wLimit w) $ flushW w
    modifyIORef' (_wOps w) $ \(ops, s) -> (R.Put k v : ops, s + n)

flushW :: Writer -> IO ()
flushW w = do
    (ops, _) <- readIORef (_wOps w)
    unless (null ops) $ R.write (_wDb w) R.defaultWriteOptions (reverse ops)
    writeIORef (_wOps w) ([], 0)

-- | Per-table copy state.
--
data CopyState = CopyState
    { _stVersions :: !Word64
    , _stKeyBytes :: !Word64
    , _stValueBytes :: !Word64
    , _stKeyIds :: !(HM.HashMap B.ByteString Word32)
    , _stBlock :: !(Word64, Word64)
        -- ^ txid range @[start, end)@ of the block whose @T@ lists are buffered
    , _stBuffer :: !(M.Map Word64 [KeyId])
        -- ^ per txid, key ids in descending rowid order
    , _stFlushedEnd :: !Word64
        -- ^ the largest block end whose @T@ lists were written
    }

copyTable
    :: Writer
    -> S.Set Word64
    -> ChainId
    -> TableId
    -> Text
    -> SQ.Database
    -> IO TableSizes
copyTable w ends c tid name sq = do
    let user = not (isSysTable name)
        query = "SELECT rowkey, txid, rowdata FROM " <> quoteName name <> " ORDER BY rowid"
    final <- withStatement sq query [] $ \stmt -> do
        let go !st = SQ.step stmt >>= \case
                SQ.Done -> return st
                SQ.Row -> do
                    k <- SQ.columnBlob stmt 0
                    txid <- fromIntegral <$> SQ.columnInt64 stmt 1
                    v <- SQ.columnBlob stmt 2
                    putW w (rowKey c tid k txid) v
                    (kid, ids') <- case HM.lookup k (_stKeyIds st) of
                        Just i -> return (i, _stKeyIds st)
                        Nothing -> do
                            let i = fromIntegral (HM.size (_stKeyIds st))
                            when user $ do
                                putW w (keyIdKey c tid k) (word32 i)
                                putW w (idKey c tid (KeyId i)) k
                            return (i, HM.insert k i (_stKeyIds st))
                    st' <- if user
                        then bufferT txid (KeyId kid) st
                        else return st
                    go st'
                        { _stVersions = _stVersions st + 1
                        , _stKeyBytes = _stKeyBytes st + fromIntegral (B.length k) + 8
                        , _stValueBytes = _stValueBytes st + fromIntegral (B.length v)
                        , _stKeyIds = ids'
                        }
        go (CopyState 0 0 0 HM.empty (0, 0) M.empty 0)
    void $ flushT final
    return TableSizes
        { _tsVersions = _stVersions final
        , _tsDistinctKeys = fromIntegral (HM.size (_stKeyIds final))
        , _tsKeyBytes = _stKeyBytes final
        , _tsValueBytes = _stValueBytes final
        , _tsSstBytes = 0
        , _tsSqliteBytes = 0
        }
  where
    blockOf txid =
        ( fromMaybe 0 (S.lookupLE txid ends)
        , fromMaybe maxBound (S.lookupGT txid ends)
        )

    bufferT txid kid st = do
        let (start, end) = _stBlock st
        st' <- if txid >= start && txid < end
            then return st
            else do
                st1 <- flushT st
                return st1 { _stBlock = blockOf txid }
        return st' { _stBuffer = M.insertWith (<>) txid [kid] (_stBuffer st') }

    -- Writes the buffered lists of the current block. If a block at or before
    -- one already written comes back (rows out of block order), the lists
    -- are appended to what was written: later rows have larger rowids.
    flushT st
        | M.null (_stBuffer st) = return st
        | otherwise = do
            let (_, end) = _stBlock st
                revisit = end <= _stFlushedEnd st
            forM_ (M.toList (_stBuffer st)) $ \(txid, revIds) -> do
                let key = txidKey c tid txid
                    ids = reverse revIds
                old <- if revisit
                    then do
                        flushW w
                        R.get (_wDb w) mempty key >>= \case
                            Nothing -> return []
                            Just bytes -> case decodeKeyIdList bytes of
                                Just l -> return l
                                Nothing -> ioError $ userError $ "corrupt T entry: " <> show key
                    else return []
                putW w key (encodeKeyIdList (old <> ids))
            return st
                { _stBuffer = M.empty
                , _stFlushedEnd = max end (_stFlushedEnd st)
                }

copyTransactionIndex :: Writer -> ChainId -> SQ.Database -> IO TableSizes
copyTransactionIndex w c sq =
    withStatement sq "SELECT txhash, blockheight FROM TransactionIndex ORDER BY rowid" [] $ \stmt -> do
        let go !n !kb = SQ.step stmt >>= \case
                SQ.Done -> return (n, kb)
                SQ.Row -> do
                    h <- SQ.columnBlob stmt 0
                    height <- fromIntegral <$> SQ.columnInt64 stmt 1
                    putW w (txIndexKey c h) (word64 height)
                    go (n + 1) (kb + fromIntegral (B.length h))
        (n, kb) <- go (0 :: Word64) (0 :: Word64)
        return TableSizes
            { _tsVersions = n
            , _tsDistinctKeys = n
            , _tsKeyBytes = kb
            , _tsValueBytes = 8 * n
            , _tsSstBytes = 0
            , _tsSqliteBytes = 0
            }

-- -------------------------------------------------------------------------- --
-- SQLite

withSqliteReadOnly :: FilePath -> (SQ.Database -> IO a) -> IO a
withSqliteReadOnly path = bracket
    (SQ.open2 (T.pack path) [SQ.SQLOpenReadOnly] SQ.SQLVFSDefault)
    SQ.close

withStatement :: SQ.Database -> Text -> [SQ.SQLData] -> (SQ.Statement -> IO a) -> IO a
withStatement db q params k = bracket (SQ.prepare db q) SQ.finalize $ \stmt -> do
    SQ.bind stmt params
    k stmt

queryAll :: SQ.Database -> Text -> [SQ.SQLData] -> IO [[SQ.SQLData]]
queryAll db q params = withStatement db q params $ \stmt -> do
    let go acc = SQ.step stmt >>= \case
            SQ.Done -> return (reverse acc)
            SQ.Row -> SQ.columns stmt >>= \r -> go (r : acc)
    go []

quoteName :: Text -> Text
quoteName n = "[" <> n <> "]"

-- | The versioned (Pact) tables of the file, in name order: every table except
-- the block bookkeeping tables, the transaction index and SQLite's own.
--
versionedTables :: SQ.Database -> IO [Text]
versionedTables db = do
    rows <- queryAll db "SELECT name FROM sqlite_master WHERE type = 'table'" []
    return $ sort
        [ n
        | [SQ.SQLText n] <- rows
        , n `notElem` (transactionIndexTable : keptTables)
        , not ("sqlite_" `T.isPrefixOf` n)
        ]

tableExists :: SQ.Database -> Text -> IO Bool
tableExists db n = not . null <$>
    queryAll db "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?" [SQ.SQLText n]

-- | The @endingtxid@ of every block.
--
blockEnds :: SQ.Database -> IO (S.Set Word64)
blockEnds db = do
    rows <- queryAll db "SELECT endingtxid FROM BlockHistory" []
    return $ S.fromList [ fromIntegral i | [SQ.SQLInteger i] <- rows ]

-- | Bytes of the pages of a table and its indexes, from @dbstat@.
--
sqliteTableBytes :: SQ.Database -> Text -> IO Word64
sqliteTableBytes db name = do
    trees <- queryAll db "SELECT name FROM sqlite_master WHERE tbl_name = ? AND type IN ('table', 'index')" [SQ.SQLText name]
    fmap sum $ forM [ t | [SQ.SQLText t] <- trees ] $ \t ->
        queryAll db "SELECT coalesce(sum(pgsize), 0) FROM dbstat WHERE name = ?" [SQ.SQLText t] >>= \case
            [[SQ.SQLInteger n]] -> return (fromIntegral n)
            r -> ioError $ userError $ "dbstat " <> T.unpack t <> ": " <> show r

-- -------------------------------------------------------------------------- --
-- Derived sizes

-- | Key and value bytes of a table.
--
tableLogicalBytes :: TableSizes -> Word64
tableLogicalBytes ts = _tsKeyBytes ts + _tsValueBytes ts

-- | Key and value bytes of every copied table.
--
coldLogicalBytes :: ColdSizes -> Word64
coldLogicalBytes = sum . fmap tableLogicalBytes . _csTables

-- | The real-engine ratio: logical bytes of everything copied over the SST
-- bytes of the whole cold store (every keyspace, so the by-txid lists, key
-- ids and the transaction index are paid for).
--
coldRatio :: ColdSizes -> Double
coldRatio cs
    | _csTotalSst cs == 0 = 0
    | otherwise = fromIntegral (coldLogicalBytes cs) / fromIntegral (_csTotalSst cs)

-- | The chain after the redesign: the cold store plus what stays in SQLite
-- (the kept tables and the anchors).
--
chainProjectedBytes :: ColdSizes -> Word64
chainProjectedBytes cs = _csTotalSst cs + _csHotKeptBytes cs + _csHotAnchorBytes cs

-- -------------------------------------------------------------------------- --
-- Cold reads

-- | Cold point reads, checked against SQLite.
--
data ColdLatency = ColdLatency
    { _clReads :: !Int
        -- ^ sampled reads; each is done once per timed pass
    , _clFound :: !Int
        -- ^ reads for which SQLite returns a row
    , _clMismatches :: !Int
        -- ^ answers, over all passes, that differ from SQLite's
    , _clWarmP50Us :: !Double
    , _clWarmP99Us :: !Double
    , _clColdP50Us :: !Double
    , _clColdP99Us :: !Double
    , _clColdMaxUs :: !Double
    }
    deriving (Show, Eq, Generic)

-- | All entries whose key starts with the prefix, in key order.
--
scanPrefix :: R.DB -> B.ByteString -> IO [(B.ByteString, B.ByteString)]
scanPrefix db prefix = R.withReadOptions mempty $ \ro -> I.withIter db ro $ \it -> do
    I.iterSeek it prefix
    let go acc = I.iterEntry it >>= \case
            Just (k, v) | prefix `B.isPrefixOf` k -> I.iterNext it >> go ((k, v) : acc)
            _ -> return (reverse acc)
    go []

-- | The cold read of spec 4.3: seek to @R c t esc(key) bound@, step back one
-- entry (from the last entry if the seek ran past the end), and accept the
-- entry if its row key is the key. Returns the newest version with
-- @txid < bound@.
--
coldReadNewest :: R.DB -> ChainId -> TableId -> B.ByteString -> Word64 -> IO (Maybe B.ByteString)
coldReadNewest db c tid key bound = R.withReadOptions mempty $ \ro -> I.withIter db ro $ \it -> do
    I.iterSeek it (rowKey c tid key bound)
    valid <- I.iterValid it
    if valid then I.iterPrev it else I.iterLast it
    I.iterEntry it >>= \case
        Just (k, v) | rowKeyPrefix c tid key `B.isPrefixOf` k -> return (Just v)
        _ -> return Nothing

-- | @measureColdReads sqlite coldDir n seed@ draws @n@ reads (table, key,
-- bound) from the SQLite file and answers each from the cold store, which was
-- built from that file by 'buildColdFromSqlite'.
--
-- A read's key and txid come from a uniformly drawn row of a table drawn by
-- its number of rows. Its bound is, in turn, that txid plus one (the row
-- itself), that txid (an older version or none) or a uniform txid up to past
-- the last block (anything, including the seek running past the end).
--
-- The expected answer is SQLite's @SELECT rowdata ... WHERE rowkey = ? AND
-- txid < ? ORDER BY txid DESC LIMIT 1@. The warm pass opens the store with
-- 'coldStoreTuning', reads every key once untimed and once timed. The cold
-- pass opens it afresh with direct reads and an 8 MiB block cache and reads
-- every key once. Every answer is compared with SQLite's.
--
measureColdReads :: FilePath -> FilePath -> Int -> Word64 -> IO ColdLatency
measureColdReads sqlitePath coldDir n seed = do
    (c, tids) <- withReadOnlyRocksDbTuned coldDir modernDefaultOptions coldStoreTuning $ \db ->
        readCatalog (_rocksDbHandle db)
    samples <- withSqliteReadOnly sqlitePath $ \sq -> drawReads sq tids
    when (length samples < n) $ ioError $ userError $
        "drew only " <> show (length samples) <> " of " <> show n <> " samples"
    let check db (tid, k, bound, expected) = do
            got <- coldReadNewest db c tid k bound
            void $ evaluate got
            return (got /= expected)
        timed db r = do
            t0 <- getMonotonicTimeNSec
            bad <- check db r
            t1 <- getMonotonicTimeNSec
            return (bad, t1 - t0)
    (badWarm, warm) <- withReadOnlyRocksDbTuned coldDir modernDefaultOptions coldStoreTuning $ \db -> do
        bad0 <- mapM (check (_rocksDbHandle db)) samples
        rs <- mapM (timed (_rocksDbHandle db)) samples
        return (length (filter id bad0) + length (filter fst rs), snd <$> rs)
    let coldTuning = coldStoreTuning
            { _tuningDirectReads = True
            , _tuningBlockCacheBytes = Just (8 * 1024 * 1024)
            }
    (badCold, cold) <- withReadOnlyRocksDbTuned coldDir modernDefaultOptions coldTuning $ \db -> do
        rs <- mapM (timed (_rocksDbHandle db)) samples
        return (length (filter fst rs), snd <$> rs)
    let warmS = sort warm
        coldS = sort cold
    return ColdLatency
        { _clReads = length samples
        , _clFound = length [ () | (_, _, _, Just _) <- samples ]
        , _clMismatches = badWarm + badCold
        , _clWarmP50Us = pctUs 0.50 warmS
        , _clWarmP99Us = pctUs 0.99 warmS
        , _clColdP50Us = pctUs 0.50 coldS
        , _clColdP99Us = pctUs 0.99 coldS
        , _clColdMaxUs = pctUs 1 coldS
        }
  where
    -- nearest rank
    pctUs :: Double -> [Word64] -> Double
    pctUs _ [] = 0
    pctUs p xs =
        let cnt = length xs
        in fromIntegral (xs !! max 0 (min (cnt - 1) (ceiling (p * fromIntegral cnt) - 1))) / 1000

    readCatalog db = do
        entries <- scanPrefix db "N"
        let parsed =
                [ (B.index k 1, (T.decodeUtf8 (B.drop 2 k), v))
                | (k, v) <- entries
                , B.length k >= 2
                ]
        case S.toList (S.fromList (fst <$> parsed)) of
            [cb] -> case [ c | c <- cids, chainByte c == cb ] of
                (c : _) -> do
                    tids <- forM (snd <$> parsed) $ \(name, v) -> case B.unpack v of
                        [a, b, x, y] -> return (name, TableId (be32 a b x y))
                        _ -> ioError $ userError $ "invalid table id of " <> T.unpack name
                    return (c, tids)
                [] -> ioError $ userError $ "unknown chain byte " <> show cb
            bs -> ioError $ userError $ "expected the tables of one chain, got chain bytes " <> show bs

    cids = unsafeChainId <$> [0 .. 255]

    be32 a b x y = foldl (\acc w -> acc * 256 + fromIntegral w) 0 [a, b, x, y]

    drawReads sq tids = do
        counts <- forM tids $ \(name, tid) ->
            queryAll sq ("SELECT coalesce(max(rowid), 0) FROM " <> quoteName name) [] >>= \case
                [[SQ.SQLInteger m]] -> return (name, tid, fromIntegral m :: Int)
                r -> ioError $ userError $ "max rowid of " <> T.unpack name <> ": " <> show r
        maxTx <- queryAll sq "SELECT coalesce(max(endingtxid), 0) FROM BlockHistory" [] >>= \case
            [[SQ.SQLInteger m]] -> return (fromIntegral m :: Word64)
            r -> ioError $ userError $ "max endingtxid: " <> show r
        let weighted = filter (\(_, _, m) -> m > 0) counts
            totalRows = sum [ m | (_, _, m) <- weighted ]
            pick r = go' weighted r
              where
                go' ((nm, t, m) : rest) x
                    | x < m = (nm, t, m)
                    | otherwise = go' rest (x - m)
                go' [] _ = error "measureColdReads: empty table list"
        when (totalRows == 0) $ ioError $ userError "no rows to sample"
        gen <- newIORef (mkStdGen (fromIntegral seed))
        let rand :: Int -> Int -> IO Int
            rand lo hi = atomicModifyIORef' gen $ \g -> let (x, g') = uniformR (lo, hi) g in (g', x)
            drawOne attempts
                | attempts > (100 :: Int) = ioError $ userError "too many missing rowids while sampling"
                | otherwise = do
                    (name, tid, m) <- pick <$> rand 0 (totalRows - 1)
                    rid <- rand 1 m
                    queryAll sq ("SELECT rowkey, txid FROM " <> quoteName name <> " WHERE rowid = ?")
                        [SQ.SQLInteger (fromIntegral rid)] >>= \case
                        [[k, SQ.SQLInteger txid]] -> do
                            kind <- rand 0 2
                            bound <- case kind of
                                0 -> return (fromIntegral txid + 1)
                                1 -> return (fromIntegral txid)
                                _ -> fromIntegral <$> rand 0 (fromIntegral maxTx + 1)
                            let keyBytes = case k of
                                    SQ.SQLText t -> T.encodeUtf8 t
                                    SQ.SQLBlob b -> b
                                    _ -> B.empty
                            expected <- queryAll sq
                                ("SELECT rowdata FROM " <> quoteName name
                                    <> " WHERE rowkey = CAST(?1 AS TEXT) AND txid < ?2 ORDER BY txid DESC LIMIT 1")
                                [SQ.SQLBlob keyBytes, SQ.SQLInteger (fromIntegral bound)] >>= \case
                                    [] -> return Nothing
                                    [[SQ.SQLBlob v]] -> return (Just v)
                                    [[SQ.SQLText v]] -> return (Just (T.encodeUtf8 v))
                                    r -> ioError $ userError $ "unexpected SQLite answer: " <> show r
                            return (tid, keyBytes, bound, expected)
                        _ -> drawOne (attempts + 1)
        replicateM n (drawOne 0)

-- -------------------------------------------------------------------------- --
-- Projection

-- | Option R, the RocksDB cold store (spec 4.3), or option S, segment files
-- (spec 9).
--
data RorS = R | S
    deriving (Show, Eq, Ord, Generic)

-- | Inputs of 'projectCold'. Byte counts are bytes; GB are 10^9 bytes.
--
data ProjectionInputs = ProjectionInputs
    { _piChain0Logical :: !(M.Map Text (Word64, Word64))
        -- ^ chain 0, per copied table: logical key and value bytes from the
        -- report's raw data ('chain0LogicalFromWalk')
    , _piChain0Ratio :: !Double
        -- ^ the real-engine ratio applied to chain 0 ('coldRatio' of a
        -- measured chain)
    , _piChain3 :: !ColdSizes
    , _piChain19 :: !ColdSizes
    , _piHot :: !Word64
        -- ^ chain 0's hot parts, anchors and system tables
        -- ('specChain0HotBytes')
    , _piOthersTodayBytes :: !Word64
        -- ^ the SQLite files of the 17 chains other than 0, 3 and 19 today;
        -- they are projected at chain 3's ratio
    }
    deriving (Show, Eq, Generic)

data Projection = Projection
    { _projChain0GB :: !Double
    , _projChain19GB :: !Double
    , _projChain3GB :: !Double
    , _projOthersGB :: !Double
        -- ^ the 17 other chains
    , _projChain3Ratio :: !Double
        -- ^ chain 3 today over chain 3 projected
    , _projPactTotalGB :: !Double
    , _projLimitGB :: !Double
        -- ^ 'specPactEstimateGB' plus 'specMarginGB'
    , _projVerdict :: !RorS
    }
    deriving (Show, Eq, Generic)

-- | Spec 4.3: "Pact state total: 83 GB expected".
--
specPactEstimateGB :: Double
specPactEstimateGB = 83

-- | Spec 11.3: "cold store at most 25 GB above its estimate (4.3, 4.4), else
-- option S".
--
specMarginGB :: Double
specMarginGB = 25

-- | Spec 4.3, chain 0: anchors in SQLite 0.6 GB plus system tables 2.4 GB.
--
specChain0HotBytes :: Word64
specChain0HotBytes = 3000000000

gb :: Word64 -> Double
gb x = fromIntegral x / 1e9

-- | 20 chains: chain 0 from its logical bytes over the real-engine ratio plus
-- its hot parts; chains 19 and 3 as measured ('chainProjectedBytes'); the 17
-- others at chain 3's ratio of today's to projected bytes. The verdict is R
-- if the total is at most 'specPactEstimateGB' + 'specMarginGB'.
--
projectCold :: ProjectionInputs -> Projection
projectCold pin = Projection
    { _projChain0GB = chain0
    , _projChain19GB = chain19
    , _projChain3GB = chain3
    , _projOthersGB = others
    , _projChain3Ratio = ratio3
    , _projPactTotalGB = total
    , _projLimitGB = limit
    , _projVerdict = if total <= limit then R else S
    }
  where
    logical0 = sum [ k + v | (k, v) <- M.elems (_piChain0Logical pin) ]
    chain0 = gb logical0 / _piChain0Ratio pin + gb (_piHot pin)
    chain19 = gb (chainProjectedBytes (_piChain19 pin))
    chain3 = gb (chainProjectedBytes (_piChain3 pin))
    ratio3 = gb (_csSqliteFileBytes (_piChain3 pin)) / chain3
    others = gb (_piOthersTodayBytes pin) / ratio3
    total = chain0 + chain19 + chain3 + others
    limit = specPactEstimateGB + specMarginGB

-- | Chain 0's logical bytes per copied table from the report's SQLite walk
-- (@cw0.json@: @trees@, each with @type@, @table@, @cells@, @key_bytes@,
-- @val_bytes@). The same definitions as 'TableSizes': a versioned table's key
-- bytes are its row key bytes plus 8 bytes of txid per row; the transaction
-- index (whose columns the walk does not split) counts 32 hash bytes and 8
-- height bytes per entry. The kept tables and SQLite's own are left out.
--
chain0LogicalFromWalk :: BL.ByteString -> Either String (M.Map Text (Word64, Word64))
chain0LogicalFromWalk bytes = do
    v <- eitherDecode bytes
    parseEither parseWalk v
  where
    parseWalk :: Value -> Parser (M.Map Text (Word64, Word64))
    parseWalk = withObject "walk" $ \o -> do
        trees <- o .: "trees"
        entries <- forM (AKM.toList trees) $ \(_, t) -> flip (withObject "tree") t $ \to -> do
            ty <- to .: "type"
            table <- to .: "table"
            cells <- to .: "cells"
            kb <- to .: "key_bytes"
            vb <- to .: "val_bytes"
            return (ty :: Text, table, cells :: Word64, kb :: Word64, vb :: Word64)
        return $ M.fromListWith add
            [ (table, entry)
            | (ty, table, cells, kb, vb) <- entries
            , ty == "table"
            , table `notElem` keptTables
            , not ("sqlite_" `T.isPrefixOf` table)
            , let entry
                    | table == transactionIndexTable = (32 * cells, 8 * cells)
                    | otherwise = (kb + 8 * cells, vb)
            ]
    add (a, b) (x, y) = (a + x, b + y)

-- | Spec 11.3: "p50 under 0.2 ms warm, p99 under 5 ms cold, else option S".
--
latencyVerdict :: ColdLatency -> RorS
latencyVerdict l
    | _clMismatches l == 0 && _clWarmP50Us l < 200 && _clColdP99Us l < 5000 = R
    | otherwise = S

-- -------------------------------------------------------------------------- --
-- JSON

jsonOptions :: Int -> Options
jsonOptions n = defaultOptions { fieldLabelModifier = lowerFirst . drop n }
  where
    lowerFirst (x : xs) = toLower x : xs
    lowerFirst [] = []

instance ToJSON TableSizes where
    toJSON = genericToJSON (jsonOptions 3)
instance FromJSON TableSizes where
    parseJSON = genericParseJSON (jsonOptions 3)

instance ToJSON ColdSizes where
    toJSON = genericToJSON (jsonOptions 3)
instance FromJSON ColdSizes where
    parseJSON = genericParseJSON (jsonOptions 3)

instance ToJSON ColdLatency where
    toJSON = genericToJSON (jsonOptions 3)
instance FromJSON ColdLatency where
    parseJSON = genericParseJSON (jsonOptions 3)

instance ToJSON RorS where
    toJSON = genericToJSON defaultOptions
instance FromJSON RorS where
    parseJSON = genericParseJSON defaultOptions

instance ToJSON Projection where
    toJSON = genericToJSON (jsonOptions 5)
instance FromJSON Projection where
    parseJSON = genericParseJSON (jsonOptions 5)

-- -------------------------------------------------------------------------- --
-- Rendering

renderColdSizes :: ColdSizes -> Text
renderColdSizes cs = T.unlines $
    [ "chain " <> _csChain cs <> ": SQLite file " <> gbT (_csSqliteFileBytes cs)
    , "cold store (total-sst-files-size): " <> gbT (_csTotalSst cs)
        <> ", logical " <> gbT (coldLogicalBytes cs)
        <> ", ratio " <> T.pack (printf "%.2f" (coldRatio cs))
    , "hot: kept tables " <> gbT (_csHotKeptBytes cs) <> ", anchors " <> gbT (_csHotAnchorBytes cs)
    , "projected chain: " <> gbT (chainProjectedBytes cs)
    , "keyspaces: " <> T.intercalate ", " [ k <> " " <> gbT v | (k, v) <- M.toList (_csKeyspaceSst cs) ]
    ] <>
    [ "  " <> name <> ": " <> sshowT (_tsVersions s) <> " versions, "
        <> sshowT (_tsDistinctKeys s) <> " keys, logical " <> gbT (tableLogicalBytes s)
        <> ", sst " <> gbT (_tsSstBytes s) <> ", sqlite " <> gbT (_tsSqliteBytes s)
    | (name, s) <- M.toList (_csTables cs)
    ]
  where
    gbT x = T.pack (printf "%.3f GB" (gb x))

renderColdLatency :: ColdLatency -> Text
renderColdLatency l = T.unlines
    [ "reads: " <> sshowT (_clReads l) <> " (" <> sshowT (_clFound l) <> " found), mismatches: " <> sshowT (_clMismatches l)
    , T.pack $ printf "warm: p50 %.1f us, p99 %.1f us" (_clWarmP50Us l) (_clWarmP99Us l)
    , T.pack $ printf "cold: p50 %.1f us, p99 %.1f us, max %.1f us" (_clColdP50Us l) (_clColdP99Us l) (_clColdMaxUs l)
    , "latency verdict (p50 warm < 200 us, p99 cold < 5 ms, no mismatch): " <> sshowT (latencyVerdict l)
    ]

renderProjection :: Projection -> Text
renderProjection p = T.unlines
    [ T.pack $ printf "chain 0: %.1f GB" (_projChain0GB p)
    , T.pack $ printf "chain 19: %.1f GB" (_projChain19GB p)
    , T.pack $ printf "chain 3: %.1f GB (ratio %.2f)" (_projChain3GB p) (_projChain3Ratio p)
    , T.pack $ printf "17 other chains: %.1f GB" (_projOthersGB p)
    , T.pack $ printf "Pact state total: %.1f GB (limit %.1f GB)" (_projPactTotalGB p) (_projLimitGB p)
    , "verdict: " <> sshowT (_projVerdict p)
    ]
