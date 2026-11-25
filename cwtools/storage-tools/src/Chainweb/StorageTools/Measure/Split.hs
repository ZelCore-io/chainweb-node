{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.StorageTools.Measure.Split
--
-- Measurements of a split or shadow Pact history store (storage redesign,
-- P3, spec 10.2 and 10.3).
--
-- * 'readFrontierHeights': the verified frontier height of each chain, read
--   from the cold store opened read-only. @cw-compare@ uses it to place
--   sampling boundaries and to split its report below and above the
--   frontier.
--
-- * 'measureSplitSize' (@cw-measure split-size@): per chain, the bytes of
--   each cold keyspace ('approximateSize' over the chain's key range in it),
--   the hot SQLite file's bytes and its live bytes
--   (@(page_count - freelist_count) * page_size@). The RocksDB of this build
--   counts only the data blocks of an SST file that a range covers in part,
--   not its index, filter and properties blocks; those are spread over the
--   keyspaces in proportion to their data bytes (as later RocksDB releases
--   do in @ApproximateOffsetOf@), so that the keyspaces of all chains add up
--   to @rocksdb.total-sst-files-size@. The raw values are kept as
--   @coldKeyspaceDataBytes@.
--
-- * 'txlogDigest' (@cw-measure txlog-digest@): through the 'PactStore' of
--   the mode, a SHA-256 per user table over the rows of every txid in
--   ascending order, the rows of one txid in the order '_psRowsAtTxId'
--   returns them; and one over all tables. Two databases with equal digests
--   answer @txlog@ alike, order included, for every txid (review focus 6).
--
-- = JSON
--
-- @split-size@:
--
-- > { "totalSstBytes": N,
-- >   "chains": { "<chain>": { "coldKeyspaceBytes": { "I", "K", "M", "N", "R", "T", "X" },
-- >                            "coldKeyspaceDataBytes": { ... },
-- >                            "coldBytes", "hotFileBytes", "hotLiveBytes", "hotDiskBytes" } } }
--
-- @hotFileBytes@ is @page_count * page_size@ (the pages of the write-ahead
-- log included), @hotDiskBytes@ the main and @-wal@ files on disk.
--
-- @txlog-digest@:
--
-- > { "chain", "store", "belowHeight", "endTxId", "all": HEX,
-- >   "tables": { "<table>": { "sha256": HEX, "txids", "rows" } } }
--
-- Digests are lower-case hex. Every row is hashed as its txid (8 bytes,
-- big-endian), then the row key and the row data, each prefixed by its
-- length (8 bytes, big-endian). The digest over all tables hashes, per table
-- in name order, the table name (length-prefixed) and that table's digest.
--
module Chainweb.StorageTools.Measure.Split
( -- * Layout
  layoutRoot
, hotFileOf
, readFrontierHeights

  -- * Sizes
, coldKeyspaceTags
, ChainSplitSize(..)
, SplitSizes(..)
, measureSplitSize

  -- * txlog digest
, TableDigest(..)
, TxlogDigest(..)
, txlogDigest
, rowDigestBytes
) where

import Control.Exception (throwIO)
import Control.Monad

import qualified Crypto.Hash as C
import Crypto.Hash.Algorithms (SHA256)
import Data.Aeson
import qualified Data.Aeson.Key as K
import qualified Data.ByteArray as BA
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import Data.Containers.ListUtils (nubOrd)
import Data.List (sort)
import qualified Data.Map.Strict as M
import Data.Maybe (catMaybes)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word

import System.Directory
import System.FilePath ((</>))

import qualified Pact.Types.Persistence as Pact4

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Cold.Key (chainByte)
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Pact.Backend.Store.Layout (coldDirName, splitDirName, sqliteDirName)
import Chainweb.Pact.Backend.Store.Node
import Chainweb.Pact.Backend.Store.SQLite (SqliteFileStats(..), sqliteBlockHashAt, sqliteFileStats, sqliteUserTables, withSqliteReadOnly)
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Pact.Backend.Utils (chainDbFileName)
import Chainweb.Storage.Table.RocksDB
import Chainweb.Utils (toText)
import Chainweb.Version

-- -------------------------------------------------------------------------- --
-- Layout

-- | The layout root (@<db-dir>/0@) of a database directory: the directory
-- itself when it holds @rocksDb@, @sqlite@, @sqlite-split@ or @pactCold@,
-- otherwise @0@ under it.
--
layoutRoot :: FilePath -> IO FilePath
layoutRoot dir = do
    here <- or <$> mapM (doesDirectoryExist . (dir </>)) ["rocksDb", sqliteDirName, splitDirName, coldDirName]
    return $ if here then dir else dir </> "0"

-- | The SQLite file of a chain that a split node writes: in @sqlite-split@
-- once the first trim renamed @sqlite@, in @sqlite@ before. In shadow mode,
-- the split side's file in @sqlite-split@.
--
hotFileOf :: FilePath -> ChainId -> IO (Maybe FilePath)
hotFileOf root cid = do
    let cands = [ root </> d </> chainDbFileName cid | d <- [splitDirName, sqliteDirName] ]
    found <- filterM doesFileExist cands
    return $ case found of
        f : _ -> Just f
        [] -> Nothing

-- | A small block cache for the short reads of the tools.
--
toolCacheBytes :: Int
toolCacheBytes = 8 * 1_048_576

-- | The verified frontier height of each given chain that has one, from the
-- cold store under the layout root of the directory, opened read-only. Empty
-- when there is no cold store (a sqlite-mode database).
--
readFrontierHeights :: FilePath -> [ChainId] -> IO (M.Map ChainId BlockHeight)
readFrontierHeights dir cids = do
    root <- layoutRoot dir
    let cold = root </> coldDirName
    doesDirectoryExist cold >>= \case
        False -> return mempty
        True -> withColdStore cold ColdReadOnly toolCacheBytes $ \cs ->
            M.fromList . catMaybes <$> forM cids (\cid ->
                fmap (\f -> (cid, _frHeight f)) <$> coldVerifiedFrontier cs cid)

-- -------------------------------------------------------------------------- --
-- Sizes

-- | The keyspaces of the cold store (see "Chainweb.Pact.Backend.Store.Cold.Key").
--
coldKeyspaceTags :: [Char]
coldKeyspaceTags = "IKMNRTX"

data ChainSplitSize = ChainSplitSize
    { _cszChain :: !ChainId
    , _cszColdKeyspaceBytes :: !(M.Map Char Word64)
        -- ^ 'approximateSize', with the SST metadata spread over the
        -- keyspaces in proportion to their data bytes
    , _cszColdKeyspaceDataBytes :: !(M.Map Char Word64)
        -- ^ 'approximateSize' as RocksDB returns it
    , _cszHotFileBytes :: !Integer
        -- ^ @page_count * page_size@ of the hot SQLite file, its write-ahead
        -- log included
    , _cszHotDiskBytes :: !Integer
        -- ^ the hot SQLite file and its @-wal@ file on disk
    , _cszHotLiveBytes :: !Integer
        -- ^ @(page_count - freelist_count) * page_size@
    }
    deriving (Show, Eq)

data SplitSizes = SplitSizes
    { _spzTotalSstBytes :: !Word64
        -- ^ @rocksdb.total-sst-files-size@ of the cold store
    , _spzChains :: ![ChainSplitSize]
    }
    deriving (Show, Eq)

instance ToJSON SplitSizes where
    toJSON s = object
        [ "totalSstBytes" .= _spzTotalSstBytes s
        , "chains" .= object
            [ K.fromText (toText (_cszChain c)) .= object
                [ "coldKeyspaceBytes" .= object
                    [ K.fromText (T.singleton k) .= n | (k, n) <- M.toList (_cszColdKeyspaceBytes c) ]
                , "coldKeyspaceDataBytes" .= object
                    [ K.fromText (T.singleton k) .= n | (k, n) <- M.toList (_cszColdKeyspaceDataBytes c) ]
                , "coldBytes" .= sum (M.elems (_cszColdKeyspaceBytes c))
                , "hotFileBytes" .= _cszHotFileBytes c
                , "hotLiveBytes" .= _cszHotLiveBytes c
                , "hotDiskBytes" .= _cszHotDiskBytes c
                ]
            | c <- _spzChains s
            ]
        ]

-- | The sizes of a split or shadow database, per chain. The cold store is
-- opened read-only: 'approximateSize' and the total count SST files only,
-- so flush it before (a node flushes at shutdown only when asked; a write
-- ahead log is replayed in memory only). The hot files are opened
-- read-only. A copy or a snapshot only, never a production node's files.
--
measureSplitSize :: FilePath -> [ChainId] -> IO SplitSizes
measureSplitSize dir cids = do
    root <- layoutRoot dir
    let cold = root </> coldDirName
    doesDirectoryExist cold >>= \ok -> unless ok $ throwIO $ userError $
        "split-size: no cold store " <> cold
    withColdStore cold ColdReadOnly toolCacheBytes $ \cs -> do
        let db = coldStoreRocksDb cs
        total <- rocksDbProperty db "rocksdb.total-sst-files-size" >>= \case
            Just v | Just (n, rest) <- B8.readInteger v, B.null rest -> return (fromIntegral n)
            v -> throwIO $ userError $ "split-size: rocksdb.total-sst-files-size: " <> show v
        -- the data bytes of the whole store: every key is in one keyspace
        dataTotal <- sum <$> forM coldKeyspaceTags (\tag ->
            approximateSize db (B.singleton (tagByte tag)) (B.singleton (tagByte tag + 1)))
        let prorate :: Word64 -> Word64
            prorate n
                | dataTotal == 0 = n
                | otherwise = round (fromIntegral n * fromIntegral total / fromIntegral dataTotal :: Double)
        chains <- forM cids $ \cid -> do
            ks <- forM coldKeyspaceTags $ \tag ->
                (,) tag <$> approximateSize db (rangeStart tag cid) (rangeEnd tag cid)
            hot <- hotFileOf root cid >>= maybe
                (throwIO $ userError $ "split-size: no SQLite file of chain " <> T.unpack (toText cid) <> " under " <> root)
                return
            disk <- sum <$> forM [hot, hot <> "-wal"] (\f ->
                doesFileExist f >>= \e -> if e then getFileSize f else return 0)
            stats <- withSqliteReadOnly hot sqliteFileStats
            return ChainSplitSize
                { _cszChain = cid
                , _cszColdKeyspaceBytes = M.fromList [ (k, prorate n) | (k, n) <- ks ]
                , _cszColdKeyspaceDataBytes = M.fromList ks
                , _cszHotFileBytes = _sfsFileBytes stats
                , _cszHotLiveBytes = _sfsLiveBytes stats
                , _cszHotDiskBytes = disk
                }
        return (SplitSizes total chains)
  where
    tagByte = fromIntegral . fromEnum :: Char -> Word8
    rangeStart tag cid = B.pack [tagByte tag, chainByte cid]
    rangeEnd tag cid
        | chainByte cid == 255 = B.pack [tagByte tag + 1]
        | otherwise = B.pack [tagByte tag, chainByte cid + 1]

-- -------------------------------------------------------------------------- --
-- txlog digest

data TableDigest = TableDigest
    { _tdSha256 :: !Text
    , _tdTxIds :: !Int
        -- ^ txids with at least one row
    , _tdRows :: !Int
    }
    deriving (Show, Eq)

data TxlogDigest = TxlogDigest
    { _txdChain :: !ChainId
    , _txdStore :: !PactHistoryStore
    , _txdBelowHeight :: !(Maybe BlockHeight)
    , _txdEndTxId :: !(Maybe Word64)
        -- ^ with a height: rows with a txid below it were hashed
    , _txdAll :: !Text
    , _txdTables :: !(M.Map Text TableDigest)
    }
    deriving (Show, Eq)

instance ToJSON TxlogDigest where
    toJSON d = object
        [ "chain" .= toText (_txdChain d)
        , "store" .= _txdStore d
        , "belowHeight" .= _txdBelowHeight d
        , "endTxId" .= _txdEndTxId d
        , "all" .= _txdAll d
        , "tables" .= object
            [ K.fromText t .= object
                [ "sha256" .= _tdSha256 td
                , "txids" .= _tdTxIds td
                , "rows" .= _tdRows td
                ]
            | (t, td) <- M.toList (_txdTables d)
            ]
        ]

-- | The bytes a row contributes to its table's digest.
--
rowDigestBytes :: Word64 -> B.ByteString -> B.ByteString -> B.ByteString
rowDigestBytes txid k v = BL.toStrict $ BB.toLazyByteString $
    BB.word64BE txid
    <> BB.word64BE (fromIntegral (B.length k)) <> BB.byteString k
    <> BB.word64BE (fromIntegral (B.length v)) <> BB.byteString v

hexDigest :: C.Digest SHA256 -> Text
hexDigest = T.decodeUtf8 . B16.encode . BA.convert

-- | The txlog digest of a chain through the 'PactStore' of the mode (sqlite
-- or split; shadow reads through the shadow store), opened as the node
-- opens it, the cold store read-only and the freezer off. With a height,
-- only the rows of the blocks below it are hashed. Pact opens the SQLite
-- file read-write: a copy or a snapshot only, never a production node's
-- files.
--
txlogDigest
    :: Logger l
    => l
    -> ChainwebVersion
    -> FilePath
    -> ChainId
    -> PactHistoryStore
    -> Maybe BlockHeight
    -> IO TxlogDigest
txlogDigest logger v dir cid mode below = do
    root <- layoutRoot dir
    hotFileOf root cid >>= \case
        Nothing -> throwIO $ userError $
            "txlog-digest: no SQLite file of chain " <> T.unpack (toText cid) <> " under " <> root
        Just _ -> return ()
    let cfg = defaultPactHistoryConfig
            { _phStore = mode
            , _phFreezerEnabled = False
            , _phColdBlockCacheBytes = toolCacheBytes
            }
    withPactStorageMode ColdReadOnly logger v cfg root False $ \st ->
        withChainStorage logger st cid False $ \cst -> do
            let sql = _cstSql cst
                store = chainStorePactStore sql (_cstStore cst)
            bound <- forM below $ \h -> if h == 0
                then return 0
                else sqliteBlockHashAt sql (h - 1) >>= \case
                    Nothing -> throwIO $ userError $
                        "txlog-digest: no block at height " <> show (h - 1) <> " in BlockHistory"
                    Just hash -> _psGetEndTxId' store "txlog-digest" (h - 1) hash >>= \case
                        Historical (Pact4.TxId t) -> return t
                        NoHistory -> throwIO $ userError $
                            "txlog-digest: no history at height " <> show (h - 1)
            tables <- sort <$> sqliteUserTables sql
            digests <- forM tables $ \t -> (,) t <$> tableDigest store bound t
            let allCtx = foldl' (\ctx (t, (_, dg)) -> C.hashUpdates ctx
                        [ lenPrefixed (T.encodeUtf8 t), BA.convert dg :: B.ByteString ]) C.hashInit
                    digests
            return TxlogDigest
                { _txdChain = cid
                , _txdStore = mode
                , _txdBelowHeight = below
                , _txdEndTxId = bound
                , _txdAll = hexDigest (C.hashFinalize allCtx :: C.Digest SHA256)
                , _txdTables = M.fromList [ (t, fst td) | (t, td) <- digests ]
                }
  where
    lenPrefixed b = BL.toStrict (BB.toLazyByteString (BB.word64BE (fromIntegral (B.length b)))) <> b

    tableDigest :: PactStore -> Maybe Word64 -> Text -> IO (TableDigest, C.Digest SHA256)
    tableDigest store bound t = do
        later <- either (throwIO . userError . (("txlog-digest: txids of " <> T.unpack t <> ": ") <>) . show) return
            =<< _psTxIdsAfter store t (Pact4.TxId 0)
        -- distinct and ascending: a txid listed twice would hash its rows twice
        let txids = [ w | w <- nubOrd (sort (0 : [ x | Pact4.TxId x <- later ])), maybe True (w <) bound ]
        (ctx, nTx, nRows) <- foldM (\(ctx, nt, nr) w -> do
            rows <- either (throwIO . userError . (("txlog-digest: rows of " <> T.unpack t <> ": ") <>) . show) return
                =<< _psRowsAtTxId store t (Pact4.TxId w)
            let ctx' = C.hashUpdates ctx [ rowDigestBytes w k d | (k, d) <- rows ]
            return (ctx', if null rows then nt else nt + 1, nr + length rows)
            ) (C.hashInit, 0, 0) txids
        let dg = C.hashFinalize ctx
        return (TableDigest (hexDigest dg) nTx nRows, dg)
