{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.StorageTools.Measure.Payload
--
-- Measures what the payload, header and metadata tables of a node's database
-- cost on disk under different RocksDB tunings, and how fast a payload is read.
--
module Chainweb.StorageTools.Measure.Payload
( -- * Sizes
  TuningSizes(..)
, NamespaceSizes(..)
, PayloadSizes(..)
, measuredNamespaces
, measurePayloadSizes
, payloadSizesTotal
, renderPayloadSizes

  -- * Reads
, Latencies(..)
, measurePayloadReads
, renderLatencies
) where

import Control.Exception (evaluate)
import Control.Monad

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import Data.Containers.ListUtils (nubOrd)
import Data.IORef
import qualified Data.IntMap.Strict as IM
import Data.List (sort)
import qualified Data.Map.Strict as M
import Data.Maybe
import qualified Data.Text as T
import Data.Word

import GHC.Clock (getMonotonicTimeNSec)

import System.Directory
import System.FilePath ((</>))
import System.Random
import Text.Printf (printf)
import Text.Read (readMaybe)

-- internal modules

import Chainweb.BlockHeight
import Chainweb.Payload
import Chainweb.Payload.PayloadStore
import Chainweb.Payload.PayloadStore.RocksDB
import Chainweb.Storage.Table
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning
import Chainweb.StorageTools.RangeDb
import Chainweb.Utils (sshow)
import Chainweb.Version (ChainwebVersion)

-- -------------------------------------------------------------------------- --
-- Sizes

-- | The size of one namespace on disk, written under one tuning.
--
data TuningSizes = TuningSizes
    { _tsFlushedBytes :: !Word64
        -- ^ SST bytes after the flush of the copy
    , _tsCompactedBytes :: !Word64
        -- ^ SST bytes after 'compactAllForced'
    , _tsCompression :: ![B.ByteString]
        -- ^ the distinct values of the SST table property
        -- @rocksdb.compression@ of the files after the compaction
    }
    deriving (Show, Eq)

data NamespaceSizes = NamespaceSizes
    { _nsName :: !B.ByteString
    , _nsEntries :: !Int
    , _nsLogicalBytes :: !Word64
        -- ^ the sum of the lengths of the keys (without the namespace) and
        -- the values
    , _nsDefault :: !TuningSizes
        -- ^ under 'defaultTuning' (Snappy)
    , _nsZstd :: !TuningSizes
        -- ^ under 'payloadZstdTuning'
    }
    deriving (Show, Eq)

newtype PayloadSizes = PayloadSizes { _psNamespaces :: [NamespaceSizes] }
    deriving (Show, Eq)

-- | The header, payload and metadata namespaces that are measured: the
-- header tables are those of the chains in the version's chain graphs.
--
measuredNamespaces :: ChainwebVersion -> [B.ByteString]
measuredNamespaces v =
    payloadNamespaces <> legacyPayloadNamespaces <> metadataNamespaces <> headerNamespaces v

-- | @measurePayloadSizes ver dbPath workPath@ copies each non-empty header,
-- payload and metadata namespace of the database at @dbPath@ into fresh
-- instances under 'defaultTuning' and under 'payloadZstdTuning', and
-- reports the logical size of the namespace and the size of the SST files
-- after a flush and after 'compactAllForced'.
--
-- The source is opened read-only. RocksDB's read-only open replays the write
-- ahead log of the source into memtables, so data that is not flushed yet is
-- seen. Flushing a database that was just written ('flushRocksDb') is still
-- good practice: the source then has all its data in SST files. The copies are
-- written below @workPath@ one at a time and are removed after they were
-- measured.
--
measurePayloadSizes :: ChainwebVersion -> FilePath -> FilePath -> IO PayloadSizes
measurePayloadSizes ver dbPath workPath = do
    createdWork <- not <$> doesDirectoryExist workPath
    createDirectoryIfMissing True workPath
    r <- withReadOnlyRocksDbTuned dbPath modernDefaultOptions defaultTuning $ \src ->
        fmap catMaybes $ forM (measuredNamespaces ver) $ \ns -> do
            (n, logical) <- logicalSize src ns
            if n == 0
                then return Nothing
                else do
                    d <- copyAndMeasure src ns "default" defaultTuning
                    z <- copyAndMeasure src ns "zstd" payloadZstdTuning
                    return $ Just $ NamespaceSizes ns n logical d z
    when createdWork $ removeDirectory workPath
    return $ PayloadSizes r
  where
    logicalSize src ns = withTableIterator (rawTable src ns) $ \it ->
        let go !n !b = iterEntry it >>= \case
                Nothing -> return (n, b)
                Just (Entry k v) -> do
                    iterNext it
                    go (n + 1 :: Int) (b + fromIntegral (B.length k + B.length v) :: Word64)
        in go 0 0

    copyAndMeasure src ns label tuning = do
        let dir = workPath </> (map sanitize (B8.unpack ns) <> "-" <> label)
        removePathForcibly dir
        (flushed, compacted) <- withRocksDbTuned dir modernDefaultOptions tuning $ \dst -> do
            copyNamespace src dst ns
            flushRocksDb dst
            f <- sstBytes dst
            compactAllForced dst
            c <- sstBytes dst
            return (f, c)
        -- the database is closed now
        info <- sstCompressionInfo dir
        removePathForcibly dir
        return $ TuningSizes flushed compacted $ nubOrd $ fst <$> M.elems info

    sanitize c = if c == '/' then '_' else c

copyNamespace :: RocksDb -> RocksDb -> B.ByteString -> IO ()
copyNamespace src dst ns = withTableIterator srcTbl $ \it ->
    let go acc !n = iterEntry it >>= \case
            Nothing -> flush acc
            Just (Entry k v) -> do
                iterNext it
                if n >= (1_000 :: Int)
                    then flush ((k, v) : acc) >> go [] 0
                    else go ((k, v) : acc) (n + 1)
        flush [] = return ()
        flush acc = tableInsertBatch dstTbl (reverse acc)
    in go [] 0
  where
    srcTbl = rawTable src ns
    dstTbl = rawTable dst ns

sstBytes :: RocksDb -> IO Word64
sstBytes db = rocksDbProperty db "rocksdb.live-sst-files-size" >>= \case
    Nothing -> ioError $ userError "property rocksdb.live-sst-files-size is not available"
    Just p -> maybe (ioError $ userError $ "invalid property value: " <> show p) return
        $ readMaybe $ B8.unpack p

-- | The sum over all namespaces.
--
payloadSizesTotal :: PayloadSizes -> NamespaceSizes
payloadSizesTotal (PayloadSizes ns) = NamespaceSizes "total"
    (sum $ _nsEntries <$> ns)
    (sum $ _nsLogicalBytes <$> ns)
    (total _nsDefault)
    (total _nsZstd)
  where
    total f = TuningSizes
        (sum $ _tsFlushedBytes . f <$> ns)
        (sum $ _tsCompactedBytes . f <$> ns)
        (nubOrd $ concatMap (_tsCompression . f) ns)

-- | A table with one line per namespace. The ratios are the logical size and
-- the Snappy size over the zstd size, after compaction.
--
renderPayloadSizes :: PayloadSizes -> T.Text
renderPayloadSizes ps = T.unlines $ header : (line <$> _psNamespaces ps) <> [line (payloadSizesTotal ps)]
  where
    header = T.pack $ printf "%-28s %10s %14s %14s %14s %14s %14s %8s %8s"
        ("namespace" :: String) ("entries" :: String) ("logical" :: String)
        ("snappy flush" :: String) ("snappy compact" :: String)
        ("zstd flush" :: String) ("zstd compact" :: String)
        ("log/zstd" :: String) ("snp/zstd" :: String)
    line n = T.pack $ printf "%-28s %10d %14d %14d %14d %14d %14d %8.2f %8.2f"
        (B8.unpack $ _nsName n) (_nsEntries n) (_nsLogicalBytes n)
        (_tsFlushedBytes $ _nsDefault n) (_tsCompactedBytes $ _nsDefault n)
        (_tsFlushedBytes $ _nsZstd n) (_tsCompactedBytes $ _nsZstd n)
        (ratio (_nsLogicalBytes n) (_tsCompactedBytes $ _nsZstd n))
        (ratio (_tsCompactedBytes $ _nsDefault n) (_tsCompactedBytes $ _nsZstd n))
    ratio :: Word64 -> Word64 -> Double
    ratio _ 0 = 0
    ratio a b = fromIntegral a / fromIntegral b

-- -------------------------------------------------------------------------- --
-- Reads

-- | Latencies of warm payload reads in nanoseconds.
--
data Latencies = Latencies
    { _latCount :: !Int
    , _latP50Ns :: !Word64
    , _latP95Ns :: !Word64
    , _latP99Ns :: !Word64
    , _latMaxNs :: !Word64
    }
    deriving (Show, Eq)

-- | @measurePayloadReads dbPath tuning n seed@ opens the database read-only
-- with the tuning, draws up to @n@ distinct payloads from the payload index
-- (reservoir sampling with the seed), reads them all once to warm the caches,
-- and then times 'lookupPayloadWithHeight' for each of them.
--
-- The percentiles are nearest-rank. Fails if a drawn payload cannot be read.
--
measurePayloadReads :: FilePath -> RocksDbTuning -> Int -> Word64 -> IO Latencies
measurePayloadReads dbPath tuning n seed =
    withReadOnlyRocksDbTuned dbPath modernDefaultOptions tuning $ \db -> do
        let pdb = newPayloadDb db
        sample <- drawSample pdb
        when (null sample) $ ioError $ userError "no payloads in the payload index"
        let readOne (h, k) = lookupPayloadWithHeight pdb (Just h) k >>= \case
                Nothing -> ioError $ userError $ "payload " <> show k <> " at height " <> show h <> " is missing"
                Just p -> void $ evaluate p
        -- warm
        mapM_ readOne sample
        times <- forM sample $ \x -> do
            t0 <- getMonotonicTimeNSec
            readOne x
            t1 <- getMonotonicTimeNSec
            return (t1 - t0)
        let sorted = sort times
            cnt = length sorted
            pct :: Double -> Word64
            pct p = sorted !! max 0 (min (cnt - 1) (ceiling (p * fromIntegral cnt) - 1))
        return $ Latencies cnt (pct 0.50) (pct 0.95) (pct 0.99) (last sorted)
  where
    drawSample :: PayloadDb RocksDbTable -> IO [(BlockHeight, BlockPayloadHash)]
    drawSample pdb = do
        let tbl = _transactionDbBlockPayloadHeightsTbl $ _transactionDb pdb
        gen <- newIORef (mkStdGen $ fromIntegral seed)
        withTableIterator tbl $ \it -> do
            let go !i !acc = iterEntry it >>= \case
                    Nothing -> return acc
                    Just (Entry k h) -> do
                        iterNext it
                        if i < n
                            then go (i + 1) (IM.insert i (h :: BlockHeight, k) acc)
                            else do
                                j <- atomicModifyIORef' gen (\g -> let (x, g') = uniformR (0, i) g in (g', x))
                                go (i + 1) $ if j < n then IM.insert j (h, k) acc else acc
            IM.elems <$> go 0 IM.empty

renderLatencies :: Latencies -> T.Text
renderLatencies l = T.unlines
    [ "reads: " <> sshow (_latCount l)
    , us "p50" (_latP50Ns l)
    , us "p95" (_latP95Ns l)
    , us "p99" (_latP99Ns l)
    , us "max" (_latMaxNs l)
    ]
  where
    us :: T.Text -> Word64 -> T.Text
    us name x = T.pack $ printf "%s: %.1f us" (T.unpack name) (fromIntegral x / 1000 :: Double)
