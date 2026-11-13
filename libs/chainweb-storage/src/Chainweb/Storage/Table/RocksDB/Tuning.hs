{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.Storage.Table.RocksDB.Tuning
-- License: MIT
-- Stability: experimental
--
-- RocksDB tuning (compression, block-based table options, block cache, bloom
-- filter, direct reads) through our own foreign imports of the RocksDB C API
-- that is vendored by @rocksdb-haskell-kadena@ (RocksDB 8.3.2).
--
-- The binding does not export these calls, or exports them with a prototype
-- that does not match the vendored @c.h@ (for instance
-- @rocksdb_approximate_sizes@, which takes a trailing @char** errptr@ in 8.3.2).
-- Every import below was checked against the vendored
-- @rocksdb-8.3.2/include/rocksdb/c.h@.
--
-- All imports are @safe@: compaction, flush, approximate sizes and properties
-- can block on disk, and none of the cheap setters is on a hot path.
--
module Chainweb.Storage.Table.RocksDB.Tuning
  (
  -- * Tuning
    TuningCompression(..)
  , RocksDbTuning(..)
  , defaultTuning
  , payloadZstdTuning
  , coldStoreTuning
  , applyTuning

  -- * SST inspection
  , sstCompressionInfo
  , readSstProperties

  -- * Foreign imports
  -- ** Opaque C types
  , BlockBasedOptions
  , FilterPolicy
  , Cache
  , CompactOptions
  , FlushOptions
  , LiveFiles

  -- ** Options
  , c_rocksdb_options_set_compression
  , c_rocksdb_options_set_compression_options
  , c_rocksdb_options_set_compression_options_zstd_max_train_bytes
  , c_rocksdb_options_set_use_direct_reads
  , c_rocksdb_options_set_block_based_table_factory

  -- ** Block-based table options
  , c_rocksdb_block_based_options_create
  , c_rocksdb_block_based_options_destroy
  , c_rocksdb_block_based_options_set_block_size
  , c_rocksdb_block_based_options_set_filter_policy
  , c_rocksdb_block_based_options_set_block_cache
  , c_rocksdb_filterpolicy_create_bloom_full
  , c_rocksdb_cache_create_lru
  , c_rocksdb_cache_destroy

  -- ** Compaction
  , c_rocksdb_compactoptions_create
  , c_rocksdb_compactoptions_destroy
  , c_rocksdb_compactoptions_set_bottommost_level_compaction
  , c_rocksdb_compact_range_opt

  -- ** Flush
  , c_rocksdb_flushoptions_create
  , c_rocksdb_flushoptions_destroy
  , c_rocksdb_flushoptions_set_wait
  , c_rocksdb_flush

  -- ** Properties, sizes and live files
  , c_rocksdb_property_value
  , c_rocksdb_approximate_sizes
  , c_rocksdb_livefiles
  , c_rocksdb_livefiles_count
  , c_rocksdb_livefiles_name
  , c_rocksdb_livefiles_destroy
  ) where

import Control.Exception (bracket, mask_)
import Control.Monad

import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Word

import Foreign.C
import Foreign.Ptr

import GHC.Generics (Generic)
import qualified GHC.Foreign as GHC
import qualified GHC.IO.Encoding as GHC

import System.FilePath ((</>))
import System.IO

import qualified Database.RocksDB.Base as R
import qualified Database.RocksDB.C as C
import qualified Database.RocksDB.Internal as R

-- -------------------------------------------------------------------------- --
-- Tuning

-- | Compression of newly written SST files.
--
data TuningCompression
    = KeepBindingCompression
        -- ^ Leave the compression that the binding's 'R.Options' set (Snappy
        -- for 'R.defaultOptions'). No call is made.
    | Zstd
        { _zstdLevel :: !Int
            -- ^ zstd compression level
        , _zstdMaxDictBytes :: !Int
            -- ^ @compression_opts.max_dict_bytes@: maximum size of the
            -- per-file dictionary; 0 disables dictionaries
        , _zstdMaxTrainBytes :: !Int
            -- ^ @compression_opts.zstd_max_train_bytes@: sample size used to
            -- train the dictionary; 0 uses the raw samples as dictionary
        }
    deriving (Show, Eq, Generic)

-- | RocksDB settings on top of the binding's 'R.Options'.
--
-- Every field's neutral value ('KeepBindingCompression', 'Nothing', 'False')
-- makes 'applyTuning' leave the respective option untouched. With
-- 'defaultTuning' no call is made at all, so the options are exactly those of
-- chainweb-node 3.2.1.
--
data RocksDbTuning = RocksDbTuning
    { _tuningCompression :: !TuningCompression
    , _tuningBlockSize :: !(Maybe Int)
        -- ^ block-based table @block_size@ in bytes
    , _tuningBloomBitsPerKey :: !(Maybe Double)
        -- ^ full bloom filter with the given bits per key
    , _tuningBlockCacheBytes :: !(Maybe Int)
        -- ^ an LRU block cache of the given capacity, owned by this database
    , _tuningDirectReads :: !Bool
        -- ^ @use_direct_reads@
    }
    deriving (Show, Eq, Generic)

-- | Applies nothing.
--
defaultTuning :: RocksDbTuning
defaultTuning = RocksDbTuning
    { _tuningCompression = KeepBindingCompression
    , _tuningBlockSize = Nothing
    , _tuningBloomBitsPerKey = Nothing
    , _tuningBlockCacheBytes = Nothing
    , _tuningDirectReads = False
    }

-- | zstd level 3 with a 64 KB dictionary trained on 6.4 MB of samples (100x
-- the dictionary), 32 KB blocks (spec 4.2).
--
payloadZstdTuning :: RocksDbTuning
payloadZstdTuning = defaultTuning
    { _tuningCompression = Zstd
        { _zstdLevel = 3
        , _zstdMaxDictBytes = 65536
        , _zstdMaxTrainBytes = 6553600
        }
    , _tuningBlockSize = Just 32768
    }

-- | 'payloadZstdTuning' plus a bloom filter with 10 bits per key and a 1 GB
-- block cache (spec 4.3, 8.2).
--
coldStoreTuning :: RocksDbTuning
coldStoreTuning = payloadZstdTuning
    { _tuningBloomBitsPerKey = Just 10
    , _tuningBlockCacheBytes = Just 1073741824
    }

-- | Applies a tuning to an options object, before the database is opened.
--
-- The block-based table options and the cache wrapper are destroyed once the
-- table factory is set: the factory holds its own copy of the table options,
-- which share the cache and the filter policy. The filter policy is owned by
-- the table options and is never destroyed here.
--
-- Throws an 'IOError' if a value does not fit the C type.
--
applyTuning :: RocksDbTuning -> C.OptionsPtr -> IO ()
applyTuning t opts = do
    applyCompression (_tuningCompression t)
    when needsTableFactory setTableFactory
    when (_tuningDirectReads t) $
        c_rocksdb_options_set_use_direct_reads opts 1
  where
    applyCompression KeepBindingCompression = return ()
    applyCompression (Zstd level dictBytes trainBytes) = do
        level' <- toCLevel level
        dictBytes' <- toCInt "zstd max dict bytes" dictBytes
        trainBytes' <- toCInt "zstd max train bytes" trainBytes
        c_rocksdb_options_set_compression opts zstdCompression
        c_rocksdb_options_set_compression_options opts
            defaultWindowBits level' defaultStrategy dictBytes'
        c_rocksdb_options_set_compression_options_zstd_max_train_bytes opts
            trainBytes'

    needsTableFactory = isJust (_tuningBlockSize t)
        || isJust (_tuningBloomBitsPerKey t)
        || isJust (_tuningBlockCacheBytes t)

    setTableFactory =
        bracket c_rocksdb_block_based_options_create c_rocksdb_block_based_options_destroy $ \tbl ->
        withMaybeCache (_tuningBlockCacheBytes t) $ \mcache -> do
            forM_ (_tuningBlockSize t) $ \n ->
                c_rocksdb_block_based_options_set_block_size tbl =<< toCSize "block size" n
            forM_ (_tuningBloomBitsPerKey t) $ \bits -> do
                unless (bits > 0) $ invalid "bloom bits per key" bits
                -- the table options take ownership of the policy
                mask_ $ c_rocksdb_filterpolicy_create_bloom_full (realToFrac bits)
                    >>= c_rocksdb_block_based_options_set_filter_policy tbl
            forM_ mcache $ c_rocksdb_block_based_options_set_block_cache tbl
            c_rocksdb_options_set_block_based_table_factory opts tbl

    withMaybeCache Nothing k = k Nothing
    withMaybeCache (Just n) k = do
        capacity <- toCSize "block cache bytes" n
        bracket (c_rocksdb_cache_create_lru capacity) c_rocksdb_cache_destroy (k . Just)

-- | @rocksdb_zstd_compression@ in @c.h@
--
zstdCompression :: CInt
zstdCompression = 7

-- | The 8.3.2 defaults of @CompressionOptions::window_bits@ and
-- @CompressionOptions::strategy@, which zstd does not use.
--
defaultWindowBits, defaultStrategy :: CInt
defaultWindowBits = -14
defaultStrategy = 0

toCInt :: String -> Int -> IO CInt
toCInt what n
    | n < 0 || toInteger n > toInteger (maxBound :: CInt) = invalid what n
    | otherwise = return (fromIntegral n)

-- | zstd also has negative (fast) levels.
--
toCLevel :: Int -> IO CInt
toCLevel n
    | toInteger n < toInteger (minBound :: CInt) = invalid "zstd level" n
    | toInteger n > toInteger (maxBound :: CInt) = invalid "zstd level" n
    | otherwise = return (fromIntegral n)

toCSize :: String -> Int -> IO CSize
toCSize what n
    | n <= 0 || toInteger n > toInteger (maxBound :: CSize) = invalid what n
    | otherwise = return (fromIntegral n)

invalid :: Show a => String -> a -> IO b
invalid what v = ioError $ userError $
    "Chainweb.Storage.Table.RocksDB.Tuning: invalid " <> what <> ": " <> show v

-- -------------------------------------------------------------------------- --
-- SST inspection

-- | For each live SST file of the database in the given directory, the values
-- of the table properties @rocksdb.compression@ (for instance @"Snappy"@ or
-- @"ZSTD"@) and @rocksdb.compression_options@. A missing property is returned
-- as the empty string. The keys are the file paths.
--
-- The live files are listed by opening the database read-only, which does not
-- take the lock and does not write. The C API of 8.3.2 has no access to table
-- properties, so they are read from the properties block of each file, which
-- RocksDB stores uncompressed.
--
sstCompressionInfo :: FilePath -> IO (M.Map FilePath (ByteString, ByteString))
sstCompressionInfo dir = do
    names <- liveFiles dir
    fmap M.fromList $ forM names $ \name -> do
        let path = dir </> dropWhile (== '/') name
        props <- readSstProperties path
        let prop k = M.findWithDefault "" k props
        return (path, (prop "rocksdb.compression", prop "rocksdb.compression_options"))

liveFiles :: FilePath -> IO [FilePath]
liveFiles dir =
    bracket (R.mkOpts R.defaultOptions) R.freeOpts $ \(R.Options' opts _ _) ->
    bracket (openReadOnly opts) C.c_rocksdb_close $ \db ->
    bracket (c_rocksdb_livefiles db) c_rocksdb_livefiles_destroy $ \lf -> do
        n <- c_rocksdb_livefiles_count lf
        forM [0 .. n - 1] $ \i ->
            GHC.peekCString GHC.utf8 =<< c_rocksdb_livefiles_name lf i
  where
    openReadOnly opts = GHC.withCString GHC.utf8 dir $ \path ->
        R.throwIfErr "open read-only" $
            C.c_rocksdb_open_for_read_only opts path 0

-- | The table properties of a block-based SST file.
--
readSstProperties :: FilePath -> IO (M.Map ByteString ByteString)
readSstProperties path = withBinaryFile path ReadMode $ \h -> do
    size <- hFileSize h
    when (size < newFooterLength) $ failSst "file too short"
    footer <- readAt h (size - newFooterLength) (fromIntegral newFooterLength)
    let magic = le64 (B.drop (fromIntegral newFooterLength - 8) footer)
    -- format_version >= 1: checksum type (1 byte), handles, padding,
    -- format_version (4 bytes), magic (8 bytes). format_version 0: handles,
    -- padding, magic.
    handles <- if
        | magic == blockBasedTableMagic -> return (B.drop 1 footer)
        | magic == legacyBlockBasedTableMagic ->
            return (B.drop (fromIntegral (newFooterLength - legacyFooterLength)) footer)
        | otherwise -> failSst "not a block-based table"
    (metaOff, metaSize) <- either failSst (return . fst) $ decodeHandle handles
    metaIndex <- readBlock h metaOff metaSize
    propsHandle <- maybe (failSst "no properties block") return $
        lookup "rocksdb.properties" metaIndex
    (propsOff, propsSize) <- either failSst (return . fst) $ decodeHandle propsHandle
    M.fromList <$> readBlock h propsOff propsSize
  where
    failSst :: String -> IO a
    failSst msg = ioError $ userError $
        "Chainweb.Storage.Table.RocksDB.Tuning.sstCompressionInfo: " <> path <> ": " <> msg

    readAt h off len = do
        hSeek h AbsoluteSeek off
        bytes <- B.hGet h len
        when (B.length bytes /= len) $ failSst "short read"
        return bytes

    -- an uncompressed block followed by its 5 byte trailer (compression type,
    -- checksum)
    readBlock h off len = do
        bytes <- readAt h (fromIntegral off) (len + blockTrailerLength)
        unless (B.index bytes len == 0) $ failSst "compressed meta block"
        either failSst return $ decodeBlockEntries (B.take len bytes)

newFooterLength, legacyFooterLength :: Integer
newFooterLength = 53
legacyFooterLength = 48

blockTrailerLength :: Int
blockTrailerLength = 5

blockBasedTableMagic, legacyBlockBasedTableMagic :: Word64
blockBasedTableMagic = 0x88e241b785f4cff7
legacyBlockBasedTableMagic = 0xdb4775248b80fb57

-- | A block handle: offset and size, both varint64.
--
decodeHandle :: ByteString -> Either String ((Int, Int), ByteString)
decodeHandle bytes = do
    (off, r1) <- decodeVarint bytes
    (len, r2) <- decodeVarint r1
    return ((off, len), r2)

-- | The entries of a block: prefix-compressed keys and values, followed by the
-- restart array and the number of restarts (32 bit). The top bit of the
-- number of restarts marks a data block hash index and is never set in meta
-- blocks.
--
decodeBlockEntries :: ByteString -> Either String [(ByteString, ByteString)]
decodeBlockEntries block = do
    when (B.length block < 4) $ Left "block too short"
    let restarts = fromIntegral (le32 (B.drop (B.length block - 4) block) .&. 0x7fffffff)
        entriesEnd = B.length block - 4 - 4 * restarts
    when (restarts < 0 || entriesEnd < 0) $ Left "bad restart array"
    go B.empty (B.take entriesEnd block)
  where
    go !prev bytes
        | B.null bytes = return []
        | otherwise = do
            (shared, r1) <- decodeVarint bytes
            (unshared, r2) <- decodeVarint r1
            (valueLen, r3) <- decodeVarint r2
            when (shared > B.length prev || unshared + valueLen > B.length r3) $
                Left "bad block entry"
            let key = B.take shared prev <> B.take unshared r3
                value = B.take valueLen (B.drop unshared r3)
            ((key, value) :) <$> go key (B.drop (unshared + valueLen) r3)

decodeVarint :: ByteString -> Either String (Int, ByteString)
decodeVarint = go 0 (0 :: Word64)
  where
    go !sh !acc bytes
        | sh > 63 = Left "varint too long"
        | otherwise = case B.uncons bytes of
            Nothing -> Left "truncated varint"
            Just (b, rest)
                | testBit b 7 -> go (sh + 7) (acc .|. (fromIntegral (clearBit b 7) `shiftL` sh)) rest
                | otherwise -> do
                    let v = acc .|. (fromIntegral b `shiftL` sh)
                    when (v > fromIntegral (maxBound :: Int)) $ Left "varint out of range"
                    return (fromIntegral v, rest)

le32 :: ByteString -> Word32
le32 = B.foldr' (\b acc -> (acc `shiftL` 8) .|. fromIntegral b) 0 . B.take 4

le64 :: ByteString -> Word64
le64 = B.foldr' (\b acc -> (acc `shiftL` 8) .|. fromIntegral b) 0 . B.take 8

-- -------------------------------------------------------------------------- --
-- Opaque C types

-- | @rocksdb_block_based_table_options_t@
data BlockBasedOptions

-- | @rocksdb_filterpolicy_t@
data FilterPolicy

-- | @rocksdb_cache_t@
data Cache

-- | @rocksdb_compactoptions_t@
data CompactOptions

-- | @rocksdb_flushoptions_t@
data FlushOptions

-- | @rocksdb_livefiles_t@
data LiveFiles

-- -------------------------------------------------------------------------- --
-- Options

-- | @void rocksdb_options_set_compression(rocksdb_options_t*, int);@
foreign import ccall safe "rocksdb_options_set_compression"
    c_rocksdb_options_set_compression
        :: C.OptionsPtr -> CInt -> IO ()

-- | @void rocksdb_options_set_compression_options(rocksdb_options_t*, int, int,
-- int, int);@ with arguments @w_bits, level, strategy, max_dict_bytes@.
foreign import ccall safe "rocksdb_options_set_compression_options"
    c_rocksdb_options_set_compression_options
        :: C.OptionsPtr -> CInt -> CInt -> CInt -> CInt -> IO ()

-- | @void rocksdb_options_set_compression_options_zstd_max_train_bytes(
-- rocksdb_options_t*, int);@
foreign import ccall safe "rocksdb_options_set_compression_options_zstd_max_train_bytes"
    c_rocksdb_options_set_compression_options_zstd_max_train_bytes
        :: C.OptionsPtr -> CInt -> IO ()

-- | @void rocksdb_options_set_use_direct_reads(rocksdb_options_t*, unsigned
-- char);@
foreign import ccall safe "rocksdb_options_set_use_direct_reads"
    c_rocksdb_options_set_use_direct_reads
        :: C.OptionsPtr -> CUChar -> IO ()

-- | @void rocksdb_options_set_block_based_table_factory(rocksdb_options_t* opt,
-- rocksdb_block_based_table_options_t* table_options);@
--
-- The factory copies the table options, including their shared references to
-- the block cache and the filter policy. The table options can be destroyed
-- afterwards.
foreign import ccall safe "rocksdb_options_set_block_based_table_factory"
    c_rocksdb_options_set_block_based_table_factory
        :: C.OptionsPtr -> Ptr BlockBasedOptions -> IO ()

-- -------------------------------------------------------------------------- --
-- Block-based table options

-- | @rocksdb_block_based_table_options_t*
-- rocksdb_block_based_options_create(void);@
foreign import ccall safe "rocksdb_block_based_options_create"
    c_rocksdb_block_based_options_create
        :: IO (Ptr BlockBasedOptions)

-- | @void rocksdb_block_based_options_destroy(
-- rocksdb_block_based_table_options_t* options);@
foreign import ccall safe "rocksdb_block_based_options_destroy"
    c_rocksdb_block_based_options_destroy
        :: Ptr BlockBasedOptions -> IO ()

-- | @void rocksdb_block_based_options_set_block_size(
-- rocksdb_block_based_table_options_t* options, size_t block_size);@
foreign import ccall safe "rocksdb_block_based_options_set_block_size"
    c_rocksdb_block_based_options_set_block_size
        :: Ptr BlockBasedOptions -> CSize -> IO ()

-- | @void rocksdb_block_based_options_set_filter_policy(
-- rocksdb_block_based_table_options_t* options, rocksdb_filterpolicy_t*
-- filter_policy);@
--
-- Takes ownership of the filter policy: the table options free it. Never
-- destroy a policy that was passed here.
foreign import ccall safe "rocksdb_block_based_options_set_filter_policy"
    c_rocksdb_block_based_options_set_filter_policy
        :: Ptr BlockBasedOptions -> Ptr FilterPolicy -> IO ()

-- | @void rocksdb_block_based_options_set_block_cache(
-- rocksdb_block_based_table_options_t* options, rocksdb_cache_t*
-- block_cache);@
--
-- Copies the shared reference to the cache: the cache wrapper can be
-- destroyed afterwards.
foreign import ccall safe "rocksdb_block_based_options_set_block_cache"
    c_rocksdb_block_based_options_set_block_cache
        :: Ptr BlockBasedOptions -> Ptr Cache -> IO ()

-- | @rocksdb_filterpolicy_t* rocksdb_filterpolicy_create_bloom_full(double
-- bits_per_key);@
foreign import ccall safe "rocksdb_filterpolicy_create_bloom_full"
    c_rocksdb_filterpolicy_create_bloom_full
        :: CDouble -> IO (Ptr FilterPolicy)

-- | @rocksdb_cache_t* rocksdb_cache_create_lru(size_t capacity);@
foreign import ccall safe "rocksdb_cache_create_lru"
    c_rocksdb_cache_create_lru
        :: CSize -> IO (Ptr Cache)

-- | @void rocksdb_cache_destroy(rocksdb_cache_t* cache);@
foreign import ccall safe "rocksdb_cache_destroy"
    c_rocksdb_cache_destroy
        :: Ptr Cache -> IO ()

-- -------------------------------------------------------------------------- --
-- Compaction

-- | @rocksdb_compactoptions_t* rocksdb_compactoptions_create(void);@
foreign import ccall safe "rocksdb_compactoptions_create"
    c_rocksdb_compactoptions_create
        :: IO (Ptr CompactOptions)

-- | @void rocksdb_compactoptions_destroy(rocksdb_compactoptions_t*);@
foreign import ccall safe "rocksdb_compactoptions_destroy"
    c_rocksdb_compactoptions_destroy
        :: Ptr CompactOptions -> IO ()

-- | @void rocksdb_compactoptions_set_bottommost_level_compaction(
-- rocksdb_compactoptions_t*, unsigned char);@
--
-- The value is a @BottommostLevelCompaction@: @kSkip@ = 0,
-- @kIfHaveCompactionFilter@ = 1, @kForce@ = 2, @kForceOptimized@ = 3.
foreign import ccall safe "rocksdb_compactoptions_set_bottommost_level_compaction"
    c_rocksdb_compactoptions_set_bottommost_level_compaction
        :: Ptr CompactOptions -> CUChar -> IO ()

-- | @void rocksdb_compact_range_opt(rocksdb_t* db, rocksdb_compactoptions_t*
-- opt, const char* start_key, size_t start_key_len, const char* limit_key,
-- size_t limit_key_len);@
--
-- A null key means an open end of the range.
foreign import ccall safe "rocksdb_compact_range_opt"
    c_rocksdb_compact_range_opt
        :: C.RocksDBPtr
        -> Ptr CompactOptions
        -> CString -> CSize
        -> CString -> CSize
        -> IO ()

-- -------------------------------------------------------------------------- --
-- Flush

-- | @rocksdb_flushoptions_t* rocksdb_flushoptions_create(void);@
foreign import ccall safe "rocksdb_flushoptions_create"
    c_rocksdb_flushoptions_create
        :: IO (Ptr FlushOptions)

-- | @void rocksdb_flushoptions_destroy(rocksdb_flushoptions_t*);@
foreign import ccall safe "rocksdb_flushoptions_destroy"
    c_rocksdb_flushoptions_destroy
        :: Ptr FlushOptions -> IO ()

-- | @void rocksdb_flushoptions_set_wait(rocksdb_flushoptions_t*, unsigned
-- char);@
foreign import ccall safe "rocksdb_flushoptions_set_wait"
    c_rocksdb_flushoptions_set_wait
        :: Ptr FlushOptions -> CUChar -> IO ()

-- | @void rocksdb_flush(rocksdb_t* db, const rocksdb_flushoptions_t* options,
-- char** errptr);@
foreign import ccall safe "rocksdb_flush"
    c_rocksdb_flush
        :: C.RocksDBPtr -> Ptr FlushOptions -> Ptr CString -> IO ()

-- -------------------------------------------------------------------------- --
-- Properties, sizes and live files

-- | @char* rocksdb_property_value(rocksdb_t* db, const char* propname);@
--
-- Returns NULL for an unknown property. A non-NULL result is allocated with
-- @strdup@ and must be freed with @rocksdb_free@.
foreign import ccall safe "rocksdb_property_value"
    c_rocksdb_property_value
        :: C.RocksDBPtr -> CString -> IO CString

-- | @void rocksdb_approximate_sizes(rocksdb_t* db, int num_ranges, const char*
-- const* range_start_key, const size_t* range_start_key_len, const char*
-- const* range_limit_key, const size_t* range_limit_key_len, uint64_t* sizes,
-- char** errptr);@
--
-- In 8.3.2 this covers SST files only: the C API has no variant that takes
-- @SizeApproximationFlags@, so memtables are never included.
foreign import ccall safe "rocksdb_approximate_sizes"
    c_rocksdb_approximate_sizes
        :: C.RocksDBPtr
        -> CInt
        -> Ptr CString -> Ptr CSize
        -> Ptr CString -> Ptr CSize
        -> Ptr Word64
        -> Ptr CString
        -> IO ()

-- | @const rocksdb_livefiles_t* rocksdb_livefiles(rocksdb_t* db);@
foreign import ccall safe "rocksdb_livefiles"
    c_rocksdb_livefiles
        :: C.RocksDBPtr -> IO (Ptr LiveFiles)

-- | @int rocksdb_livefiles_count(const rocksdb_livefiles_t*);@
foreign import ccall safe "rocksdb_livefiles_count"
    c_rocksdb_livefiles_count
        :: Ptr LiveFiles -> IO CInt

-- | @const char* rocksdb_livefiles_name(const rocksdb_livefiles_t*, int
-- index);@
--
-- The name is relative to the database directory and starts with @/@. It is
-- owned by the live files object.
foreign import ccall safe "rocksdb_livefiles_name"
    c_rocksdb_livefiles_name
        :: Ptr LiveFiles -> CInt -> IO CString

-- | @void rocksdb_livefiles_destroy(const rocksdb_livefiles_t*);@
foreign import ccall safe "rocksdb_livefiles_destroy"
    c_rocksdb_livefiles_destroy
        :: Ptr LiveFiles -> IO ()
