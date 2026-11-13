{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.Storage.Test.Tuning
-- License: MIT
-- Stability: experimental
--
-- Tests for "Chainweb.Storage.Table.RocksDB.Tuning" and the tuned open,
-- compaction, flush and property functions of "Chainweb.Storage.Table.RocksDB".
--
module Chainweb.Storage.Test.Tuning
  ( tuningTests
  ) where

import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning

import Chainweb.Storage.Table (tableInsertBatch)

import Control.Concurrent.Async (mapConcurrently_)
import Control.Exception (Exception, bracket, throwIO)
import Control.Monad (forM_, unless, when)
import Control.Monad.Catch (throwM)

import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as B8
import Data.ByteString.Unsafe qualified as BU
import Data.List qualified as L
import Data.Map.Strict qualified as M
import Data.Maybe (isJust)

import Foreign
import Foreign.C

import GHC.Stack (HasCallStack, callStack, prettyCallStack)

import System.Directory (doesDirectoryExist)
import System.IO.Temp (withSystemTempDirectory)
import Text.Read (readMaybe)

import Database.RocksDB.Internal qualified as R
import Database.RocksDB.Base qualified as R
import Database.RocksDB.C qualified as C

-- -------------------------------------------------------------------------- --
-- Utils

newtype TuningTestFailure = TuningTestFailure String

instance Show TuningTestFailure where
    show (TuningTestFailure msg) = unlines ["RocksDb tuning test failure", msg]

instance Exception TuningTestFailure

assertBool :: HasCallStack => String -> Bool -> IO ()
assertBool msg ok = unless ok $
    throwIO $ TuningTestFailure $ unlines [msg, prettyCallStack callStack]

-- | Runs an action that takes a @char** errptr@ and fails if it set an error.
--
noErr :: HasCallStack => String -> (Ptr CString -> IO a) -> IO a
noErr what act = alloca $ \errPtr -> do
    poke errPtr nullPtr
    r <- act errPtr
    err <- peek errPtr
    unless (err == nullPtr) $ do
        msg <- peekCString err
        assertBool (what <> ": " <> msg) False
    return r

-- -------------------------------------------------------------------------- --
-- Test data

-- | A table with 'Int' keys and raw 'ByteString' values, so that values can
-- be compared byte for byte.
--
bytesTable :: RocksDb -> ByteString -> RocksDbTable Int ByteString
bytesTable db name = newTable db valCodec keyCodec [name]
  where
    valCodec = Codec id return
    keyCodec = Codec
        (B8.pack . show)
        (\b -> maybe (throwM $ TuningTestFailure $ "bad key: " <> show b) return
            $ readMaybe (B8.unpack b))

-- | A deterministic, compressible value of a few hundred bytes.
--
valueFor :: Int -> ByteString
valueFor i = B8.pack $ take (200 + i `mod` 200) $ cycle $
    "payload-" <> show i <> "-" <> show ((i * 7919) `mod` 10007) <> ";"

writeEntries :: RocksDbTable Int ByteString -> [Int] -> IO ()
writeEntries t ks = forM_ (chunksOf 1000 ks) $ \c ->
    tableInsertBatch t [ (k, valueFor k) | k <- c ]

-- | Every key maps to exactly the bytes that 'writeEntries' wrote.
--
checkEntries :: HasCallStack => RocksDbTable Int ByteString -> [Int] -> IO ()
checkEntries t ks = forM_ (chunksOf 1000 ks) $ \c -> do
    vs <- tableLookupBatch t c
    forM_ (zip c vs) $ \(k, v) ->
        assertBool ("value mismatch at key " <> show k <> ": " <> show v)
            (v == Just (valueFor k))

chunksOf :: Int -> [a] -> [[a]]
chunksOf _ [] = []
chunksOf n l = let (a, b) = splitAt n l in a : chunksOf n b

-- | Opens a fresh database directory. The directory does not exist yet when
-- the inner action starts, so that the first open creates it.
--
withTestDir :: String -> (FilePath -> IO a) -> IO a
withTestDir name k = withSystemTempDirectory name $ \tmp -> do
    let dir = tmp <> "/db"
    exists <- doesDirectoryExist dir
    assertBool ("test directory exists: " <> dir) (not exists)
    k dir

-- | Every live SST file of the database uses the expected compression. Returns
-- the compression info for further checks.
--
assertAllCompression
    :: HasCallStack
    => FilePath
    -> ByteString
    -> IO (M.Map FilePath (ByteString, ByteString))
assertAllCompression dir expected = do
    info <- sstCompressionInfo dir
    assertBool "no live SST files" (not (M.null info))
    forM_ (M.toList info) $ \(f, (c, _)) ->
        assertBool (f <> ": expected " <> show expected <> ", got " <> show c)
            (c == expected)
    return info

assertContains :: HasCallStack => String -> ByteString -> ByteString -> IO ()
assertContains ctx needle hay = assertBool
    (ctx <> ": " <> show needle <> " not in " <> show hay)
    (needle `B.isInfixOf` hay)

-- -------------------------------------------------------------------------- --
-- Tests

tuningTests :: IO ()
tuningTests = forM_ tests $ \(name, test) -> do
    putStrLn $ "tuning: " <> name
    test
  where
    tests =
        [ ("tuningLinkTest", tuningLinkTest)
        , ("tuningValuesTest", tuningValuesTest)
        , ("zstdFilesTest", zstdFilesTest)
        , ("defaultIsSnappyTest", defaultIsSnappyTest)
        , ("mixedCompressionReadTest", mixedCompressionReadTest)
        , ("forcedCompactionRewritesTest", forcedCompactionRewritesTest)
        , ("reopenWithCacheAndBloomTest", reopenWithCacheAndBloomTest)
        , ("propertyAndSizeTest", propertyAndSizeTest)
        , ("compactRangeForcedTest", compactRangeForcedTest)
        , ("readOnlyTunedTest", readOnlyTunedTest)
        ]

-- | Every foreign import of "Chainweb.Storage.Table.RocksDB.Tuning" resolves
-- and runs once without error on a fresh object, which is then destroyed.
--
tuningLinkTest :: IO ()
tuningLinkTest = do
    -- options, block-based table options, filter policy, cache
    bracket (R.mkOpts R.defaultOptions) R.freeOpts $ \(R.Options' opts _ _) -> do
        c_rocksdb_options_set_compression opts 7
        c_rocksdb_options_set_compression_options opts (-14) 3 0 65536
        c_rocksdb_options_set_compression_options_zstd_max_train_bytes opts 6553600
        c_rocksdb_options_set_use_direct_reads opts 0
        bracket c_rocksdb_block_based_options_create c_rocksdb_block_based_options_destroy $ \tbl ->
            bracket (c_rocksdb_cache_create_lru (1 `shiftL` 20)) c_rocksdb_cache_destroy $ \cache -> do
                c_rocksdb_block_based_options_set_block_size tbl 32768
                -- owned by the table options from here on
                c_rocksdb_filterpolicy_create_bloom_full 10
                    >>= c_rocksdb_block_based_options_set_filter_policy tbl
                c_rocksdb_block_based_options_set_block_cache tbl cache
                c_rocksdb_options_set_block_based_table_factory opts tbl

    -- compact and flush options
    bracket c_rocksdb_compactoptions_create c_rocksdb_compactoptions_destroy $ \copts ->
        c_rocksdb_compactoptions_set_bottommost_level_compaction copts 2
    bracket c_rocksdb_flushoptions_create c_rocksdb_flushoptions_destroy $ \fopts ->
        c_rocksdb_flushoptions_set_wait fopts 1

    -- calls on an open database
    withTempRocksDb "tuning-link" $ \db -> do
        let R.DB dbPtr = _rocksDbHandle db
        bracket c_rocksdb_flushoptions_create c_rocksdb_flushoptions_destroy $ \fopts -> do
            c_rocksdb_flushoptions_set_wait fopts 1
            noErr "rocksdb_flush" $ c_rocksdb_flush dbPtr fopts
        bracket c_rocksdb_compactoptions_create c_rocksdb_compactoptions_destroy $ \copts ->
            c_rocksdb_compact_range_opt dbPtr copts nullPtr 0 nullPtr 0
        prop <- withCString "rocksdb.total-sst-files-size" $ \name ->
            bracket (c_rocksdb_property_value dbPtr name) C.c_rocksdb_free peekCString
        assertBool ("unexpected property value: " <> prop) (not (null prop))
        BU.unsafeUseAsCStringLen "a" $ \(sPtr, sLen) ->
            BU.unsafeUseAsCStringLen "z" $ \(lPtr, lLen) ->
            with sPtr $ \sPtrs -> with (fromIntegral sLen) $ \sLens ->
            with lPtr $ \lPtrs -> with (fromIntegral lLen) $ \lLens ->
            alloca $ \sizes ->
                noErr "rocksdb_approximate_sizes" $
                    c_rocksdb_approximate_sizes dbPtr 1 sPtrs sLens lPtrs lLens sizes
        bracket (c_rocksdb_livefiles dbPtr) c_rocksdb_livefiles_destroy $ \lf -> do
            n <- c_rocksdb_livefiles_count lf
            forM_ [0 .. n - 1] $ \i -> do
                name <- peekCString =<< c_rocksdb_livefiles_name lf i
                assertBool ("unexpected live file name: " <> name) (".sst" `B8.isSuffixOf` B8.pack name)

-- | The tunings carry exactly the values of the spec (4.2, 4.3) and
-- 'defaultTuning' sets nothing.
--
tuningValuesTest :: IO ()
tuningValuesTest = do
    assertBool "defaultTuning" $ defaultTuning == RocksDbTuning
        { _tuningCompression = KeepBindingCompression
        , _tuningBlockSize = Nothing
        , _tuningBloomBitsPerKey = Nothing
        , _tuningBlockCacheBytes = Nothing
        , _tuningDirectReads = False
        }
    assertBool "payloadZstdTuning" $ payloadZstdTuning == defaultTuning
        { _tuningCompression = Zstd
            { _zstdLevel = 3
            , _zstdMaxDictBytes = 65536
            , _zstdMaxTrainBytes = 6553600
            }
        , _tuningBlockSize = Just 32768
        }
    assertBool "coldStoreTuning" $ coldStoreTuning == payloadZstdTuning
        { _tuningBloomBitsPerKey = Just 10
        , _tuningBlockCacheBytes = Just 1073741824
        }

zstdFilesTest :: IO ()
zstdFilesTest = withTestDir "tuning-zstd" $ \dir -> do
    withRocksDbTuned dir modernDefaultOptions payloadZstdTuning $ \db -> do
        writeEntries (bytesTable db "t") [0 .. 9999]
        flushRocksDb db
    info <- assertAllCompression dir "ZSTD"
    forM_ (M.toList info) $ \(f, (_, o)) -> do
        assertContains f "; level=3;" o
        assertContains f "max_dict_bytes=65536;" o
        assertContains f "zstd_max_train_bytes=6553600;" o

-- | 3.2.1 behaviour: the binding's default options write Snappy SSTs without a
-- dictionary.
--
defaultIsSnappyTest :: IO ()
defaultIsSnappyTest = withTestDir "tuning-snappy" $ \dir -> do
    withRocksDbTuned dir modernDefaultOptions defaultTuning $ \db -> do
        writeEntries (bytesTable db "t") [0 .. 9999]
        flushRocksDb db
    info <- assertAllCompression dir "Snappy"
    forM_ (M.toList info) $ \(f, (_, o)) ->
        assertContains f "max_dict_bytes=0;" o

mixedCompressionReadTest :: IO ()
mixedCompressionReadTest = withTestDir "tuning-mixed" $ \dir -> do
    withRocksDbTuned dir modernDefaultOptions defaultTuning $ \db -> do
        writeEntries (bytesTable db "t") [0 .. 9999]
        flushRocksDb db
    withRocksDbTuned dir modernDefaultOptions payloadZstdTuning $ \db -> do
        writeEntries (bytesTable db "t") [10000 .. 19999]
        flushRocksDb db
    info <- sstCompressionInfo dir
    assertBool ("expected Snappy and ZSTD files: " <> show info) $
        L.sort (L.nub (fst <$> M.elems info)) == ["Snappy", "ZSTD"]
    forM_ [defaultTuning, payloadZstdTuning, coldStoreTuning] $ \tuning ->
        withRocksDbTuned dir modernDefaultOptions tuning $ \db ->
            checkEntries (bytesTable db "t") [0 .. 19999]

-- | The Snappy data sits in the bottommost level, apart from the sentinel key
-- that every open writes, so that no compaction from a level above touches
-- it: only the forced bottommost compaction rewrites it.
--
forcedCompactionRewritesTest :: IO ()
forcedCompactionRewritesTest = withTestDir "tuning-forced" $ \dir -> do
    withRocksDbTuned dir modernDefaultOptions defaultTuning $ \db -> do
        flushRocksDb db
        writeEntries (bytesTable db "t") [0 .. 9999]
        flushRocksDb db
        compactRangeRocksDb (bytesTable db "t") (Nothing, Nothing)
    _ <- assertAllCompression dir "Snappy"
    withRocksDbTuned dir modernDefaultOptions payloadZstdTuning $ \db -> do
        compactAllForced db
        checkEntries (bytesTable db "t") [0 .. 9999]
    _ <- assertAllCompression dir "ZSTD"
    withRocksDbTuned dir modernDefaultOptions defaultTuning $ \db ->
        checkEntries (bytesTable db "t") [0 .. 9999]

-- | 50 open/write/close cycles with a block cache and a bloom filter, with
-- concurrent writers and readers (the test suite runs with @-N@).
--
reopenWithCacheAndBloomTest :: IO ()
reopenWithCacheAndBloomTest = withTestDir "tuning-cold" $ \dir -> do
    forM_ [0 .. 49] $ \i -> do
        let base = i * 200
        withRocksDbTuned dir modernDefaultOptions coldStoreTuning $ \db -> do
            let t = bytesTable db "cold"
            mapConcurrently_
                (\w -> writeEntries t [base + w * 50 .. base + w * 50 + 49])
                [0 .. 3]
            mapConcurrently_ (checkEntries t) (chunksOf 500 [0 .. base + 199])
            -- missing keys go through the bloom filter
            vs <- tableLookupBatch t [base + 200 .. base + 299]
            assertBool "unexpected value for a missing key" (all (== Nothing) vs)
            when (even i) $ flushRocksDb db
    withRocksDbTuned dir modernDefaultOptions defaultTuning $ \db ->
        checkEntries (bytesTable db "cold") [0 .. 9999]

-- | One assertion each for 'flushRocksDb', 'rocksDbProperty' and
-- 'approximateSize' (SST files only).
--
propertyAndSizeTest :: IO ()
propertyAndSizeTest = withTempRocksDbTuned "tuning-props" defaultTuning $ \db -> do
    let t = bytesTable db "size"
    writeEntries t [0 .. 9999]
    before <- approximateSize db "-size$" "-size%"
    assertBool ("approximate size before flush: " <> show before) (before == 0)
    flushRocksDb db
    after <- approximateSize db "-size$" "-size%"
    assertBool ("approximate size after flush: " <> show after) (after > 0)
    total <- rocksDbProperty db "rocksdb.total-sst-files-size"
    assertBool ("total-sst-files-size: " <> show total) $
        maybe False (> (0 :: Integer)) (readMaybe . B8.unpack =<< total)
    unknown <- rocksDbProperty db "rocksdb.no-such-property"
    assertBool ("unknown property: " <> show unknown) (not (isJust unknown))

-- | A forced compaction of one table's range rewrites that table's SSTs and
-- leaves the other SSTs alone.
--
compactRangeForcedTest :: IO ()
compactRangeForcedTest = withTestDir "tuning-range" $ \dir -> do
    -- Three non-overlapping L0 files: the sentinel key that every open writes,
    -- table a, table b.
    withRocksDbTuned dir modernDefaultOptions defaultTuning $ \db -> do
        flushRocksDb db
        writeEntries (bytesTable db "a") [0 .. 9999]
        flushRocksDb db
        writeEntries (bytesTable db "b") [0 .. 9999]
        flushRocksDb db
    before <- assertAllCompression dir "Snappy"
    assertBool ("expected three SST files: " <> show before) (M.size before == 3)
    withRocksDbTuned dir modernDefaultOptions payloadZstdTuning $ \db -> do
        compactRangeForced (bytesTable db "a") (Nothing, Nothing)
        checkEntries (bytesTable db "a") [0 .. 9999]
        checkEntries (bytesTable db "b") [0 .. 9999]
    after <- sstCompressionInfo dir
    let byCompression c = M.keys (M.filter ((== c) . fst) after)
        gone = before `M.withoutKeys` M.keysSet after
        kept = before `M.restrictKeys` M.keysSet after
    -- exactly one file (table a's) is gone, rewritten as ZSTD ...
    assertBool ("table a not rewritten: " <> show after) $
        M.size gone == 1 && not (null (byCompression "ZSTD"))
    -- ... and the sentinel's and table b's files are untouched.
    assertBool ("other SSTs changed: " <> show after) $
        M.size kept == 2 && byCompression "Snappy" == M.keys kept

readOnlyTunedTest :: IO ()
readOnlyTunedTest = withTestDir "tuning-ro" $ \dir -> do
    withRocksDbTuned dir modernDefaultOptions payloadZstdTuning $ \db -> do
        writeEntries (bytesTable db "t") [0 .. 9999]
        flushRocksDb db
        -- left in the WAL; the read-only open replays it
        writeEntries (bytesTable db "t") [10000 .. 10999]
    withReadOnlyRocksDbTuned dir modernDefaultOptions payloadZstdTuning $ \db ->
        checkEntries (bytesTable db "t") [0 .. 10999]
