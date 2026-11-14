{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.StorageTools.Test.Migrate
--
-- Tests of @storage-migrate@ and of the P1 write options of the range
-- databases. The fixture is a range database of the test source (see
-- "Chainweb.StorageTools.Test.RangeDb") plus entries in the old-layout tables
-- (@BlockPayload@, @BlockTransactions@, @BlockOutputs@, @TransactionTree@,
-- @OutputTree@), which 'addNewPayload' never writes. Each namespace of the
-- fixture sits in SST files of its own, so the tests see which namespaces a
-- step rewrote.
--
-- The leveled fixtures ('withLeveledFixture') are the database of a node
-- instead: the old-layout tables are empty, and files at several levels span
-- them with keys of their neighbours.
--
module Chainweb.StorageTools.Test.Migrate
( tests
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (poll, wait, withAsync)
import Control.Exception (IOException, SomeException, try)
import Control.Monad

import Data.Bits (complement)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import Data.IORef
import Data.List (isInfixOf, isPrefixOf, sortOn)
import Data.Maybe (isNothing)
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Foreign.C (CInt(..), CUChar(..))

import GHC.Conc (getNumProcessors)
import Foreign.Ptr (Ptr)

import Options.Applicative

import System.Directory
import System.Exit (ExitCode(..))
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import System.LogLevel (LogLevel(..))
import System.Timeout (timeout)

import Test.Tasty
import Test.Tasty.HUnit

-- internal modules

import Chainweb.Chainweb.Configuration (PayloadCompression(..))
import Chainweb.Logger (GenericLogger, genericLogger)
import Chainweb.Payload
import Chainweb.Payload.PayloadStore
import Chainweb.Payload.PayloadStore.RocksDB
import Chainweb.Storage.Table
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning
import Chainweb.StorageTools.Migrate
-- the payload namespaces of storage-migrate, not those of the range databases
import Chainweb.StorageTools.RangeDb hiding (payloadNamespaces)
import Chainweb.StorageTools.Test.RangeDb hiding (tests)

-- -------------------------------------------------------------------------- --
-- Fixture

-- | Every namespace of a range database that is not a tree namespace.
--
otherNamespaces :: [B.ByteString]
otherNamespaces = migratePayloadNamespaces <> headerNamespaces testVer <> metadataNamespaces

-- | Every namespace of a range database.
--
everyNamespace :: [B.ByteString]
everyNamespace = treeNamespaces <> otherNamespaces

-- | A closed range database at @<dir>/db@: the range database of the source,
-- with the old-layout entries of every payload.
--
-- Each namespace sits in SST files of its own: the namespaces are written one
-- after the other, and each is flushed and compacted before the next one is
-- written. A range compaction of one namespace therefore rewrites the files
-- of that namespace only, so the tests see which namespaces a step rewrote.
-- The fixture asserts this.
--
withFixture :: (FilePath -> FilePath -> IO a) -> IO a
withFixture act = withMixedFixture $ \dir mixed -> do
    let db = dir </> "db"
    withReadOnlyRocksDb mixed modernDefaultOptions $ \from ->
        withRocksDb db modernDefaultOptions $ \rdb -> do
            -- the sentinel key of the open goes to a file of its own
            flushRocksDb rdb
            forM_ everyNamespace $ \ns -> do
                es <- readNamespace from ns
                updateBatch [RocksDbInsert (rawTable rdb ns) k v | (k, v) <- es]
                flushRocksDb rdb
                compactRangeForced (rawTable rdb ns) (Nothing, Nothing)
    assertNonEmpty db
    -- no live file holds keys of two namespaces
    files <- withReadOnlyRocksDb db modernDefaultOptions $ \rdb -> do
        fs <- liveFileRanges rdb
        return [(f, filter (\ns -> overlapsNamespace rdb ns f) everyNamespace) | f <- fs]
    forM_ files $ \(f, nss) ->
        assertBool ("fixture: " <> show f <> " overlaps " <> show nss) (length nss <= 1)
    forM_ (treeNamespaces <> migratePayloadNamespaces) $ \ns -> do
        fs <- filesOverlapping db ns
        assertBool ("fixture: no file for " <> B8.unpack ns) (not $ null fs)
    act dir db

-- | A closed range database at @<dir>/mixed@, with the content of the
-- fixture, flushed once: its SST files hold keys of every namespace.
--
withMixedFixture :: (FilePath -> FilePath -> IO a) -> IO a
withMixedFixture act = withSource $ \src -> withSystemTempDirectory "storage-migrate" $ \dir -> do
    let db = dir </> "mixed"
    withRocksDb db modernDefaultOptions $ \rdb -> do
        _ <- buildRangeDbWith silent defaultPayloadDbConfig (_srcApi src) (srcCut src) (RangeSpec maxH) rdb
        addOldLayout rdb
        flushRocksDb rdb
    assertNonEmpty db
    act dir db

-- | A part of a namespace of the fixture: the namespace and some of the four
-- quarters (1 to 4) of its entries in key order.
--
type Part = (B.ByteString, [Int])

-- | A closed database at @<dir>/db@ with the given parts of the fixture's
-- namespaces; a namespace that no part names is empty. The parts of a group
-- are written together and flushed, so they share SST files:
--
-- * a group for level 0 stays in the file of the flush;
-- * a group for level 1 is compacted ('compactRawRangeForced' of its keys),
--   which puts it at level 1;
-- * a group for a deeper level is compacted, and the result is moved to that
--   level (@change_level@). This moves every file of level 1 and needs the
--   levels in between to be empty, so such a group comes first.
--
-- The sentinel key of the open (@.@) is flushed alone and then compacted to
-- level 1, so that level 0 holds the groups for level 0 only.
--
withLeveledFixture :: [(Int, [Part])] -> (FilePath -> FilePath -> IO a) -> IO a
withLeveledFixture groups act = withMixedFixture $ \dir mixed -> do
    let db = dir </> "db"
    withReadOnlyRocksDb mixed modernDefaultOptions $ \from ->
        withRocksDb db modernDefaultOptions $ \rdb -> do
            flushRocksDb rdb
            forM_ groups $ \(level, parts) -> do
                keys <- fmap concat $ forM parts $ \(ns, qs) -> do
                    es <- readNamespace from ns
                    let es' = concat [quarter q es | q <- qs]
                    updateBatch [RocksDbInsert (rawTable rdb ns) k v | (k, v) <- es']
                    return [fst (namespaceBounds rdb ns) <> k | (k, _) <- es']
                assertBool ("fixture: a group without entries: " <> show parts) (not $ null keys)
                flushRocksDb rdb
                let range = (minimum keys, maximum keys)
                if
                    | level == 0 -> return ()
                    | level == 1 -> compactRawRangeForced rdb range
                    | otherwise -> compactRawRangeWith (toLevel level) rdb range
            compactRawRangeForced rdb (".", ".")
    act dir db
  where
    quarter q es = take (hi - lo) (drop lo es)
      where
        n = length es
        lo = (q - 1) * n `div` 4
        hi = q * n `div` 4
    toLevel level copts = do
        c_rocksdb_compactoptions_set_change_level copts 1
        c_rocksdb_compactoptions_set_target_level copts (fromIntegral level)

-- | @void rocksdb_compactoptions_set_change_level(rocksdb_compactoptions_t*,
-- unsigned char);@
foreign import ccall safe "rocksdb_compactoptions_set_change_level"
    c_rocksdb_compactoptions_set_change_level
        :: Ptr CompactOptions -> CUChar -> IO ()

-- | @void rocksdb_compactoptions_set_target_level(rocksdb_compactoptions_t*,
-- int);@
foreign import ccall safe "rocksdb_compactoptions_set_target_level"
    c_rocksdb_compactoptions_set_target_level
        :: Ptr CompactOptions -> CInt -> IO ()

-- | A node's payload tables: the old-layout @BlockPayload@ is empty, and files
-- at levels 3, 1 and 0 span it, each with keys of @BlockOutputs2@ and
-- @BlockPayload2@ (the namespaces before and after it). @BlockPayloadIndex@
-- and @BlockTransactions2@ sit in files of their own.
--
payloadStraddle :: [(Int, [Part])]
payloadStraddle =
    [ (3, [("BlockOutputs2", [1]), ("BlockPayload2", [4])])
    , (1, [("BlockOutputs2", [4]), ("BlockPayload2", [1])])
    , (1, [("BlockPayloadIndex", [1 .. 4])])
    , (1, [("BlockTransactions2", [1 .. 4])])
    , (0, [("BlockOutputs2", [2, 3]), ("BlockPayload2", [2, 3])])
    ]

-- | A node's tree tables: the old-layout @TransactionTree@ is empty, and files
-- at levels 3, 1 and 0 span it, each with keys of @OutputTree2@ and
-- @TransactionTree2@. The file at level 0 also holds @CutHashes@, so it spans
-- the empty @OutputTree@ as well.
--
treeStraddle :: [(Int, [Part])]
treeStraddle =
    [ (3, [("OutputTree2", [1]), ("TransactionTree2", [4])])
    , (1, [("OutputTree2", [4]), ("TransactionTree2", [1])])
    , (0, [("CutHashes", [1 .. 4]), ("OutputTree2", [2, 3]), ("TransactionTree2", [2, 3])])
    ]

-- | The empty namespace has no entries, and the live files that overlap it
-- sit at the given levels and are Snappy.
--
assertSpanned :: FilePath -> B.ByteString -> [Int] -> Assertion
assertSpanned db ns levels = do
    sizes <- namespaceSizes db [ns]
    assertEqual ("fixture: " <> B8.unpack ns <> " is empty") [(ns, 0)] sizes
    fs <- filesOverlapping db ns
    assertEqual ("fixture: levels of the files that span " <> B8.unpack ns <> ": " <> show fs)
        (S.fromList levels) (S.fromList $ _lfLevel <$> fs)
    assertNamespacesSnappy "fixture" db [ns]

-- | Every namespace has entries, so no check of a test is vacuous.
--
assertNonEmpty :: FilePath -> Assertion
assertNonEmpty db = do
    sizes <- namespaceSizes db everyNamespace
    forM_ sizes $ \(ns, n) -> assertBool ("fixture: " <> B8.unpack ns <> " is empty") (n > 0)

-- | Writes each payload of the new-layout tables again in the old layout.
--
addOldLayout :: RocksDb -> IO ()
addOldLayout rdb = do
    payloads <- entriesOf (_newTransactionDbBlockPayloadsTbl tdb)
    forM_ payloads $ \((h, _), p) -> do
        let txsHash = _blockPayloadTransactionsHash p
            outsHash = _blockPayloadOutputsHash p
        casInsert (_oldTransactionDbBlockPayloadsTbl tdb) p
        txs <- required "transactions" =<< tableLookup (_newTransactionDbBlockTransactionsTbl tdb) (h, txsHash)
        casInsert (_oldTransactionDbBlockTransactionsTbl tdb) txs
        outs <- required "outputs" =<< tableLookup (_newBlockOutputsTbl outputsStore) (h, outsHash)
        casInsert (_oldBlockOutputsTbl outputsStore) outs
        txTree <- required "transaction tree" =<< tableLookup (_newTransactionTreeStoreTbl txTrees) (h, txsHash)
        tableInsert (unCasify $ _oldTransactionTreeStoreTbl txTrees) txsHash txTree
        outTree <- required "output tree" =<< tableLookup (_newOutputTreeStoreTbl outTrees) (h, outsHash)
        tableInsert (unCasify $ _oldOutputTreeStoreTbl outTrees) outsHash outTree
  where
    pdb = newPayloadDb rdb
    tdb = _transactionDb pdb
    outputsStore = _payloadCacheBlockOutputs $ _payloadCache pdb
    txTrees = _payloadCacheTransactionTrees $ _payloadCache pdb
    outTrees = _payloadCacheOutputTrees $ _payloadCache pdb
    required what = maybe (assertFailure $ "fixture: no " <> what) return

entriesOf :: RocksDbTable k v -> IO [(k, v)]
entriesOf tbl = withTableIterator tbl $ \it ->
    let go acc = iterEntry it >>= \case
            Nothing -> return $ reverse acc
            Just (Entry k v) -> iterNext it >> go ((k, v) : acc)
    in go []

-- | A copy of a closed database.
--
copyDb :: FilePath -> FilePath -> IO ()
copyDb from to = do
    createDirectory to
    fs <- listDirectory from
    forM_ fs $ \f -> copyFile (from </> f) (to </> f)

namespaceSizes :: FilePath -> [B.ByteString] -> IO [(B.ByteString, Int)]
namespaceSizes db nss = withReadOnlyRocksDb db modernDefaultOptions $ \rdb ->
    forM nss $ \ns -> (,) ns . length <$> readNamespace rdb ns

diffDbs :: FilePath -> FilePath -> [B.ByteString] -> IO DiffSummary
diffDbs a b nss =
    withReadOnlyRocksDb a modernDefaultOptions $ \da ->
    withReadOnlyRocksDb b modernDefaultOptions $ \db ->
        diffRangeDbs da db nss

-- | The compression of each live SST file.
--
compressions :: FilePath -> IO [B.ByteString]
compressions db = fmap fst . M.elems <$> sstCompressionInfo db

assertAllZstd :: String -> FilePath -> Assertion
assertAllZstd msg db = do
    info' <- sstCompressionInfo db
    assertBool (msg <> ": no SST files") (not $ M.null info')
    forM_ (M.toList info') $ \(f, (c, o)) -> do
        assertEqual (msg <> ": compression of " <> f) "ZSTD" c
        assertBool (msg <> ": dictionary of " <> f <> ": " <> show o)
            ("max_dict_bytes=65536;" `B.isInfixOf` o)

-- | Every live SST file is ZSTD with a dictionary, except the files that hold
-- only the sentinel key of an open (@.@), which no step rewrites.
--
assertAllZstdButSentinel :: String -> FilePath -> Assertion
assertAllZstdButSentinel msg db = do
    files <- withReadOnlyRocksDb db modernDefaultOptions liveFileRanges
    info' <- sstCompressionInfo db
    let sentinel f = _lfSmallestKey f == "." && _lfLargestKey f == "."
        checked = filter (not . sentinel) files
    assertBool (msg <> ": no SST files") (not $ null checked)
    forM_ checked $ \f -> case M.lookup (liveFilePath db f) info' of
        Nothing -> assertFailure $ msg <> ": no SST info for " <> show f
        Just (c, o) -> do
            assertEqual (msg <> ": compression of " <> show f) "ZSTD" c
            assertBool (msg <> ": dictionary of " <> show f <> ": " <> show o)
                ("max_dict_bytes=65536;" `B.isInfixOf` o)

-- | The live SST files whose key ranges overlap a namespace.
--
filesOverlapping :: FilePath -> B.ByteString -> IO [LiveFile]
filesOverlapping db ns = withReadOnlyRocksDb db modernDefaultOptions $ \rdb ->
    filter (overlapsNamespace rdb ns) <$> liveFileRanges rdb

-- | The compression and the compression options of each live SST file that
-- overlaps a namespace.
--
namespaceSstInfo :: FilePath -> B.ByteString -> IO [(FilePath, (B.ByteString, B.ByteString))]
namespaceSstInfo db ns = do
    files <- filesOverlapping db ns
    info' <- sstCompressionInfo db
    forM files $ \f -> let p = liveFilePath db f in
        maybe (assertFailure $ "no SST info for " <> show f) (return . (,) p) $ M.lookup p info'

-- | The compression of each live SST file that overlaps a namespace.
--
namespaceCompressions :: FilePath -> B.ByteString -> IO [B.ByteString]
namespaceCompressions db ns = fmap (fst . snd) <$> namespaceSstInfo db ns

-- | Every live SST file of the namespaces is ZSTD with a dictionary, and each
-- namespace has files.
--
assertNamespacesZstd :: String -> FilePath -> [B.ByteString] -> Assertion
assertNamespacesZstd msg db nss = forM_ nss $ \ns -> do
    info' <- namespaceSstInfo db ns
    assertBool (msg <> ": no SST files for " <> B8.unpack ns) (not $ null info')
    forM_ info' $ \(f, (c, o)) -> do
        assertEqual (msg <> ": compression of " <> f <> " (" <> B8.unpack ns <> ")") "ZSTD" c
        assertBool (msg <> ": dictionary of " <> f <> ": " <> show o)
            ("max_dict_bytes=65536;" `B.isInfixOf` o)

-- | Every live SST file of the namespaces is Snappy.
--
assertNamespacesSnappy :: String -> FilePath -> [B.ByteString] -> Assertion
assertNamespacesSnappy msg db nss = forM_ nss $ \ns -> do
    cs <- namespaceCompressions db ns
    assertBool (msg <> ": no SST files for " <> B8.unpack ns) (not $ null cs)
    assertEqual (msg <> ": compressions of " <> B8.unpack ns) ("Snappy" <$ cs) cs

-- | No live file overlaps a tree namespace: the space of the trees was
-- reclaimed, not only their entries hidden by a range tombstone.
--
assertTreeFilesGone :: String -> FilePath -> [B.ByteString] -> Assertion
assertTreeFilesGone msg db nss = forM_ nss $ \ns -> do
    fs <- filesOverlapping db ns
    assertEqual (msg <> ": live files that overlap " <> B8.unpack ns) [] fs

-- | The newest OPTIONS file of a closed database.
--
latestOptionsFile :: FilePath -> IO B.ByteString
latestOptionsFile dir = do
    fs <- filter isOptions <$> listDirectory dir
    case sortOn (read @Int . drop (length prefix)) fs of
        [] -> assertFailure ("no OPTIONS file in " <> dir)
        xs -> B.readFile (dir </> last xs)
  where
    prefix = "OPTIONS-"
    isOptions f = prefix `isPrefixOf` f && all (`elem` ['0' .. '9']) (drop (length prefix) f)

-- | The namespaces in the progress file.
--
doneKeys :: FilePath -> IO (S.Set Text)
doneKeys db = maybe mempty _progressDone <$> readProgress db

-- | Runs an action with a logger at level 'Info' and returns the log lines.
--
capturing :: (GenericLogger -> IO a) -> IO (a, [Text])
capturing act = do
    ref <- newIORef []
    a <- act $ genericLogger Info $ \t -> atomicModifyIORef' ref (\ls -> (t : ls, ()))
    ls <- reverse <$> readIORef ref
    return (a, ls)

-- | A log line names the namespace and contains the text.
--
assertLogged :: [Text] -> B.ByteString -> Text -> Assertion
assertLogged logs ns txt = assertBool ("a log line names " <> B8.unpack ns <> " and says " <> show txt <> ": " <> show logs) $
    not $ null $ loggedFor logs ns txt

-- | The log lines of a namespace (@<namespace>: @, or the error line that
-- names it) that contain the text.
--
loggedFor :: [Text] -> B.ByteString -> Text -> [Text]
loggedFor logs ns txt = filter (\l -> any (`T.isInfixOf` l) names && txt `T.isInfixOf` l) logs
  where
    names = [T.decodeUtf8 ns <> ": ", "namespace " <> T.decodeUtf8 ns <> " "]

keysOf :: MigrateStep -> [B.ByteString] -> S.Set Text
keysOf step = S.fromList . fmap (progressKey step)

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = testGroup "Migrate"
    [ testCase "trees: four tree namespaces empty; diffRangeDbs on payload namespaces: no differences" $
        withFixture $ \dir db -> do
            let pre = dir </> "pre"
            copyDb db pre
            migrateTrees silent db defaultMigrateOpts
            sizes <- namespaceSizes db treeNamespaces
            assertEqual "tree namespaces" [(ns, 0) | ns <- treeNamespaces] sizes
            assertTreeFilesGone "after trees" db treeNamespaces
            -- the seven payload namespaces, and the headers and cuts as well
            d <- diffDbs pre db otherNamespaces
            assertNoDiff "trees changed another namespace" d
            done <- doneKeys db
            assertEqual "progress" (keysOf MigrateTrees treeNamespaces) done

    , testCase "payloads: every SST of the seven payload namespaces ZSTD, the others still Snappy; diffRangeDbs against a pre-migration copy over all seven payload namespaces, old layout included: no differences" $
        withFixture $ \dir db -> do
            let pre = dir </> "pre"
            copyDb db pre
            before <- compressions db
            assertBool ("before: Snappy only: " <> show before) (all (== "Snappy") before && not (null before))
            migratePayloads silent db defaultMigrateOpts
            assertNamespacesZstd "after" db migratePayloadNamespaces
            -- the files of the other namespaces were not rewritten
            assertNamespacesSnappy "after" db (treeNamespaces <> headerNamespaces testVer <> metadataNamespaces)
            d <- diffDbs pre db migratePayloadNamespaces
            assertNoDiff "payloads changed a payload" d
            assertEqual "the seven payload namespaces are compared" 7 (length $ _dsNamespaces d)
            forM_ (_dsNamespaces d) $ \n ->
                assertBool (B8.unpack (_ndNamespace n) <> " is empty") (_ndLeft n > 0)
            -- the trees, headers and cuts as well
            d' <- diffDbs pre db (treeNamespaces <> headerNamespaces testVer <> metadataNamespaces)
            assertNoDiff "payloads changed another namespace" d'
            done <- doneKeys db
            assertEqual "progress" (keysOf MigratePayloads migratePayloadNamespaces) done

    , testCase "stop after namespace 2, rerun: final state equals an uninterrupted run; progress lists all" $
        withFixture $ \dir db -> do
            let other = dir </> "uninterrupted"
                stop2 = defaultMigrateOpts { _moStopAfter = Just 2 }
            copyDb db other

            migrateTrees silent db stop2
            p1 <- doneKeys db
            assertEqual "progress after the stop" (keysOf MigrateTrees $ take 2 treeNamespaces) p1
            sizes <- namespaceSizes db treeNamespaces
            assertEqual "the first two tree namespaces are empty" [0, 0] (snd <$> take 2 sizes)
            assertBool "the other two are not" (all ((> 0) . snd) (drop 2 sizes))
            assertTreeFilesGone "after the stop" db (take 2 treeNamespaces)
            migrateTrees silent db defaultMigrateOpts

            migratePayloads silent db stop2
            p2 <- doneKeys db
            assertEqual "progress after the second stop"
                (keysOf MigrateTrees treeNamespaces <> keysOf MigratePayloads (take 2 migratePayloadNamespaces)) p2
            -- the files of the first two namespaces were rewritten, those of
            -- the other five were not
            forM_ (zip [1 :: Int ..] migratePayloadNamespaces) $ \(i, ns) -> do
                cs <- namespaceCompressions db ns
                assertBool ("files of " <> B8.unpack ns) (not $ null cs)
                let expected = if i <= 2 then "ZSTD" else "Snappy"
                assertEqual ("after the stop, compression of the files of namespace "
                    <> show i <> ", " <> B8.unpack ns) (expected <$ cs) cs
            migratePayloads silent db defaultMigrateOpts

            migrateTrees silent other defaultMigrateOpts
            migratePayloads silent other defaultMigrateOpts

            d <- diffDbs other db everyNamespace
            assertNoDiff "interrupted and uninterrupted runs differ" d
            let allKeys = keysOf MigrateTrees treeNamespaces <> keysOf MigratePayloads migratePayloadNamespaces
            done <- doneKeys db
            assertEqual "progress lists all" allKeys done
            done' <- doneKeys other
            assertEqual "progress of the uninterrupted run" allKeys done'
            assertNamespacesZstd "interrupted" db migratePayloadNamespaces
            assertNamespacesZstd "uninterrupted" other migratePayloadNamespaces

    , testCase "payloads: a namespace whose compaction did not happen fails its check: exit 1, named, not marked done; a rerun completes" $
        withFixture $ \_ db -> do
            let victim = migratePayloadNamespaces !! 2
                skip = defaultMigrateOpts { _moSkipWork = [victim] }
            assertEqual "the victim" "BlockPayload2" victim
            (code, logs) <- capturing $ \l -> runMigrate l db MigratePayloads skip
            assertEqual "exit code" (ExitFailure 1) code
            assertLogged logs victim "not ZSTD"
            -- the check failed after each of the compactions: two more, then
            -- the namespace fails, with the names of the files
            assertEqual "compactions after a failed check" (compactionAttempts - 1)
                (length $ loggedFor logs victim "compacting again")
            assertLogged logs victim ("after " <> T.pack (show compactionAttempts) <> " compactions")
            assertLogged logs victim ".sst"
            assertEqual "no backtrace in the log" [] (filter ("HasCallStack" `T.isInfixOf`) logs)
            r <- try @MigrateException $ migratePayloads silent db skip
            case r of
                Left (NamespaceFailed step ns _) -> assertEqual "the failure" (MigratePayloads, victim) (step, ns)
                Right () -> assertFailure "migratePayloads did not fail"
            done <- doneKeys db
            assertEqual "progress: the namespaces before it, and not it"
                (keysOf MigratePayloads (take 2 migratePayloadNamespaces)) done
            assertNamespacesSnappy "the namespace that failed" db [victim]
            code' <- runMigrate silent db MigratePayloads defaultMigrateOpts
            assertEqual "exit code of the rerun" ExitSuccess code'
            done' <- doneKeys db
            assertEqual "progress after the rerun" (keysOf MigratePayloads migratePayloadNamespaces) done'
            assertNamespacesZstd "after the rerun" db migratePayloadNamespaces

    , testCase "trees: a namespace whose delete and compaction did not happen fails its check: exit 1, named, not marked done; a rerun completes" $
        withFixture $ \_ db -> do
            let victim = treeNamespaces !! 2
                skip = defaultMigrateOpts { _moSkipWork = [victim] }
            assertEqual "the victim" "OutputTree" victim
            (code, logs) <- capturing $ \l -> runMigrate l db MigrateTrees skip
            assertEqual "exit code" (ExitFailure 1) code
            assertLogged logs victim "still has entries"
            done <- doneKeys db
            assertEqual "progress: the namespaces before it, and not it"
                (keysOf MigrateTrees (take 2 treeNamespaces)) done
            sizes <- namespaceSizes db [victim]
            assertBool "the namespace that failed has its entries" (all ((> 0) . snd) sizes)
            code' <- runMigrate silent db MigrateTrees defaultMigrateOpts
            assertEqual "exit code of the rerun" ExitSuccess code'
            done' <- doneKeys db
            assertEqual "progress after the rerun" (keysOf MigrateTrees treeNamespaces) done'
            sizes' <- namespaceSizes db treeNamespaces
            assertEqual "tree namespaces after the rerun" [(ns, 0) | ns <- treeNamespaces] sizes'
            assertTreeFilesGone "after the rerun" db treeNamespaces

    , testCase "payloads: a compaction that fails (a corrupt block) raises rocksdb.background-errors: exit 1, named, not marked done; a rerun after the repair completes" $
        withFixture $ \_ db -> do
            let victim = migratePayloadNamespaces !! 4
            assertEqual "the victim" "BlockTransactions2" victim
            f <- filesOverlapping db victim >>= \case
                f : _ -> return $ liveFilePath db f
                [] -> assertFailure "no file of the victim"
            original <- B.readFile f
            -- the first data block starts at offset 0; its checksum no longer
            -- matches, which the compaction detects when it reads the block
            B.writeFile f $ B.map complement (B.take 16 original) <> B.drop 16 original
            (code, logs) <- capturing $ \l -> runMigrate l db MigratePayloads defaultMigrateOpts
            assertEqual "exit code" (ExitFailure 1) code
            assertLogged logs victim "rocksdb.background-errors went from 0 to 1"
            done <- doneKeys db
            assertEqual "progress: the namespaces before it, and not it"
                (keysOf MigratePayloads (take 4 migratePayloadNamespaces)) done
            B.writeFile f original
            code' <- runMigrate silent db MigratePayloads defaultMigrateOpts
            assertEqual "exit code of the rerun" ExitSuccess code'
            done' <- doneKeys db
            assertEqual "progress after the rerun" (keysOf MigratePayloads migratePayloadNamespaces) done'
            assertNamespacesZstd "after the rerun" db migratePayloadNamespaces

    , testCase "payloads, a node's database: the empty BlockPayload is spanned by files of BlockOutputs2 and BlockPayload2 at levels 0, 1 and 3: the run completes, all seven done, every file ZSTD" $
        withLeveledFixture payloadStraddle $ \dir db -> do
            assertSpanned db "BlockPayload" [0, 1, 3]
            let pre = dir </> "pre"
            copyDb db pre
            (code, logs) <- capturing $ \l -> runMigrate l db MigratePayloads defaultMigrateOpts
            assertEqual ("exit code: " <> show logs) ExitSuccess code
            assertLogged logs "BlockPayload" "compaction 1: 3 live files overlap the namespace; compacting their keys"
            done <- doneKeys db
            assertEqual "progress" (keysOf MigratePayloads migratePayloadNamespaces) done
            assertAllZstdButSentinel "after" db
            d <- diffDbs pre db migratePayloadNamespaces
            assertNoDiff "payloads changed a payload" d

    , testCase "trees, a node's database: the empty TransactionTree and OutputTree are spanned by files of CutHashes, OutputTree2 and TransactionTree2, at levels 0, 1 and 3: the run completes, all four done, no file overlaps a tree namespace" $
        withLeveledFixture treeStraddle $ \dir db -> do
            assertSpanned db "TransactionTree" [0, 1, 3]
            assertSpanned db "OutputTree" [0]
            let pre = dir </> "pre"
            copyDb db pre
            (code, logs) <- capturing $ \l -> runMigrate l db MigrateTrees defaultMigrateOpts
            assertEqual ("exit code: " <> show logs) ExitSuccess code
            done <- doneKeys db
            assertEqual "progress" (keysOf MigrateTrees treeNamespaces) done
            sizes <- namespaceSizes db treeNamespaces
            assertEqual "tree namespaces" [(ns, 0) | ns <- treeNamespaces] sizes
            assertTreeFilesGone "after trees" db treeNamespaces
            d <- diffDbs pre db ["CutHashes"]
            assertNoDiff "trees changed CutHashes" d
            assertBool "CutHashes is compared" (all ((> 0) . _ndLeft) (_dsNamespaces d))

    , testCase "trees: a namespace that is deleted but whose compaction did not happen fails its check (its files are still live): exit 1, named, not marked done; a rerun completes" $
        withFixture $ \_ db -> do
            let victim = treeNamespaces !! 2
                skip = defaultMigrateOpts { _moSkipCompaction = [victim] }
            assertEqual "the victim" "OutputTree" victim
            (code, logs) <- capturing $ \l -> runMigrate l db MigrateTrees skip
            assertEqual "exit code" (ExitFailure 1) code
            assertLogged logs victim "are still live, so the compaction did not rewrite them"
            done <- doneKeys db
            assertEqual "progress: the namespaces before it, and not it"
                (keysOf MigrateTrees (take 2 treeNamespaces)) done
            -- the range tombstone hides the entries, which an iterator check
            -- alone would accept; the files of the namespace are still there
            sizes <- namespaceSizes db [victim]
            assertEqual "the tombstone hides the entries" [(victim, 0)] sizes
            fs <- filesOverlapping db victim
            assertBool "the files of the namespace are still live" (not $ null fs)
            code' <- runMigrate silent db MigrateTrees defaultMigrateOpts
            assertEqual "exit code of the rerun" ExitSuccess code'
            done' <- doneKeys db
            assertEqual "progress after the rerun" (keysOf MigrateTrees treeNamespaces) done'
            assertTreeFilesGone "after the rerun" db treeNamespaces

    , testCase "progress file and no IDENTITY: refused, and the error says to delete the progress file; after that, the step runs" $
        withFixture $ \_ db -> do
            migrateTrees silent db defaultMigrateOpts { _moStopAfter = Just 1 }
            removeFile (db </> "IDENTITY")
            r <- try @IOException $ migrateTrees silent db defaultMigrateOpts
            case r of
                Left e -> assertBool ("the error: " <> show e) $ all (`isInfixOf` show e)
                    ["no IDENTITY file in " <> db, progressFile db, "Delete the progress file to migrate this database from the start"]
                Right () -> assertFailure "a progress file without IDENTITY is not refused"
            code <- runMigrate silent db MigrateTrees defaultMigrateOpts
            assertEqual "exit code" (ExitFailure 1) code
            removeFile (progressFile db)
            migrateTrees silent db defaultMigrateOpts
            done <- doneKeys db
            assertEqual "progress" (keysOf MigrateTrees treeNamespaces) done

    , testCase "rerun after completion changes nothing" $
        withFixture $ \dir db -> do
            migrateTrees silent db defaultMigrateOpts
            migratePayloads silent db defaultMigrateOpts
            let done = dir </> "done"
            copyDb db done
            files0 <- sstCompressionInfo db
            progress0 <- B.readFile (progressFile db)
            migrateTrees silent db defaultMigrateOpts
            migratePayloads silent db defaultMigrateOpts
            files1 <- sstCompressionInfo db
            assertEqual "live SST files" (M.keys files0) (M.keys files1)
            progress1 <- B.readFile (progressFile db)
            assertEqual "progress file" progress0 progress1
            d <- diffDbs done db everyNamespace
            assertNoDiff "rerun changed an entry" d

    , testCase "trees, stop after namespace 1: TransactionTree empty and its files gone; TransactionTree2 and every other namespace unchanged" $
        withFixture $ \dir db -> do
            let pre = dir </> "pre"
            copyDb db pre
            migrateTrees silent db defaultMigrateOpts { _moStopAfter = Just 1 }
            done <- doneKeys db
            assertEqual "progress" (keysOf MigrateTrees ["TransactionTree"]) done
            sizes <- M.fromList <$> namespaceSizes db ["TransactionTree", "TransactionTree2"]
            assertEqual "TransactionTree is empty" (Just 0) (M.lookup "TransactionTree" sizes)
            assertBool "TransactionTree2 has its entries" (maybe False (> 0) $ M.lookup "TransactionTree2" sizes)
            assertTreeFilesGone "after the stop" db ["TransactionTree"]
            d <- diffDbs pre db (drop 1 treeNamespaces <> otherNamespaces)
            assertNoDiff "deleting TransactionTree changed another namespace" d
            assertEqual "TransactionTree2 is compared" ["TransactionTree2"] (take 1 $ _ndNamespace <$> _dsNamespaces d)

    , testCase "progress file: holds the IDENTITY of the database, refused on a database with another IDENTITY; nothing-to-do line names it" $
        withFixture $ \dir db -> do
            migrateTrees silent db defaultMigrateOpts { _moStopAfter = Just 1 }
            ident <- dbIdentity db
            assertBool "IDENTITY is not empty" (not $ T.null ident)
            p <- readProgress db
            assertEqual "progress" (Just $ Progress ident (keysOf MigrateTrees ["TransactionTree"])) p
            -- a database that replaced this one in place, with its progress file
            let other = dir </> "other"
            withRocksDb other modernDefaultOptions $ \_ -> return ()
            otherIdent <- dbIdentity other
            assertBool "the other database has another IDENTITY" (otherIdent /= ident)
            copyFile (progressFile db) (progressFile other)
            progress0 <- B.readFile (progressFile other)
            forM_ [MigrateTrees, MigratePayloads] $ \step -> do
                r <- try @IOException $ migrate silent other step defaultMigrateOpts
                case r of
                    Left e -> assertBool ("the error names both IDENTITYs: " <> show e) $
                        all (`isInfixOf` show e) ["IDENTITY", T.unpack ident, T.unpack otherIdent, progressFile other]
                    Right () -> assertFailure "a progress file of another database is not refused"
            progress1 <- B.readFile (progressFile other)
            assertEqual "the progress file is unchanged" progress0 progress1
            -- the step is done: the log line names the progress file
            migrateTrees silent db defaultMigrateOpts
            (_, logs) <- capturing $ \l -> migrateTrees l db defaultMigrateOpts
            assertBool ("nothing-to-do line: " <> show logs) $ any
                (\l -> T.pack (progressFile db) `T.isInfixOf` l && "delete that file" `T.isInfixOf` l)
                logs

    , testCase "command line: trees takes --payload-compression (default snappy), both take --pause-file; the help names the node's settings" $ do
        let parse args = getParseResult $ execParserPure defaultPrefs migrateParserInfo args
            helpOf args = case execParserPure defaultPrefs migrateParserInfo (args <> ["--help"]) of
                Failure f -> unwords $ words $ fst $ renderFailure f "storage-migrate"
                _ -> ""
            dir = ["--database-directory", "d"]
            expect step o = Just (MigrateCommand step "d" o)
        assertEqual "trees, defaults" (expect MigrateTrees defaultMigrateOpts) (parse $ "trees" : dir)
        assertEqual "not given by default" Nothing (_moPayloadCompression defaultMigrateOpts)
        assertEqual "the default is the node's" PayloadSnappy (treesCompression defaultMigrateOpts)
        assertEqual "trees, snappy given"
            (expect MigrateTrees defaultMigrateOpts { _moPayloadCompression = Just PayloadSnappy })
            (parse $ "trees" : dir <> ["--payload-compression", "snappy"])
        assertEqual "trees, zstd-dict and a pause file"
            (expect MigrateTrees defaultMigrateOpts { _moPayloadCompression = Just PayloadZstdDict, _moPauseFile = Just "p" })
            (parse $ "trees" : dir <> ["--payload-compression", "zstd-dict", "--pause-file", "p"])
        assertEqual "trees rejects zstd" Nothing (parse $ "trees" : dir <> ["--payload-compression", "zstd"])
        assertEqual "payloads, a pause file"
            (expect MigratePayloads defaultMigrateOpts { _moPauseFile = Just "p" })
            (parse $ "payloads" : dir <> ["--pause-file", "p"])
        assertEqual "payloads takes no compression" Nothing (parse $ "payloads" : dir <> ["--payload-compression", "snappy"])
        let top = helpOf []
            trees = helpOf ["trees"]
            payloads = helpOf ["payloads"]
        forM_ [ "--store-merkle-trees false", "--payload-compression zstd-dict", "read-only", "LOCK", "storage-migrate.progress.json"
              , "trees --payload-compression zstd-dict", "SIGTERM or kill is safe at any time"
              , "compaction output is installed atomically", "Ctrl-C waits for the running compaction"
              , "approximate size" ] $ \t ->
            assertBool ("help: " <> show t <> " in " <> show top) (t `isInfixOf` top)
        forM_ ["use the value the node runs with", "or zstd-dict when payloads runs next", "trees refuses to run without this option", "--store-merkle-trees false", "--pause-file"] $ \t ->
            assertBool ("trees help: " <> show t <> " in " <> show trees) (t `isInfixOf` trees)
        forM_ ["--payload-compression zstd-dict", "--pause-file"] $ \t ->
            assertBool ("payloads help: " <> show t <> " in " <> show payloads) (t `isInfixOf` payloads)

    , testCase "a namespace already in the target state is not compacted again; each compaction is logged first, with its approximate size; the steps open the database with max_subcompactions = min 4 cores" $
        withFixture $ \_ db -> do
            forM_ [(MigrateTrees, treeNamespaces), (MigratePayloads, migratePayloadNamespaces)] $ \(step, nss) -> do
                (code, logs) <- capturing $ \l -> runMigrate l db step defaultMigrateOpts
                assertEqual "exit code" ExitSuccess code
                forM_ nss $ \ns -> do
                    let compactions = loggedFor logs ns "approximately"
                    assertEqual ("one compaction of " <> B8.unpack ns <> ": " <> show logs) 1 (length compactions)
                    assertBool ("the size is given: " <> show compactions) $
                        all (\l -> " bytes (" `T.isInfixOf` l && not ("approximately 0 bytes" `T.isInfixOf` l)) compactions
                    assertLogged logs ns "SIGTERM or kill is safe at any time"
                    assertEqual "not in the target state before" [] (loggedFor logs ns "already in the target state")
                -- as after a stop between the compaction of a namespace and
                -- the write of the progress file: all but the first namespace
                -- are done, but not recorded
                done0 <- doneKeys db
                ident <- dbIdentity db
                writeProgress db (Progress ident (done0 `S.difference` keysOf step (drop 1 nss)))
                files0 <- forM nss (filesOverlapping db)
                (code', logs') <- capturing $ \l -> runMigrate l db step defaultMigrateOpts
                assertEqual "exit code of the rerun" ExitSuccess code'
                forM_ (drop 1 nss) $ \ns -> do
                    assertLogged logs' ns "already in the target state"
                    assertEqual ("no compaction of " <> B8.unpack ns) [] (loggedFor logs' ns "approximately")
                files1 <- forM nss (filesOverlapping db)
                assertEqual "the live files of the namespaces are unchanged" files0 files1
                done1 <- doneKeys db
                assertEqual "progress after the rerun" done0 done1
            n <- min 4 <$> getNumProcessors
            opts <- latestOptionsFile db
            assertBool ("max_subcompactions=" <> show n <> " in the OPTIONS file") $
                B8.pack ("max_subcompactions=" <> show n <> "\n") `B.isInfixOf` opts

    , testCase "trees --payload-compression zstd-dict, then payloads: the first compaction of trees rewrites every file with zstd-dict, and payloads compacts no namespace" $
        withMixedFixture $ \_ mixed -> do
            migrateTrees silent mixed defaultMigrateOpts { _moPayloadCompression = Just PayloadZstdDict }
            assertAllZstd "after trees" mixed
            (code, logs) <- capturing $ \l -> runMigrate l mixed MigratePayloads defaultMigrateOpts
            assertEqual "exit code" ExitSuccess code
            forM_ migratePayloadNamespaces $ \ns -> do
                assertLogged logs ns "already in the target state"
                assertEqual ("no compaction of " <> B8.unpack ns) [] (loggedFor logs ns "approximately")
            done <- doneKeys mixed
            assertEqual "progress" (keysOf MigrateTrees treeNamespaces <> keysOf MigratePayloads migratePayloadNamespaces) done

    , testCase "payloads: a namespace whose files are ZSTD without a dictionary fails its check: exit 1, named, not marked done" $
        withFixture $ \_ db -> do
            let victim = migratePayloadNamespaces !! 3
                noDict = payloadZstdTuning
                    { _tuningCompression = Zstd { _zstdLevel = 3, _zstdMaxDictBytes = 0, _zstdMaxTrainBytes = 0 } }
            assertEqual "the victim" "BlockTransactions" victim
            withRocksDbTuned db modernDefaultOptions noDict $ \rdb ->
                compactRawRangeForced rdb (namespaceBounds rdb victim)
            info' <- namespaceSstInfo db victim
            assertBool ("the files of the victim are ZSTD without a dictionary: " <> show info') $
                not (null info') && all (\(_, (c, o)) -> c == "ZSTD" && not ("max_dict_bytes=65536;" `B.isInfixOf` o)) info'
            (code, logs) <- capturing $ \l ->
                runMigrate l db MigratePayloads defaultMigrateOpts { _moSkipWork = [victim] }
            assertEqual "exit code" (ExitFailure 1) code
            assertLogged logs victim "ZSTD without a dictionary"
            assertEqual "not in the target state" [] (loggedFor logs victim "already in the target state")
            done <- doneKeys db
            assertEqual "progress: the namespaces before it, and not it"
                (keysOf MigratePayloads (take 3 migratePayloadNamespaces)) done

    , testCase "payloads: the check of a namespace reads the files that overlap it only: ZSTD files of one namespace pass while the files of another are still Snappy, and that other namespace fails" $
        withFixture $ \_ db -> do
            let done = migratePayloadNamespaces !! 3
                pending = migratePayloadNamespaces !! 0
            withRocksDbTuned db modernDefaultOptions payloadZstdTuning $ \rdb ->
                compactRawRangeForced rdb (namespaceBounds rdb done)
            doneCs <- namespaceCompressions db done
            pendingCs <- namespaceCompressions db pending
            assertBool ("the files of the namespace are ZSTD: " <> show doneCs) $
                not (null doneCs) && all (== "ZSTD") doneCs
            assertBool ("the files of the other namespace are not ZSTD: " <> show pendingCs) $
                not (null pendingCs) && all (== "Snappy") pendingCs
            withReadOnlyRocksDb db modernDefaultOptions $ \rdb -> do
                verifyNamespace rdb db MigratePayloads done []
                r <- try @SomeException (verifyNamespace rdb db MigratePayloads pending [])
                case r of
                    Left e -> assertBool ("the error names the compression: " <> show e) ("Snappy" `isInfixOf` show e)
                    Right () -> assertFailure "the check of a Snappy namespace passed"

    , testCase "trees: live payload files that are zstd-dict and no --payload-compression: refused, naming both choices, nothing changed; given, it warns when the files differ and runs; a payload namespace done and snappy given: warns that zstd-dict is used" $
        withFixture $ \dir db -> do
            let snappyDb = dir </> "snappy"
                partialDb = dir </> "partial"
                victim = migratePayloadNamespaces !! 2
            copyDb db snappyDb
            copyDb db partialDb
            -- the node wrote zstd-dict files of a payload namespace
            withRocksDbTuned db modernDefaultOptions payloadZstdTuning $ \rdb ->
                compactRawRangeForced rdb (namespaceBounds rdb victim)
            assertNamespacesZstd "fixture" db [victim]
            sizes0 <- namespaceSizes db treeNamespaces
            (code, logs) <- capturing $ \l -> runMigrate l db MigrateTrees defaultMigrateOpts
            assertEqual "exit code without the option" (ExitFailure 1) code
            forM_ ["are ZSTD with a dictionary", "--payload-compression zstd-dict", "--payload-compression snappy"] $ \t ->
                assertBool ("the refusal says " <> show t <> ": " <> show logs) (any (t `T.isInfixOf`) logs)
            sizes1 <- namespaceSizes db treeNamespaces
            assertEqual "tree namespaces after the refusal" sizes0 sizes1
            noProgress <- not <$> doesFileExist (progressFile db)
            assertBool "no progress file after the refusal" noProgress
            -- given explicitly: a warning, and the step runs
            (code', logs') <- capturing $ \l ->
                runMigrate l db MigrateTrees defaultMigrateOpts { _moPayloadCompression = Just PayloadSnappy }
            assertEqual "exit code with snappy given" ExitSuccess code'
            assertBool ("warning with snappy given: " <> show logs') $
                any (\l -> "[Warn]" `T.isInfixOf` l && "--payload-compression snappy, but" `T.isInfixOf` l) logs'
            done <- doneKeys db
            assertEqual "progress" (keysOf MigrateTrees treeNamespaces) done
            -- Snappy files and zstd-dict given: a warning; nothing given: none
            (codeZ, logsZ) <- capturing $ \l ->
                runMigrate l snappyDb MigrateTrees defaultMigrateOpts { _moPayloadCompression = Just PayloadZstdDict, _moStopAfter = Just 1 }
            assertEqual "exit code with zstd-dict given" ExitSuccess codeZ
            assertBool ("warning with zstd-dict given: " <> show logsZ) $
                any (\l -> "[Warn]" `T.isInfixOf` l && "--payload-compression zstd-dict, but" `T.isInfixOf` l) logsZ
            (codeN, logsN) <- capturing $ \l -> runMigrate l partialDb MigrateTrees defaultMigrateOpts { _moStopAfter = Just 1 }
            assertEqual "exit code on Snappy files, nothing given" ExitSuccess codeN
            assertEqual "no warning on Snappy files, nothing given" [] (filter ("[Warn]" `T.isInfixOf`) logsN)
            -- a payload namespace is done: zstd-dict, and snappy given is named as ignored
            migratePayloads silent partialDb defaultMigrateOpts { _moStopAfter = Just 1 }
            (codeP, logsP) <- capturing $ \l ->
                runMigrate l partialDb MigrateTrees defaultMigrateOpts { _moPayloadCompression = Just PayloadSnappy }
            assertEqual "exit code, a payload namespace done" ExitSuccess codeP
            assertBool ("warning that snappy is ignored: " <> show logsP) $
                any (\l -> "[Warn]" `T.isInfixOf` l && "--payload-compression snappy is ignored" `T.isInfixOf` l) logsP

    , testCase "trees: the progress file changes between its first read and the lock (a payload namespace done meanwhile): fails, asks for a rerun, starts no namespace; the rerun writes zstd-dict" $
        withFixture $ \dir db -> do
            let pause = dir </> "PAUSE"
            writeFile pause "pause"
            -- the step reads the progress, then waits for the pause file
            -- before it opens the database
            withAsync (capturing $ \l -> runMigrate l db MigrateTrees defaultMigrateOpts { _moPauseFile = Just pause }) $ \a -> do
                threadDelay 1_500_000
                r <- poll a
                assertBool "the run waits" (isNothing r)
                ident <- dbIdentity db
                writeProgress db (Progress ident (keysOf MigratePayloads (take 1 migratePayloadNamespaces)))
                removeFile pause
                finished <- timeout 60_000_000 (wait a)
                case finished of
                    Nothing -> assertFailure "the run does not end"
                    Just (code, logs) -> do
                        assertEqual "exit code" (ExitFailure 1) code
                        assertBool ("the error asks for a rerun: " <> show logs) $
                            any (\l -> "run the step again" `T.isInfixOf` l && "zstd-dict" `T.isInfixOf` l) logs
                        assertEqual "no namespace started" [] (filter (": start" `T.isInfixOf`) logs)
            sizes <- namespaceSizes db treeNamespaces
            assertBool "the tree namespaces have their entries" (all ((> 0) . snd) sizes)
            done <- doneKeys db
            assertEqual "progress unchanged" (keysOf MigratePayloads (take 1 migratePayloadNamespaces)) done
            code' <- runMigrate silent db MigrateTrees defaultMigrateOpts
            assertEqual "exit code of the rerun" ExitSuccess code'
            assertTreeFilesGone "after the rerun" db treeNamespaces

    , testCase "trees --payload-compression: files that trees rewrites get the node's compression (snappy by default, zstd-dict when given)" $
        withMixedFixture $ \dir mixed -> do
            let snappyDb = dir </> "snappy"
            copyDb mixed snappyDb
            migrateTrees silent snappyDb defaultMigrateOpts
            cs <- compressions snappyDb
            assertBool ("default: Snappy only: " <> show cs) (all (== "Snappy") cs && not (null cs))
            migrateTrees silent mixed defaultMigrateOpts { _moPayloadCompression = Just PayloadZstdDict }
            assertAllZstd "zstd-dict" mixed
            d <- diffDbs snappyDb mixed everyNamespace
            assertNoDiff "the compression changed an entry" d

    , testCase "--pause-file: no namespace starts while the file exists; the run waits, and finishes after the file is removed" $
        withFixture $ \dir db -> do
            let pause = dir </> "PAUSE"
            writeFile pause "pause"
            withAsync (migrateTrees silent db defaultMigrateOpts { _moPauseFile = Just pause }) $ \a -> do
                threadDelay 1_500_000
                r <- poll a
                assertBool "the run waits" (isNothing r)
                done <- doneKeys db
                assertEqual "no namespace is done while paused" mempty done
                sizes <- namespaceSizes db treeNamespaces
                assertBool "no tree namespace was deleted while paused" (all ((> 0) . snd) sizes)
                -- the database is not opened while paused: its LOCK is free
                locked <- try @IOException $ withRocksDb db modernDefaultOptions $ \_ -> return ()
                assertEqual "the database is not locked while paused" (Right ()) (either (Left . show) Right locked)
                removeFile pause
                finished <- timeout 60_000_000 (wait a)
                assertEqual "the run finishes after the removal" (Just ()) finished
            done <- doneKeys db
            assertEqual "progress" (keysOf MigrateTrees treeNamespaces) done

    , testCase "range DB built with --store-merkle-trees false --payload-compression zstd-dict: no tree entries, ZSTD files" $
        withSource $ \src -> withSystemTempDirectory "storage-migrate-range" $ \dir -> do
            let parse args = getParseResult $
                    execParserPure defaultPrefs (info rangeDbWriteOptsParser mempty) args
            assertEqual "defaults" (Just defaultRangeDbWriteOpts) (parse [])
            assertEqual "defaults of the node" (RangeDbWriteOpts True PayloadSnappy) defaultRangeDbWriteOpts
            forM_ [["--store-merkle-trees", "no"], ["--payload-compression", "zstd"]] $ \args ->
                assertEqual ("rejects " <> show args) Nothing (parse args)
            o <- maybe (assertFailure "options do not parse") return $
                parse ["--store-merkle-trees", "false", "--payload-compression", "zstd-dict"]
            assertEqual "parsed" (RangeDbWriteOpts False PayloadZstdDict) o
            let db = dir </> "db"
            s <- withRangeDbWriting o db $
                buildRangeDbWith silent (rangeDbPayloadDbConfig o) (_srcApi src) (srcCut src) (RangeSpec maxH)
            assertBool "payloads were written" (_rsPayloads s > 0)
            sizes <- namespaceSizes db treeNamespaces
            assertEqual "tree namespaces" [(ns, 0) | ns <- treeNamespaces] sizes
            -- everything else equals the source, which was written with trees
            d <- withReadOnlyRocksDb db modernDefaultOptions $ \rdb ->
                diffRangeDbs (_srcRocks src) rdb
                    (filter (`notElem` treeNamespaces) $ allNamespaces testVer)
            assertNoDiff "range DB differs from the source" d
            assertAllZstd "range DB" db

    , testCase "refuses a database that is open elsewhere and a directory without a database; changes nothing" $
        withFixture $ \dir db -> do
            sizes0 <- namespaceSizes db treeNamespaces
            withRocksDb db modernDefaultOptions $ \_ -> forM_ [MigrateTrees, MigratePayloads] $ \step -> do
                r <- try @IOException $ migrate silent db step defaultMigrateOpts
                case r of
                    -- the error of RocksDB's lock on the LOCK file of the
                    -- database (in one process: "lock hold by current process")
                    Left e -> assertBool ("the lock error: " <> show e) $
                        (db </> "LOCK: ") `isInfixOf` show e && "lock" `isInfixOf` show e
                    Right () -> assertFailure "a database in use is not refused"
                code <- runMigrate silent db step defaultMigrateOpts
                assertEqual "exit code" (ExitFailure 1) code
            sizes1 <- namespaceSizes db treeNamespaces
            assertEqual "tree namespaces after the refusal" sizes0 sizes1
            noProgress <- not <$> doesFileExist (progressFile db)
            assertBool "no progress file" noProgress
            let notDb = dir </> "not-a-db"
            createDirectory notDb
            r <- try @IOException $ migrateTrees silent notDb defaultMigrateOpts
            case r of
                Left e -> assertBool ("the error: " <> show e) ("not a RocksDB database" `isInfixOf` show e)
                Right () -> assertFailure "a directory without a database is not refused"
            nothing <- null <$> listDirectory notDb
            assertBool "nothing is created" nothing
    ]
