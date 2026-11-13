{-# LANGUAGE LambdaCase #-}
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
module Chainweb.StorageTools.Test.Migrate
( tests
) where

import Control.Exception (SomeException, try)
import Control.Monad

import Data.Bits (complement)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import Data.IORef
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Options.Applicative

import System.Directory
import System.Exit (ExitCode(..))
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import System.LogLevel (LogLevel(..))

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
otherNamespaces = payloadNamespaces <> headerNamespaces testVer <> metadataNamespaces

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
    forM_ (treeNamespaces <> payloadNamespaces) $ \ns -> do
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

-- | The namespaces in the progress file.
--
doneKeys :: FilePath -> IO (S.Set Text)
doneKeys db = _progressDone <$> readProgress db

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
    any (\l -> T.decodeUtf8 ns `T.isInfixOf` l && txt `T.isInfixOf` l) logs

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
            Progress done <- readProgress db
            assertEqual "progress" (keysOf MigrateTrees treeNamespaces) done

    , testCase "payloads: every SST of the seven payload namespaces ZSTD, the others still Snappy; diffRangeDbs against a pre-migration copy over all seven payload namespaces, old layout included: no differences" $
        withFixture $ \dir db -> do
            let pre = dir </> "pre"
            copyDb db pre
            before <- compressions db
            assertBool ("before: Snappy only: " <> show before) (all (== "Snappy") before && not (null before))
            migratePayloads silent db defaultMigrateOpts
            assertNamespacesZstd "after" db payloadNamespaces
            -- the files of the other namespaces were not rewritten
            assertNamespacesSnappy "after" db (treeNamespaces <> headerNamespaces testVer <> metadataNamespaces)
            d <- diffDbs pre db payloadNamespaces
            assertNoDiff "payloads changed a payload" d
            assertEqual "the seven payload namespaces are compared" 7 (length $ _dsNamespaces d)
            forM_ (_dsNamespaces d) $ \n ->
                assertBool (B8.unpack (_ndNamespace n) <> " is empty") (_ndLeft n > 0)
            -- the trees, headers and cuts as well
            d' <- diffDbs pre db (treeNamespaces <> headerNamespaces testVer <> metadataNamespaces)
            assertNoDiff "payloads changed another namespace" d'
            Progress done <- readProgress db
            assertEqual "progress" (keysOf MigratePayloads payloadNamespaces) done

    , testCase "stop after namespace 2, rerun: final state equals an uninterrupted run; progress lists all" $
        withFixture $ \dir db -> do
            let other = dir </> "uninterrupted"
                stop2 = defaultMigrateOpts { _moStopAfter = Just 2 }
            copyDb db other

            migrateTrees silent db stop2
            Progress p1 <- readProgress db
            assertEqual "progress after the stop" (keysOf MigrateTrees $ take 2 treeNamespaces) p1
            sizes <- namespaceSizes db treeNamespaces
            assertEqual "the first two tree namespaces are empty" [0, 0] (snd <$> take 2 sizes)
            assertBool "the other two are not" (all ((> 0) . snd) (drop 2 sizes))
            assertTreeFilesGone "after the stop" db (take 2 treeNamespaces)
            migrateTrees silent db defaultMigrateOpts

            migratePayloads silent db stop2
            Progress p2 <- readProgress db
            assertEqual "progress after the second stop"
                (keysOf MigrateTrees treeNamespaces <> keysOf MigratePayloads (take 2 payloadNamespaces)) p2
            -- the files of the first two namespaces were rewritten, those of
            -- the other five were not
            forM_ (zip [1 :: Int ..] payloadNamespaces) $ \(i, ns) -> do
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
            let allKeys = keysOf MigrateTrees treeNamespaces <> keysOf MigratePayloads payloadNamespaces
            Progress done <- readProgress db
            assertEqual "progress lists all" allKeys done
            Progress done' <- readProgress other
            assertEqual "progress of the uninterrupted run" allKeys done'
            assertNamespacesZstd "interrupted" db payloadNamespaces
            assertNamespacesZstd "uninterrupted" other payloadNamespaces

    , testCase "payloads: a namespace whose compaction did not happen fails its check: exit 1, named, not marked done; a rerun completes" $
        withFixture $ \_ db -> do
            let victim = payloadNamespaces !! 2
                skip = defaultMigrateOpts { _moSkipWork = [victim] }
            assertEqual "the victim" "BlockPayload2" victim
            (code, logs) <- capturing $ \l -> runMigrate l db MigratePayloads skip
            assertEqual "exit code" (ExitFailure 1) code
            assertLogged logs victim "not ZSTD"
            r <- try @MigrateException $ migratePayloads silent db skip
            case r of
                Left (NamespaceFailed step ns _) -> assertEqual "the failure" (MigratePayloads, victim) (step, ns)
                Right () -> assertFailure "migratePayloads did not fail"
            done <- doneKeys db
            assertEqual "progress: the namespaces before it, and not it"
                (keysOf MigratePayloads (take 2 payloadNamespaces)) done
            assertNamespacesSnappy "the namespace that failed" db [victim]
            code' <- runMigrate silent db MigratePayloads defaultMigrateOpts
            assertEqual "exit code of the rerun" ExitSuccess code'
            done' <- doneKeys db
            assertEqual "progress after the rerun" (keysOf MigratePayloads payloadNamespaces) done'
            assertNamespacesZstd "after the rerun" db payloadNamespaces

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
            let victim = payloadNamespaces !! 4
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
                (keysOf MigratePayloads (take 4 payloadNamespaces)) done
            B.writeFile f original
            code' <- runMigrate silent db MigratePayloads defaultMigrateOpts
            assertEqual "exit code of the rerun" ExitSuccess code'
            done' <- doneKeys db
            assertEqual "progress after the rerun" (keysOf MigratePayloads payloadNamespaces) done'
            assertNamespacesZstd "after the rerun" db payloadNamespaces

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
            withRocksDb db modernDefaultOptions $ \_ -> forM_ [migrateTrees, migratePayloads] $ \m -> do
                r <- try @SomeException $ m silent db defaultMigrateOpts
                assertBool "a database in use is refused" (either (const True) (const False) r)
            sizes1 <- namespaceSizes db treeNamespaces
            assertEqual "tree namespaces after the refusal" sizes0 sizes1
            noProgress <- not <$> doesFileExist (progressFile db)
            assertBool "no progress file" noProgress
            let notDb = dir </> "not-a-db"
            createDirectory notDb
            r <- try @SomeException $ migrateTrees silent notDb defaultMigrateOpts
            assertBool "a directory without a database is refused" (either (const True) (const False) r)
            nothing <- null <$> listDirectory notDb
            assertBool "nothing is created" nothing
    ]
