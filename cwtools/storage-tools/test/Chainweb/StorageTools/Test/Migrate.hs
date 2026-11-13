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
-- @OutputTree@), which 'addNewPayload' never writes.
--
module Chainweb.StorageTools.Test.Migrate
( tests
) where

import Control.Exception (SomeException, try)
import Control.Monad

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import Data.Text (Text)

import Options.Applicative

import System.Directory
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)

import Test.Tasty
import Test.Tasty.HUnit

-- internal modules

import Chainweb.Chainweb.Configuration (PayloadCompression(..))
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
-- with the old-layout entries of every payload, flushed to SST files.
--
withFixture :: (FilePath -> FilePath -> IO a) -> IO a
withFixture act = withSource $ \src -> withSystemTempDirectory "storage-migrate" $ \dir -> do
    let db = dir </> "db"
    withRocksDb db modernDefaultOptions $ \rdb -> do
        _ <- buildRangeDbWith silent defaultPayloadDbConfig (_srcApi src) (srcCut src) (RangeSpec maxH) rdb
        addOldLayout rdb
        flushRocksDb rdb
    -- every namespace has entries, so no check below is vacuous
    sizes <- namespaceSizes db everyNamespace
    forM_ sizes $ \(ns, n) -> assertBool ("fixture: " <> B8.unpack ns <> " is empty") (n > 0)
    act dir db

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
            -- the seven payload namespaces, and the headers and cuts as well
            d <- diffDbs pre db otherNamespaces
            assertNoDiff "trees changed another namespace" d
            Progress done <- readProgress db
            assertEqual "progress" (keysOf MigrateTrees treeNamespaces) done

    , testCase "payloads: every SST ZSTD; diffRangeDbs against a pre-migration copy over all seven payload namespaces, old layout included: no differences" $
        withFixture $ \dir db -> do
            let pre = dir </> "pre"
            copyDb db pre
            before <- compressions db
            assertBool ("before: Snappy only: " <> show before) (all (== "Snappy") before && not (null before))
            migratePayloads silent db defaultMigrateOpts
            assertAllZstd "after" db
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
                stop2 = MigrateOpts (Just 2)
            copyDb db other

            migrateTrees silent db stop2
            Progress p1 <- readProgress db
            assertEqual "progress after the stop" (keysOf MigrateTrees $ take 2 treeNamespaces) p1
            sizes <- namespaceSizes db treeNamespaces
            assertEqual "the first two tree namespaces are empty" [0, 0] (snd <$> take 2 sizes)
            assertBool "the other two are not" (all ((> 0) . snd) (drop 2 sizes))
            migrateTrees silent db defaultMigrateOpts

            migratePayloads silent db stop2
            Progress p2 <- readProgress db
            assertEqual "progress after the second stop"
                (keysOf MigrateTrees treeNamespaces <> keysOf MigratePayloads (take 2 payloadNamespaces)) p2
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
            assertAllZstd "interrupted" db
            assertAllZstd "uninterrupted" other

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
