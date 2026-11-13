{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Test.Pact.Store.SQLite
--
-- Unit tests of the SQLite 'PactStore'.
--
module Chainweb.Test.Pact.Store.SQLite (tests) where

import Control.Exception (ErrorCall(..), IOException, SomeException, throwIO, try)
import Control.Lens (view)
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Short as SB
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import qualified Data.HashSet as HashSet
import Data.List (intersperse, isPrefixOf, sort)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import qualified Database.SQLite3.Direct as SQ3
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource (runResourceT)
import Test.Tasty
import Test.Tasty.HUnit

import qualified Pact.Core.Names as P5
import qualified Pact.Core.Persistence as P5
import qualified Pact.Interpreter as P4 (PactDbEnv(..))
import qualified Pact.Types.Runtime as P4
import qualified Pact.Types.SQLite as P4SQL

import Chainweb.BlockHash
import Chainweb.BlockHeader.Internal
import Chainweb.BlockHeight
import Chainweb.Logger (GenericLogger)
import Chainweb.Pact.Types (BlockTxHistory(..))
import Chainweb.Pact.Backend.Store.SQLite
import Chainweb.Pact.Backend.Store.Types
import qualified Chainweb.Pact.Backend.InMemDb as InMemDb
import Chainweb.Pact.Backend.Types
    ( BlockHandle(..), SQLiteEnv, SQLitePendingData(..), emptySQLitePendingData )
import Chainweb.Pact.Backend.Utils
    ( chainwebPragmas, withSQLiteConnection, withTempSQLiteConnection )
import qualified Chainweb.Pact.PactService.Checkpointer.Internal as Cp
import qualified Chainweb.Pact4.Backend.ChainwebPactDb as Pact4
import qualified Chainweb.Pact5.Backend.ChainwebPactDb as Pact5
import qualified Chainweb.Test.Pact.Store.Characterization as C
import qualified Chainweb.Test.Pact.Store.Reference as Ref
import Chainweb.Test.Utils (withTempDir)
import Chainweb.Version (PactVersion(..), PactVersionT(..))
import Chainweb.Utils.Serialization (runGetEitherS)

tests :: TestTree
tests = testGroup "PactStore SQLite"
    [ testCase "readNewest: newest version, and newest below a bound" $ withStore $ \_ ps -> do
        commit ps 1 ["t"] [("t", [("a", 1, "v1"), ("a", 2, "v2"), ("b", 3, "v3"), ("a", 5, "v5")])] []
        _psReadNewest ps "t" "a" Nothing >>= (@?= Right (Just "v5"))
        _psReadNewest ps "t" "a" (Just 5) >>= (@?= Right (Just "v2"))
        _psReadNewest ps "t" "a" (Just 2) >>= (@?= Right (Just "v1"))
        _psReadNewest ps "t" "a" (Just 1) >>= (@?= Right Nothing)
        _psReadNewest ps "t" "zz" Nothing >>= (@?= Right Nothing)

    , testCase "readNewest on a missing table returns Left ErrorError" $ withStore $ \_ ps -> do
        _psReadNewest ps "nope" "a" Nothing >>= (@?= Left SQ3.ErrorError)
        _psReadNewest ps "nope" "a" (Just 3) >>= (@?= Left SQ3.ErrorError)

    , testCase "distinctKeys KeysOrderedByKey is ascending bytewise; KeysAsStored is the set SQLite returns" $ withStore $ \sql ps -> do
        commit ps 1 ["t"] [("t", [("b", 1, "x"), ("a", 2, "x"), ("C", 3, "x"), ("a", 4, "x"), ("k10", 5, "x"), ("k9", 6, "x")])] []
        _psDistinctKeys ps (KeysOrderedByKey 7) "t" >>= (@?= Right ["C", "a", "b", "k10", "k9"])
        _psDistinctKeys ps (KeysOrderedByKey 3) "t" >>= (@?= Right ["a", "b"])
        raw <- (\rs -> [ k | [P4SQL.SText (SQ3.Utf8 k)] <- rs ]) <$> P4SQL.qry_ sql "SELECT DISTINCT rowkey FROM [t]" [P4SQL.RText]
        Right asStored <- _psDistinctKeys ps (KeysAsStored Nothing) "t"
        asStored @?= raw
        sort asStored @?= ["C", "a", "b", "k10", "k9"]
        Right asStoredBelow3 <- _psDistinctKeys ps (KeysAsStored (Just 3)) "t"
        sort asStoredBelow3 @?= ["a", "b"]

    , testCase "commitBlock: rowids ascend in _bcRows order; each table's mutation row right after its rows; index and history rows written" $ withStore $ \sql ps -> do
        commit ps 1 ["t1", "t2"] [] ["h1"]
        query sql 3 "SELECT rowid, tablename, createBlockheight FROM VersionedTableCreation ORDER BY rowid"
            >>= (@?= [["1", "t1", "1"], ["2", "t2", "1"]])
        -- record the order of every insert from here on
        forM_
            [ "CREATE TABLE log (seq INTEGER PRIMARY KEY, what TEXT)"
            , "CREATE TRIGGER log_t1 AFTER INSERT ON [t1] BEGIN INSERT INTO log (what) VALUES ('t1 ' || NEW.rowkey); END"
            , "CREATE TRIGGER log_t2 AFTER INSERT ON [t2] BEGIN INSERT INTO log (what) VALUES ('t2 ' || NEW.rowkey); END"
            , "CREATE TRIGGER log_mut AFTER INSERT ON VersionedTableMutation BEGIN INSERT INTO log (what) VALUES ('mutation ' || NEW.tablename); END"
            , "CREATE TRIGGER log_idx AFTER INSERT ON TransactionIndex BEGIN INSERT INTO log (what) VALUES ('index ' || CAST(NEW.txhash AS TEXT)); END"
            , "CREATE TRIGGER log_hist AFTER INSERT ON BlockHistory BEGIN INSERT INTO log (what) VALUES ('history ' || NEW.blockheight); END"
            ] $ P4SQL.exec_ sql
        commit ps 2 []
            [ ("t2", [("y", 3, "y3"), ("x", 2, "x2"), ("y", 4, "y4")])
            , ("t1", [("b", 5, "b5"), ("a", 2, "a2")])
            ]
            ["h2a", "h2b"]
        query sql 1 "SELECT what FROM log ORDER BY seq" >>= (@?= fmap pure
            [ "t2 y", "t2 x", "t2 y", "mutation t2"
            , "t1 b", "t1 a", "mutation t1"
            , "index h2a", "index h2b"
            , "history 2"
            ])
        query sql 4 "SELECT rowid, rowkey, txid, rowdata FROM [t2] ORDER BY rowid"
            >>= (@?= [["1", "y", "3", "y3"], ["2", "x", "2", "x2"], ["3", "y", "4", "y4"]])
        query sql 3 "SELECT rowid, tablename, blockheight FROM VersionedTableMutation ORDER BY rowid"
            >>= (@?= [["1", "t2", "2"], ["2", "t1", "2"]])
        query sql 2 "SELECT CAST(txhash AS TEXT), blockheight FROM TransactionIndex ORDER BY rowid"
            >>= (@?= [["h1", "1"], ["h2a", "2"], ["h2b", "2"]])
        query sql 3 "SELECT blockheight, hex(hash), endingtxid FROM BlockHistory ORDER BY rowid"
            >>= (@?= [["1", hashHex 1, "11"], ["2", hashHex 2, "12"]])

    , testCase "commitBlock with a duplicate TransactionIndex hash returns Left, throws nothing" $ withStore $ \_ ps -> do
        commit ps 1 [] [] ["h"]
        r <- try @SomeException $ _psCommitBlock ps (blockCommit 2 [] [] ["h"])
        case r of
            Right (Left (CommitError CommitTxIndex CommitStep SQ3.ErrorConstraint)) -> return ()
            Right other -> assertFailure $ "expected Left ErrorConstraint at the TransactionIndex step, got " <> show other
            Left e -> assertFailure $ "threw " <> show e
        -- exec': the finalize's error wins over the step's, as in Pact 5's exec'
        _psCommitBlock ps (blockCommit 1 [] [] []) >>= (@?= Left (CommitError CommitBlockHistory CommitFinalize SQ3.ErrorConstraint))

    , testGroup "commit error text: each caller's 3.2.1 message" $
        let failures =
                -- (what fails, setup, new tables, tx hashes, Pact 4 text, Pact 5 first line)
                [ ( "TransactionIndex step (duplicate hash)", \_ ps -> commit ps 1 [] [] ["h"], [], ["h"]
                  , "user error (error during batch insert: ErrorConstraint)", "ErrorConstraint" )
                , ( "TransactionIndex prepare (missing table)", \sql _ -> P4SQL.exec_ sql "DROP TABLE TransactionIndex", [], ["h"]
                  , "user error (Database error: ErrorError)", "ErrorError" )
                , ( "BlockHistory step (height taken)", \_ ps -> commit ps 2 [] [] [], [], []
                  , "user error (Database error: ErrorConstraint)", "ErrorConstraint" )
                , ( "BlockHistory prepare (missing table)", \sql _ -> P4SQL.exec_ sql "DROP TABLE BlockHistory", [], []
                  , "user error (Database error: ErrorError)", "ErrorError" )
                , ( "CREATE TABLE (an index has the name)", \sql _ -> P4SQL.exec_ sql "CREATE INDEX [boom] ON BlockHistory(blockheight)", ["boom"], []
                  , "user error (Database error: (ErrorError,\"there is already an index named boom\"))", "ErrorError" )
                ]
        in
        [ testCase "Pact 4: Database error, or error during batch insert for a batched step" $
            forM_ failures $ \(lbl, setup, tables, txs, p4Text, _) -> withStore $ \sql ps -> do
                setup sql ps
                let pending = (emptySQLitePendingData mempty)
                        { _pendingTableCreation = HashSet.fromList tables
                        , _pendingSuccessfulTxs = HashSet.fromList txs }
                r <- try @IOException $ Pact4.commitBlockStateToDatabase ps (mkHash 2) 2 (BlockHandle 20 pending)
                assertEqual lbl p4Text (either show (const "no error") r)
        , testCase "Pact 5: the SQLite error, through throwOnDbError" $
            forM_ failures $ \(lbl, setup, tables, txs, _, p5Text) -> withStore $ \sql ps -> do
                setup sql ps
                let pending = (emptySQLitePendingData InMemDb.empty)
                        { _pendingTableCreation = HashSet.fromList tables
                        , _pendingSuccessfulTxs = HashSet.fromList txs }
                r <- try @SomeException $ pact5Commit sql ps (mkHash 2) 2 (BlockHandle 20 pending)
                case r of
                    Right () -> assertFailure (lbl <> ": no error")
                    Left e -> do
                        let (firstLine, rest) = break (== '\n') (show e)
                        assertEqual lbl p5Text firstLine
                        assertBool (lbl <> ": call stack") ("\n\nCallStack (from HasCallStack):" `isPrefixOf` rest)
        ]

    , testCase "rowsAtTxId returns rows in rowid order" $ withStore $ \_ ps -> do
        commit ps 1 ["t"] [("t", [("b", 7, "x"), ("a", 7, "y"), ("c", 8, "z"), ("d", 7, "w")])] []
        _psRowsAtTxId ps "t" 7 >>= (@?= Right [("b", "x"), ("a", "y"), ("d", "w")])
        _psRowsAtTxId ps "t" 9 >>= (@?= Right [])

    , testCase "rowsInTxIdRange: from inclusive, to exclusive; readPrev and readHistorical: newest below the bound" $ withStore $ \sql ps -> do
        -- two rows at txid 3, written out of key order
        commit ps 1 ["t"] [("t", [("a", 1, "a1"), ("a", 2, "a2"), ("d", 3, "d3"), ("b", 3, "b3"), ("a", 4, "a4")])] []
        Right r <- _psRowsInTxIdRange ps "t" 2 4
        sort r @?= [(2, "a", "a2"), (3, "b", "b3"), (3, "d", "d3")]
        -- guard: a store sorted by (txid, rowkey) would return sort r
        assertBool ("rows not in (txid, rowkey) order: " <> show r) (r /= sort r)
        -- at one txid, the rows come in rowid (insertion) order
        [ k | (3, k, _) <- r ] @?= ["d", "b"]
        -- unsorted: the rows in the order 3.2.1's queryHistory statement returns them
        raw <- P4SQL.qry sql "SELECT txid, rowkey, rowdata FROM [t] WHERE txid >= ? AND txid < ?"
            [P4SQL.SInt 2, P4SQL.SInt 4] [P4SQL.RInt, P4SQL.RText, P4SQL.RBlob]
        r @?= [ (fromIntegral i, k, v) | [P4SQL.SInt i, P4SQL.SText (SQ3.Utf8 k), P4SQL.SBlob v] <- raw ]
        _psRowsInTxIdRange ps "t" 5 9 >>= (@?= Right [])
        _psReadPrev ps "t" "a" 4 >>= (@?= Right (Just "a2"))
        _psReadPrev ps "t" "a" 5 >>= (@?= Right (Just "a4"))
        _psReadPrev ps "t" "a" 1 >>= (@?= Right Nothing)
        _psReadHistorical ps "t" "a" 4 >>= (@?= Right (Just ("a", "a2")))
        _psReadHistorical ps "t" "b" 4 >>= (@?= Right (Just ("b", "b3")))
        _psReadHistorical ps "t" "b" 3 >>= (@?= Right Nothing)
        _psReadPrev ps "nope" "a" 4 >>= (@?= Left SQ3.ErrorError)

    , testCase "withBatch rolls back on exception; withReadSnapshot never persists" $ withStore $ \sql ps -> do
        r <- try @ErrorCall $ _psWithBatch ps $ do
            commit ps 1 ["t"] [("t", [("a", 1, "x")])] ["h"]
            throwIO (ErrorCall "abort") :: IO ()
        r @?= Left (ErrorCall "abort")
        _psGetLatestBlock ps >>= (@?= Nothing)
        _psTableInCatalog ps False "t" >>= (@?= Right False)
        seen <- _psWithReadSnapshot ps $ do
            commit ps 1 ["t"] [("t", [("a", 1, "x")])] ["h"]
            _psReadNewest ps "t" "a" Nothing
        seen @?= Right (Just "x")
        _psGetLatestBlock ps >>= (@?= Nothing)
        query sql 1 "SELECT COUNT(*) FROM TransactionIndex" >>= (@?= [["0"]])
        _psWithBatch ps $ commit ps 1 ["t"] [("t", [("a", 1, "x")])] ["h"]
        _psGetLatestBlock ps >>= (@?= Just (1, mkHash 1))
        _psTableInCatalog ps False "t" >>= (@?= Right True)
        _psTableInCatalog ps False "T" >>= (@?= Right False)
        _psTableInCatalog ps True "T" >>= (@?= Right True)
        _psTableExistsAt ps "T" 2 >>= (@?= Right True)
        _psTableExistsAt ps "t" 1 >>= (@?= Right False)

    , testCase "backupTo writes a file whose per-table dumps equal the source" $ withStore $ \sql ps ->
        runResourceT $ withTempDir "pact-store-backup" >>= \dir -> liftIO $ do
            commit ps 1 ["t1"] [("t1", [("a", 1, "x")])] ["h1"]
            commit ps 2 ["t2"] [("t2", [("b", 2, "y")]), ("t1", [("a", 2, "z")])] ["h2"]
            let file = dir <> "/backup.sqlite"
            _psBackupTo ps file
            source <- dumpAll sql
            copy <- withSQLiteConnection file chainwebPragmas dumpAll
            assertBool "the source has user tables" (length source > 9)
            copy @?= source

    , testGroup "differential: each store field against the 3.2.1 function it replaces"
        [ testCase "block queries on an empty database" $ withStore $ \sql ps -> do
            let p = (0, mkHash 1)
            same "getEarliestBlock" (_psGetEarliestBlock ps) (Ref.getEarliestBlock sql)
            same "getLatestBlock" (_psGetLatestBlock ps) (Ref.getLatestBlock sql)
            same "lookupBlock" (_psLookupBlock ps p) (Ref.lookupBlock sql p)
            same "getBlockParent" (_psGetBlockParent ps C.v4 C.cid p) (Ref.getBlockParent C.v4 C.cid sql p)
            same "getEndTxId" (_psGetEndTxId ps "msg" Nothing) (Ref.getEndTxId "msg" sql Nothing)
            same "getEndTxId'" (_psGetEndTxId' ps "msg" 0 (mkHash 1)) (Ref.getEndTxId' "msg" sql 0 (mkHash 1))

        , testCase "block queries on the characterization script" $ C.withP4 $ \cp sql -> do
            scriptBlocks cp
            let ps = sqlitePactStore sql
                inDb = [ (view blockHeight h, view blockHash h) | h <- take 7 C.p4Headers ]
                -- unknown hash at a known height, known hash at the wrong
                -- height, a height above the tip, and the genesis hash
                notInDb =
                    [ (3, mkHash 99), (3, view blockHash (C.p4Header 4))
                    , (7, view blockHash (C.p4Header 7)), (0, mkHash 99) ]
            same "getEarliestBlock" (_psGetEarliestBlock ps) (Ref.getEarliestBlock sql)
            same "getLatestBlock" (_psGetLatestBlock ps) (Ref.getLatestBlock sql)
            forM_ (inDb <> notInDb) $ \p -> do
                same ("lookupBlock " <> show p) (_psLookupBlock ps p) (Ref.lookupBlock sql p)
                same ("getBlockParent " <> show p)
                    (_psGetBlockParent ps C.v4 C.cid p) (Ref.getBlockParent C.v4 C.cid sql p)
            -- _psGetEndTxId: the tip, every parent, a parent that is not in the db
            let parents = Nothing : [ Just (ParentHeader h) | h <- take 8 C.p4Headers ]
                         <> [ Just (ParentHeader ((C.p4Header 3) { _blockHash = mkHash 99 })) ]
            forM_ parents $ \mp ->
                same ("getEndTxId " <> show (fmap (view blockHeight . _parentHeader) mp))
                    (_psGetEndTxId ps "msg" mp) (Ref.getEndTxId "msg" sql mp)
            forM_ (inDb <> notInDb) $ \(h, bh) ->
                same ("getEndTxId' " <> show (h, bh)) (_psGetEndTxId' ps "msg" h bh) (Ref.getEndTxId' "msg" sql h bh)
            -- the script yields 1, 4, 7, 9, 10, 12, 13
            _psGetEndTxId' ps "msg" 2 (view blockHash (C.p4Header 2)) >>= (@?= Historical 7)

        , testCase "doLookupSuccessful" $ C.withP4 $ \cp sql -> do
            scriptBlocks cp
            let ps = sqlitePactStore sql
                hashes = V.fromList $ SB.toShort . B8.pack <$>
                    ["tx-0-0", "tx-1-1", "tx-2-0", "tx-3-1", "tx-6-0", "unknown", ""]
            forM_ [0, 1, 2, 3, 7, 100] $ \h -> do
                same ("doLookupSuccessful " <> show h)
                    (_psDoLookupSuccessful ps h hashes) (Ref.doLookupSuccessful sql h hashes)
            r <- _psDoLookupSuccessful ps 7 hashes
            assertBool "found some hashes" (HM.size r >= 5)

        , testCase "txIdsAfter equals _txids of the Pact 4 db and 3.2.1's doTxIds statement" $ C.withP4 $ \cp sql -> do
            scriptBlocks cp
            let ps = sqlitePactStore sql
            -- _txids reaches the store itself since Task 14, so the 3.2.1
            -- statement, run directly, is the oracle of the unsorted result
            forM_ [("t1", 0), ("t1", 4), ("t1", 9), ("t2", 0), ("t2", 5)] $ \(t, n) -> do
                raw <- P4SQL.qry sql ("SELECT DISTINCT txid FROM [" <> SQ3.Utf8 (T.encodeUtf8 t) <> "] WHERE txid > ?")
                    [P4SQL.SInt n] [P4SQL.RInt]
                new <- _psTxIdsAfter ps t (P4.TxId (fromIntegral n))
                assertEqual (show (t, n)) (Right [ P4.TxId (fromIntegral i) | [P4SQL.SInt i] <- raw ]) new
            r <- Cp.readFrom cp (Just (ParentHeader (C.p4Header 6))) Pact4T $ \env _ ->
                forM [("t1", 0), ("t1", 4), ("t1", 9), ("t1", 100), ("t2", 0), ("t2", 5)] $ \(t, n) -> do
                    let P4.PactDbEnv pdb mv = Pact4._cpPactDbEnv env
                    old <- P4._txids pdb (P4.TableName t) (P4.TxId n) mv
                    new <- _psTxIdsAfter ps t (P4.TxId n)
                    return (show (t, n), Right old, fmap sort new)
            case r of
                NoHistory -> assertFailure "NoHistory"
                Historical rs -> do
                    forM_ rs $ \(lbl, old, new) -> assertEqual lbl old new
                    assertBool "non-trivial" (any (\(_, o, _) -> o /= Right []) rs)

        , testCase "rewindDbTo: same txid or exception, same dump of every table afterwards" $ do
            let targets = Nothing : [ Just (ParentHeader h) | h <- take 7 C.p4Headers ]
                    -- a parent that is not in the db
                    <> [ Just (ParentHeader ((C.p4Header 3) { _blockHash = mkHash 99 })), Just (ParentHeader (C.p4Header 8)) ]
            forM_ targets $ \mp -> C.withP4 $ \cpA sqlA -> C.withP4 $ \cpB sqlB -> do
                scriptBlocks cpA
                scriptBlocks cpB
                dumpAll sqlA >>= \a -> dumpAll sqlB >>= (@?= a)
                new <- try @SomeException $ _psRewindDbTo (sqlitePactStore sqlA) mp
                old <- try @SomeException $ Ref.rewindDbTo sqlB mp
                let lbl = "rewindDbTo " <> show (fmap (view blockHeight . _parentHeader) mp)
                assertEqual lbl (either show show old) (either show show new)
                a <- dumpAll sqlA
                b <- dumpAll sqlB
                assertEqual (lbl <> ": dumps") b a

        , testCase "getBlockHistory and lookupHistorical on a Pact 5 script: same results, row order included" $
            C.withP5 $ \cpA sqlA -> C.withP5 $ \cpB sqlB -> do
                -- A and B are built by the same script; A is read through the
                -- store, B through the frozen 3.2.1 functions
                p5ScriptBlocks cpA
                p5ScriptBlocks cpB
                -- block 7 writes five rows of t1 in one transaction, so one
                -- txid has rows that are not in key order
                forM_ [cpA, cpB] $ \cp -> C.extend cp (Just (C.p5Headers !! 6))
                    [C.p5Block (C.p5Headers !! 7)
                        [C.tx 7 0 [ C.Put "t1" k n | (k, n) <- zip ["q", "e", "m", "c", "w"] [16..] ]]]
                dumpAll sqlA >>= \a -> dumpAll sqlB >>= (@?= a)
                let ps = sqlitePactStore sqlA
                    -- blocks 0 to 7 are in the db, block 8 is not
                    headers = take 9 C.p5Headers
                    domains = [ P5.DUserTables (C.tn5 t) | t <- ["t1", "t2", "nope"] ]
                    keys = P5.RowKey <$> ["a", "b", "C", "zz", "k9", "k10", "B", "x", "y", "missing"]
                    -- BlockTxHistory's Show leaves out the prev map; compare both
                    hist = fmap (fmap (\bth -> (_blockTxHistory bth, _blockPrevHistory bth)))
                multi <- fmap concat $ forM headers $ \h -> fmap concat $ forM domains $ \d -> do
                    let lbl = show (view blockHeight h, P5.renderDomain d)
                    new <- try @SomeException $ Cp.getBlockHistory ps h d
                    old <- try @SomeException $ Ref.getBlockHistory sqlB h d
                    sameOutcome ("getBlockHistory " <> lbl) (hist old) (hist new)
                    forM_ keys $ \k -> do
                        newL <- try @SomeException $ Cp.lookupHistorical ps h d k
                        oldL <- try @SomeException $ Ref.lookupHistorical sqlB h d k
                        sameOutcome ("lookupHistorical " <> lbl <> " " <> show k) oldL newL
                    return [ ks | Right (Historical bth) <- [old], ls <- M.elems (_blockTxHistory bth)
                                , let ks = P5._txKey <$> ls, ks /= sort ks, ks /= reverse (sort ks) ]
                -- some txid's rows are in neither ascending nor descending key
                -- order, so a store that sorts by (txid, rowkey) gives
                -- different log lists
                assertBool "a txid whose rows are not in key order" (not (null multi))
        ]
    ]

-- -------------------------------------------------------------------------- --
-- Utils

withStore :: (SQLiteEnv -> PactStore -> IO a) -> IO a
withStore k = withTempSQLiteConnection chainwebPragmas $ \sql -> do
    let ps = sqlitePactStore sql
    _psInitSchema ps
    k sql ps

mkHash :: Int -> BlockHash
mkHash n = either error id $ runGetEitherS decodeBlockHash (BS.replicate 32 (fromIntegral n))

hashHex :: Int -> Text
hashHex n = T.replicate 32 ("0" <> T.pack (show n)) -- n < 10

blockCommit
    :: BlockHeight -> [Text] -> [(Text, [(ByteString, Int, ByteString)])] -> [ByteString]
    -> BlockCommit
blockCommit h newTables rows txs = BlockCommit
    { _bcHeight = h
    , _bcHash = mkHash (fromIntegral h)
    , _bcEndTxId = fromIntegral (10 + h)
    , _bcNewTables = newTables
    , _bcRows = [ (t, [ (k, fromIntegral i, v) | (k, i, v) <- rs ]) | (t, rs) <- rows ]
    , _bcTxHashes = txs
    }

commit
    :: PactStore -> BlockHeight -> [Text] -> [(Text, [(ByteString, Int, ByteString)])] -> [ByteString]
    -> IO ()
commit ps h newTables rows txs =
    _psCommitBlock ps (blockCommit h newTables rows txs) >>= either (fail . show) return

-- | Pact 5's @commitBlockStateToDatabase@.
pact5Commit :: SQLiteEnv -> PactStore -> BlockHash -> BlockHeight -> BlockHandle Pact5 -> IO ()
pact5Commit _ ps = Pact5.commitBlockStateToDatabase ps

-- | Run both actions and compare their results.
same :: (Eq a, Show a) => String -> IO a -> IO a -> Assertion
same lbl new old = do
    n <- new
    o <- old
    assertEqual lbl o n

-- | Both threw the same text, or both returned equal values.
sameOutcome :: (Eq a, Show a) => String -> Either SomeException a -> Either SomeException a -> Assertion
sameOutcome lbl old new = case (old, new) of
    (Right o, Right n) -> assertEqual lbl o n
    _ -> assertEqual lbl (either show show old) (either show show new)

-- | The seven blocks of the characterization script.
scriptBlocks :: Cp.Checkpointer GenericLogger -> IO ()
scriptBlocks cp = C.extend cp Nothing (zipWith C.p4Block C.p4Headers C.scenario)

-- | The same script in Pact 5 blocks.
p5ScriptBlocks :: Cp.Checkpointer GenericLogger -> IO ()
p5ScriptBlocks cp = C.extend cp Nothing (zipWith C.p5Block C.p5Headers C.scenario)

-- | Run a query of @n@ columns, all rendered as text.
query :: SQLiteEnv -> Int -> SQ3.Utf8 -> IO [[Text]]
query sql n q = do
    rows <- P4SQL.qry_ sql q (replicate n P4SQL.RText)
    return $ fmap (fmap (\case P4SQL.SText (SQ3.Utf8 t) -> T.decodeUtf8 t; s -> error (show s))) rows

-- | Every table, every row, every column quoted, in rowid order.
dumpAll :: SQLiteEnv -> IO [(Text, [[Text]])]
dumpAll sql = do
    tables <- (\rs -> [ t | [P4SQL.SText t] <- rs ]) <$> P4SQL.qry_ sql
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name" [P4SQL.RText]
    forM tables $ \t@(SQ3.Utf8 tb) -> do
        cols <- (\rs -> [ c | [_, P4SQL.SText c] <- rs ]) <$> P4SQL.qry_ sql ("PRAGMA table_info('" <> t <> "')") [P4SQL.RInt, P4SQL.RText]
        rows <- P4SQL.qry_ sql
            ("SELECT " <> mconcat (intersperse ", " ("quote(rowid)" : fmap (\c -> "quote(" <> c <> ")") cols))
                <> " FROM [" <> t <> "] ORDER BY rowid")
            (P4SQL.RText <$ ("rowid" : cols))
        return (T.decodeUtf8 tb, fmap (fmap (\case P4SQL.SText (SQ3.Utf8 v) -> T.decodeUtf8 v; s -> error (show s))) rows)
