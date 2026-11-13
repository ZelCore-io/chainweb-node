{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

-- |
-- Module: Chainweb.StorageTools.Test.Cold
--
-- Tests of the cold-store measurement. The fixture is a Pact SQLite file
-- written by the checkpointer from a Pact 4 test chain with intra-block
-- versions (PersistIntraBlockWrites), user tables, SYS tables and a
-- transaction index.
--
module Chainweb.StorageTools.Test.Cold (tests) where

import Control.Lens ((&), (.~), view)
import Control.Monad

import Data.Aeson (decode, encode)

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy.Char8 as BL8
import qualified Data.ByteString.Short as SBS
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import qualified Database.SQLite3 as SQ

import qualified Streaming.Prelude as SP

import System.Directory
import System.FilePath ((</>))
import System.IO.Temp (createTempDirectory, getCanonicalTemporaryDirectory)
import System.LogLevel (LogLevel(..))

import Test.Tasty
import Test.Tasty.HUnit

import qualified Database.RocksDB.Base as R

import qualified Pact.Interpreter as P (PactDbEnv(..))
import qualified Pact.Types.Command as P
import qualified Pact.Types.Hash as P
import qualified Pact.Types.RowData as P
import qualified Pact.Types.Runtime as P

-- internal modules

import Chainweb.BlockHash
import Chainweb.BlockHeader.Internal (BlockHeader, blockHash, blockHeight, blockParent, genesisBlockHeader)
import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.MerkleLogHash (merkleLogHash)
import Chainweb.Pact.Backend.Store.Cold.Key
import Chainweb.Pact.Backend.Types (IntraBlockPersistence(..))
import Chainweb.Pact.Backend.Utils (chainwebPragmas, closeSQLiteConnection, openSQLiteConnection)
import qualified Chainweb.Pact.PactService.Checkpointer.Internal as Checkpointer
import Chainweb.Pact.Types (RunnableBlock(..), defaultModuleCacheLimit)
import Chainweb.Pact4.Backend.ChainwebPactDb (CurrentBlockDbEnv(..))
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning
import Chainweb.StorageTools.Measure.Cold
import Chainweb.Test.TestVersions (slowForkingCpmTestVersion)
import Chainweb.Test.Utils (petersen)
import Chainweb.Version

-- -------------------------------------------------------------------------- --
-- Fixture

data Fixture = Fixture
    { _fxDir :: FilePath
    , _fxSqlite :: FilePath
    , _fxCold :: FilePath
    , _fxSizes :: ColdSizes
    }

cid :: ChainId
cid = unsafeChainId 3

testVer :: ChainwebVersion
testVer = slowForkingCpmTestVersion petersen

quietLogger :: GenericLogger
quietLogger = genericLogger Error (\_ -> return ())

-- | Row keys that share prefixes, so that the escaped order matters.
--
keyPool :: [Text]
keyPool = ["a", "ab", "b", "k1", "k10", "k2", "z"]

blockCount :: Int
blockCount = 30

txsPerBlock :: Int
txsPerBlock = 4

-- | Writes the fixture file: a genesis block that creates two user tables,
-- then 'blockCount' blocks of 'txsPerBlock' transactions. Every transaction
-- writes several keys of @user1@ and one key of @user2@, so most keys have
-- several versions inside one block, and registers its hash in the
-- transaction index. Some transactions write @SYS:KeySets@ and @SYS:Pacts@.
--
writeFixture :: FilePath -> IO ()
writeFixture path = do
    sql <- openSQLiteConnection path chainwebPragmas
    cp <- Checkpointer.initCheckpointerResources defaultModuleCacheLimit sql
        PersistIntraBlockWrites quietLogger testVer cid
    (h0, hs) <- fmap (\l -> (l !! 0, drop 1 l)) $ forM [0 .. blockCount] $ \i ->
        let s = show (i :: Int) in BlockHash <$> merkleLogHash (B8.pack (replicate (32 - length s) '0' <> s))
    let genesis = genesisBlockHeader testVer cid & blockHash .~ h0
        headers = scanl child genesis hs
        child p h = p
            & blockHash .~ h
            & blockParent .~ view blockHash p
            & blockHeight .~ view blockHeight p + 1
        blocks = zipWith runBlock [0 ..] headers
    void $ Checkpointer.restoreAndSave cp Nothing (SP.each blocks)
    closeSQLiteConnection sql
  where
    runBlock :: Int -> BlockHeader -> RunnableBlock GenericLogger ()
    runBlock b hdr = Pact4RunnableBlock $ \env _ -> do
        let P.PactDbEnv pdb e = _cpPactDbEnv env
            tx :: Int -> IO () -> IO ()
            tx j act = do
                void $ P._beginTx pdb P.Transactional e
                act
                void $ P._commitTx pdb e
                _cpRegisterProcessedTx env $ P.RequestKey $ P.Hash $ SBS.toShort $ txHash b j
        if b == 0
          then tx 0 $ do
            P._createUserTable pdb "user1" "m" e
            P._createUserTable pdb "user2" "m" e
            P._writeRow pdb P.Write P.KeySets "ks0" (keyset 0) e
          else forM_ [0 .. txsPerBlock - 1] $ \j -> tx j $ do
            let pool = length keyPool
                ks = [ keyPool !! ((b * m + j * n) `mod` pool) | (m, n) <- [(1, 1), (1, 2), (3, 1)] ]
            forM_ (dedup ks) $ \k ->
                P._writeRow pdb P.Write (P.UserTables "user1") (P.RowKey k) (row (b * 100 + j)) e
            P._writeRow pdb P.Write (P.UserTables "user2") (P.RowKey ("x" <> T.pack (show (b `mod` 3)))) (row b) e
            when (j == 0) $
                P._writeRow pdb P.Write P.Pacts (P.PactId ("p" <> T.pack (show (b `mod` 5)))) Nothing e
            when (j == 1) $
                P._writeRow pdb P.Write P.KeySets (fromString' ("ks" <> show (b `mod` 2))) (keyset b) e
        return ((), hdr)

    dedup = M.keys . M.fromList . fmap (, ())
    row :: Int -> P.RowData
    row i = P.RowData P.RDV1 $ P.ObjectMap $ M.fromList
        [("v", P.RDLiteral (P.LInteger (fromIntegral i)))]
    keyset i = P.mkKeySet [P.PublicKeyText (T.pack ("key" <> show (i `mod` 2 :: Int)))] "keys-all"
    fromString' s = P.KeySetName (T.pack s) Nothing
    txHash b j = B8.pack $ take 32 $ "tx-" <> show b <> "-" <> show j <> repeat '.'

acquire :: IO Fixture
acquire = do
    tmp <- getCanonicalTemporaryDirectory
    dir <- createTempDirectory tmp "cold-measure"
    let sqlite = dir </> "pact.sqlite"
        cold = dir </> "cold"
    writeFixture sqlite
    sizes <- buildColdFromSqlite quietLogger ColdBuildSpec
        { _cbSqlite = sqlite
        , _cbChain = cid
        , _cbTables = Nothing
        , _cbOut = cold
        , _cbBatchBytes = 4096
        }
    return $ Fixture dir sqlite cold sizes

release :: Fixture -> IO ()
release = removeDirectoryRecursive . _fxDir

-- -------------------------------------------------------------------------- --
-- Helpers

queryAll :: SQ.Database -> Text -> [SQ.SQLData] -> IO [[SQ.SQLData]]
queryAll db q params = do
    stmt <- SQ.prepare db q
    SQ.bind stmt params
    let go acc = SQ.step stmt >>= \case
            SQ.Done -> return (reverse acc)
            SQ.Row -> SQ.columns stmt >>= \r -> go (r : acc)
    rs <- go []
    SQ.finalize stmt
    return rs

withSqlite :: FilePath -> (SQ.Database -> IO a) -> IO a
withSqlite path k = do
    db <- SQ.open2 (T.pack path) [SQ.SQLOpenReadOnly] SQ.SQLVFSDefault
    r <- k db
    SQ.close db
    return r

bytesOf :: SQ.SQLData -> B.ByteString
bytesOf = \case
    SQ.SQLText t -> T.encodeUtf8 t
    SQ.SQLBlob b -> b
    d -> error $ "unexpected SQLite value: " <> show d

withCold :: Fixture -> (R.DB -> IO a) -> IO a
withCold fx k = withReadOnlyRocksDbTuned (_fxCold fx) modernDefaultOptions coldStoreTuning
    (k . _rocksDbHandle)

-- | The @N@ entries: table name and id.
--
catalog :: R.DB -> IO [(Text, TableId)]
catalog db = do
    entries <- scanPrefix db (B.pack [78, chainByte cid])
    forM entries $ \(k, v) -> case B.unpack v of
        [a, b, c, d] -> return
            ( T.decodeUtf8 (B.drop 2 k)
            , TableId (foldl (\acc w -> acc * 256 + fromIntegral w) 0 [a, b, c, d])
            )
        _ -> assertFailure ("invalid table id: " <> show v) >> undefined

tablePrefix :: Char -> TableId -> B.ByteString
tablePrefix ch (TableId t) = B.pack
    [ fromIntegral (fromEnum ch), chainByte cid
    , fromIntegral (t `div` 16777216), fromIntegral (t `div` 65536 `mod` 256)
    , fromIntegral (t `div` 256 `mod` 256), fromIntegral (t `mod` 256)
    ]

isUser :: Text -> Bool
isUser = not . T.isPrefixOf "SYS:"

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = withResource acquire release $ \getFx -> testGroup "Cold measure"
    [ testCase "the fixture has intra-block versions, user and SYS tables" $ do
        fx <- getFx
        withSqlite (_fxSqlite fx) $ \sq -> do
            -- rows of one key in one block: same table, key and block, different txids
            rs <- queryAll sq
                "SELECT count(*) FROM user1 a JOIN user1 b ON a.rowkey = b.rowkey AND a.txid < b.txid \
                \WHERE (SELECT min(endingtxid) FROM BlockHistory WHERE endingtxid > a.txid) \
                \    = (SELECT min(endingtxid) FROM BlockHistory WHERE endingtxid > b.txid)" []
            case rs of
                [[SQ.SQLInteger n]] -> assertBool ("intra-block versions: " <> show n) (n > 0)
                _ -> assertFailure (show rs)
        let names = M.keys (_csTables (_fxSizes fx))
        forM_ ["user1", "user2", "SYS:KeySets", "SYS:Pacts", "TransactionIndex"] $ \n ->
            assertBool ("missing table " <> T.unpack n <> " in " <> show names) (n `elem` names)

    , testCase "every SQLite row has exactly one R entry with identical bytes" $ do
        fx <- getFx
        withCold fx $ \db -> withSqlite (_fxSqlite fx) $ \sq -> do
            cat <- catalog db
            expected <- fmap concat $ forM cat $ \(name, tid) -> do
                rows <- queryAll sq ("SELECT rowkey, txid, rowdata FROM [" <> name <> "] ORDER BY rowid") []
                let sizes = _csTables (_fxSizes fx) M.! name
                fromIntegral (length rows) @?= _tsVersions sizes
                return
                    [ (rowKey cid tid (bytesOf k) (fromIntegral txid), bytesOf v)
                    | [k, SQ.SQLInteger txid, v] <- rows
                    ]
            got <- scanPrefix db "R"
            assertBool "no rows" (not (null expected))
            -- equal as lists in key order and as maps: one entry per row
            M.size (M.fromList expected) @?= length expected
            got @?= M.toList (M.fromList expected)

    , testCase "T list of every txid equals SELECT rowkey FROM t WHERE txid = ? ORDER BY rowid" $ do
        fx <- getFx
        withCold fx $ \db -> withSqlite (_fxSqlite fx) $ \sq -> do
            cat <- catalog db
            longest <- fmap concat $ forM (filter (isUser . fst) cat) $ \(name, tid) -> do
                txids <- queryAll sq ("SELECT DISTINCT txid FROM [" <> name <> "] ORDER BY txid") []
                lens <- forM [ t | [SQ.SQLInteger t] <- txids ] $ \txid -> do
                    expected <- fmap (fmap bytesOf . concat) $
                        queryAll sq ("SELECT rowkey FROM [" <> name <> "] WHERE txid = ? ORDER BY rowid")
                            [SQ.SQLInteger txid]
                    v <- R.get db mempty (txidKey cid tid (fromIntegral txid))
                    ids <- maybe (assertFailure ("no T entry for txid " <> show txid) >> undefined) return
                        (v >>= decodeKeyIdList)
                    keys <- forM ids $ \i ->
                        fromMaybe B.empty <$> R.get db mempty (idKey cid tid i)
                    keys @?= expected
                    -- K resolves back to the same ids
                    forM_ (zip keys ids) $ \(k, KeyId i) -> do
                        kv <- R.get db mempty (keyIdKey cid tid k)
                        kv @?= Just (B.pack [ fromIntegral (i `div` 16777216), fromIntegral (i `div` 65536 `mod` 256)
                                            , fromIntegral (i `div` 256 `mod` 256), fromIntegral (i `mod` 256) ])
                    return (length ids)
                tEntries <- scanPrefix db (tablePrefix 'T' tid)
                length tEntries @?= length txids
                return lens
            assertBool "no T list with more than one row" (any (> 1) longest)

    , testCase "SYS tables get no T, K or I entries" $ do
        fx <- getFx
        withCold fx $ \db -> do
            cat <- catalog db
            let sys = filter (not . isUser . fst) cat
            assertBool ("no SYS tables in " <> show (fst <$> cat)) (length sys >= 2)
            forM_ sys $ \(name, tid) -> forM_ ("TKI" :: String) $ \ks -> do
                es <- scanPrefix db (tablePrefix ks tid)
                assertBool (T.unpack name <> " has " <> [ks] <> " entries") (null es)
            -- and the user tables do have them
            forM_ (filter (isUser . fst) cat) $ \(name, tid) -> forM_ ("TKI" :: String) $ \ks -> do
                es <- scanPrefix db (tablePrefix ks tid)
                assertBool (T.unpack name <> " has no " <> [ks] <> " entries") (not (null es))

    , testCase "cold read answer equals SQLite newest-before-bound for 1,000 sampled reads" $ do
        fx <- getFx
        l <- measureColdReads (_fxSqlite fx) (_fxCold fx) 1000 7
        _clReads l @?= 1000
        _clMismatches l @?= 0
        assertBool ("found " <> show (_clFound l)) (_clFound l > 0 && _clFound l < 1000)

    , testCase "the sizes cover every keyspace and the hot parts" $ do
        fx <- getFx
        let cs = _fxSizes fx
        M.keys (_csKeyspaceSst cs) @?= ["I", "K", "N", "R", "T", "X"]
        -- approximate sizes have block granularity: the fixture's few
        -- blocks may be attributed to one keyspace, never more than the total
        let ks = sum (M.elems (_csKeyspaceSst cs))
        assertBool ("keyspaces: " <> show cs) (ks > 0 && ks <= _csTotalSst cs)
        assertBool ("total: " <> show cs) (_csTotalSst cs > 0)
        assertBool ("kept: " <> show cs) (_csHotKeptBytes cs > 0)
        assertBool ("anchors: " <> show cs) (_csHotAnchorBytes cs > 0)
        _csChain cs @?= "3"
        -- cw-measure project reads what cw-measure cold writes
        decode (encode cs) @?= Just cs
        let tx = _csTables cs M.! "TransactionIndex"
        _tsVersions tx @?= fromIntegral (blockCount * txsPerBlock + 1)
        _tsKeyBytes tx @?= 32 * _tsVersions tx

    , testCase "projection of the spec's per-part inputs (chain 0 44.8, chain 19 4.3, chain 3 plus the other 17 34 GB) yields 83 GB and verdict R" $ do
        let p = projectCold specInputs
        assertBool (show p) $ abs (_projChain0GB p - 44.8) < 1e-9
        assertBool (show p) $ abs (_projChain19GB p - 4.3) < 1e-9
        assertBool (show p) $ abs (_projChain3GB p + _projOthersGB p - 34) < 1e-9
        assertBool (show p) $ abs (_projChain3Ratio p - 3) < 1e-9
        assertBool (show p) $ abs (_projPactTotalGB p - 83.1) < 1e-9
        round (_projPactTotalGB p) @?= (83 :: Int)
        _projVerdict p @?= R

    , testCase "projection above the estimate plus 25 GB yields verdict S" $ do
        -- spec 4.3 pessimistic: chain 0 93.7, chain 19 7.5, the other 18 45 GB
        let pin = specInputs
                { _piChain0Logical = M.fromList [("coin_coin-table", (40000000000, 867000000000))]
                , _piChain19 = (_piChain19 specInputs) { _csTotalSst = 7500000000 - 300000000 }
                , _piChain3 = (_piChain3 specInputs) { _csTotalSst = 2000000000 - 300000000 }
                }
            p = projectCold pin
        assertBool (show p) $ abs (_projChain0GB p - 93.7) < 1e-9
        assertBool (show p) $ abs (_projChain3GB p + _projOthersGB p - 45.333333333) < 1e-6
        _projVerdict p @?= S

    , testCase "chain 0 logical bytes from the report's walk format" $ do
        let walk = BL8.pack
                "{\"page_size\":4096,\"trees\":{\
                \\"coin_coin-table\":{\"type\":\"table\",\"table\":\"coin_coin-table\",\"cells\":10,\"key_bytes\":100,\"val_bytes\":1000,\"distinct_keys\":null},\
                \\"sqlite_autoindex_coin_coin-table_1\":{\"type\":\"index\",\"table\":\"coin_coin-table\",\"cells\":10,\"key_bytes\":0,\"val_bytes\":0,\"distinct_keys\":3},\
                \\"TransactionIndex\":{\"type\":\"table\",\"table\":\"TransactionIndex\",\"cells\":5,\"key_bytes\":0,\"val_bytes\":0,\"distinct_keys\":null},\
                \\"BlockHistory\":{\"type\":\"table\",\"table\":\"BlockHistory\",\"cells\":7,\"key_bytes\":70,\"val_bytes\":70,\"distinct_keys\":null}}}"
        chain0LogicalFromWalk walk @?= Right (M.fromList
            [ ("coin_coin-table", (180, 1000))
            , ("TransactionIndex", (160, 40))
            ])
    ]

-- | Spec 4.3's per-part figures as inputs. Chain 0: 418 GB logical at ratio
-- 10 plus 3.0 GB hot is 44.8. Chain 19: 4.0 GB cold store plus 0.3 GB hot is
-- 4.3. Chain 3: 4.5 GB today, 1.5 GB projected (ratio 3.0); the other 17
-- chains: 97.5 GB today, 32.5 GB projected; 34 GB with chain 3.
--
specInputs :: ProjectionInputs
specInputs = ProjectionInputs
    { _piChain0Logical = M.fromList
        [ ("coin_coin-table", (18000000000, 300000000000))
        , ("SYS:Pacts", (10000000000, 90000000000))
        ]
    , _piChain0Ratio = 10
    , _piChain3 = sizes "3" 4500000000 1200000000 300000000
    , _piChain19 = sizes "19" 30500000000 4000000000 300000000
    , _piHot = specChain0HotBytes
    , _piOthersTodayBytes = 97500000000
    }
  where
    sizes c today cold hot = ColdSizes
        { _csChain = c
        , _csSqliteFileBytes = today
        , _csTables = M.empty
        , _csKeyspaceSst = M.empty
        , _csTotalSst = cold
        , _csHotKeptBytes = hot `div` 3
        , _csHotAnchorBytes = hot - hot `div` 3
        }
