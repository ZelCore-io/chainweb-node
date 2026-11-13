{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.StorageTools.Test.InProcess
--
-- Tests of the in-process test side of @cw-compare@. The fixture is a test
-- chain of 'instantCpmTransitionTestVersion' on the pair graph, mined by a
-- 'CutDb' and the Pact services of both chains, with coin transfers at Pact 4
-- and at Pact 5 heights. It is built once into a node database directory
-- (@rocksDb@ and @sqlite@), and every test runs on its own copy.
--
module Chainweb.StorageTools.Test.InProcess
( tests
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, cancel)
import Control.Exception (bracket, throwIO)
import Control.Lens (view)
import Control.Monad
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource (ResourceT, allocate, runResourceT)

import Data.Aeson
import qualified Data.Aeson.Key as K
import Data.Aeson.Types (parseMaybe)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import Data.Int (Int64)
import Data.List (sort)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V

import qualified Database.SQLite3 as SQ

import qualified Network.HTTP.Client as HTTP

import System.Directory
import System.FilePath (takeExtension, (</>))
import System.IO.Temp (createTempDirectory, getCanonicalTemporaryDirectory)
import qualified System.LogLevel as L

import Test.Tasty
import Test.Tasty.HUnit

import Pact.Core.Capabilities (CapToken(..))
import Pact.Core.Hash (hash, hashToText)
import Pact.Core.Names (ModuleName(..), QualifiedName(..))
import Pact.Core.PactValue

-- internal modules

import Chainweb.BlockHeader (Nonce(..), ParentHeader(..), blockHash, blockHeight, blockPayloadHash)
import Chainweb.BlockHeight (BlockHeight)
import Chainweb.ChainId
import Chainweb.Cut (Cut, _cutMap, _cutHeight)
import Chainweb.Cut.CutHashes (CutHashes(..), cutToCutHashes)
import Chainweb.CutDB
import Chainweb.Graph (pairChainGraph)
import Chainweb.Logger
import Chainweb.Mempool.Consensus (mkMempoolConsensus)
import Chainweb.Mempool.Mempool (InsertType(..), mempoolInsert)
import Chainweb.Miner.Pact (noMiner)
import Chainweb.Pact.Backend.Types (IntraBlockPersistence(..))
import Chainweb.Pact.Backend.Utils (chainDbFileName, startSqliteDb, stopSqliteDb)
import Chainweb.Pact.PactService (runPactService)
import Chainweb.Pact.Service.PactInProcApi (pactMemPoolAccess)
import Chainweb.Pact.Service.PactQueue (newPactQueue)
import Chainweb.Pact.Types
import Chainweb.Payload
import Chainweb.Payload.PayloadStore
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning (defaultTuning)
import Chainweb.StorageTools.Compare.InProcess
import Chainweb.StorageTools.Compare.Report
import Chainweb.StorageTools.Compare.Rules (answerPin)
import Chainweb.StorageTools.Compare.Run (runCompare, tipProbe)
import Chainweb.StorageTools.Compare.Types
import Chainweb.Sync.WebBlockHeaderStore (newWebBlockHeaderStore, newWebPayloadStore)
import Chainweb.Test.Cut (testMineWithPayloadHash)
import Chainweb.Test.Pact5.CmdBuilder
import Chainweb.Test.Pact5.Utils (withBlockDbs, withMempool)
import Chainweb.Test.TestVersions (instantCpmTransitionTestVersion)
import Chainweb.Time (Time(..), TimeSpan(..), Micros, getCurrentTimeIntegral)
import Chainweb.Utils (T2(..), toText)
import Chainweb.Version
import Chainweb.Version.Registry (registerVersion)
import Chainweb.WebBlockHeaderDB (getWebBlockHeaderDb)
import Chainweb.WebPactExecutionService

-- -------------------------------------------------------------------------- --
-- Parameters

fxVersion :: ChainwebVersion
fxVersion = instantCpmTransitionTestVersion pairChainGraph

cid0 :: ChainId
cid0 = unsafeChainId 0

fxChains :: [ChainId]
fxChains = sort (HS.toList (chainIds fxVersion))

-- | The first Pact 5 height of 'fxVersion' is 20.
--
pact4TransferHeights, pact5TransferHeights :: [BlockHeight]
pact4TransferHeights = [firstTransferHeight, 8, 12]
pact5TransferHeights = [21, 23]

firstTransferHeight :: BlockHeight
firstTransferHeight = 5

-- | The height at which the SQLite files are copied for the snapshot.
--
snapshotHeight :: BlockHeight
snapshotHeight = 22

finalHeight :: BlockHeight
finalHeight = 25

-- | The Pact 4 height that the @/local@ requests address.
--
localTarget :: BlockHeight
localTarget = 10

apiPrefix :: T.Text
apiPrefix = "/chainweb/0.0/" <> toText (_versionName fxVersion)

quietLogger :: GenericLogger
quietLogger = genericLogger L.Error (\_ -> return ())

-- -------------------------------------------------------------------------- --
-- Fixture

data Fixture = Fixture
    { _fxRoot :: !FilePath
    , _fxDb :: !FilePath
        -- ^ node database directory, headers and Pact state up to 'finalHeight'
    , _fxSnapshotSqlite :: !FilePath
        -- ^ the @sqlite@ directory as of 'snapshotHeight'
    , _fxPayloadHash :: !BlockPayloadHash
        -- ^ the payload of chain 0 at the first Pact 4 transfer height
    , _fxAccount :: !T.Text
        -- ^ a coin account of chain 0 that has a single row, from genesis
    }

-- | Copies of the database directory of the fixture, one per test.
--
copyDb :: Fixture -> String -> IO FilePath
copyDb fx name = do
    dir <- createTempDirectory (_fxRoot fx) name
    copyTree (_fxDb fx) dir
    return dir

copyTree :: FilePath -> FilePath -> IO ()
copyTree src dst = do
    createDirectoryIfMissing True dst
    es <- listDirectory src
    forM_ es $ \e -> do
        isDir <- doesDirectoryExist (src </> e)
        if isDir
        then copyTree (src </> e) (dst </> e)
        else copyFile (src </> e) (dst </> e)

buildFixture :: IO Fixture
buildFixture = do
    registerVersion fxVersion
    tmp <- getCanonicalTemporaryDirectory
    root <- createTempDirectory tmp "cw-compare-inprocess"
    let db = root </> "fixture"
        snap = root </> "snapshot-sqlite"
    createDirectoryIfMissing True snap
    ph <- withRocksDb (db </> "rocksDb") modernDefaultOptions $ \rdb -> do
        r <- runResourceT $ mineFixture rdb (db </> "sqlite") snap
        -- SST files, as in a node database, not only a write-ahead log
        flushRocksDb rdb
        return r
    acct <- singleRowAccount (db </> "sqlite" </> chainDbFileName cid0)
    return Fixture
        { _fxRoot = root
        , _fxDb = db
        , _fxSnapshotSqlite = snap
        , _fxPayloadHash = ph
        , _fxAccount = acct
        }

removeFixture :: Fixture -> IO ()
removeFixture = removePathForcibly . _fxRoot

fxPactConfig :: PactServiceConfig
fxPactConfig = testPactServiceConfig
    { _pactResetDb = False
    , _pactPersistIntraBlockWrites = PersistIntraBlockWrites
    }

-- | Mines the fixture chain: 'finalHeight' blocks on each chain, through a
-- 'CutDb' and a Pact service per chain on file-backed SQLite under
-- @sqliteDir@. The SQLite files are copied to @snapDir@ at 'snapshotHeight'.
-- Returns the payload hash of chain 0 at the first Pact 4 transfer height.
--
mineFixture :: RocksDb -> FilePath -> FilePath -> ResourceT IO BlockPayloadHash
mineFixture rdb sqliteDir snapDir = do
    let logger = genericLogger L.Error (\t -> putStrLn ("fixture: " <> T.unpack t))
    (pdb, wbhdb) <- withBlockDbs fxVersion rdb
    perChain <- forM fxChains $ \cid -> do
        q <- liftIO $ newPactQueue 2_000
        mp <- withMempool fxVersion cid q
        bhdb <- liftIO $ getWebBlockHeaderDb wbhdb cid
        mpc <- liftIO $ mkMempoolConsensus mp bhdb (Just pdb)
        (_, sqlenv) <- allocate (startSqliteDb cid logger sqliteDir False) stopSqliteDb
        _ <- allocate
            (async $ runPactService fxVersion cid logger Nothing q (pactMemPoolAccess mpc logger) bhdb pdb sqlenv fxPactConfig)
            cancel
        return (cid, (mp, mkPactExecutionService q))
    let webPact = mkWebPactExecutionService (HM.fromList [ (c, p) | (c, (_, p)) <- perChain ])
        mempools = HM.fromList [ (c, m) | (c, (m, _)) <- perChain ]
    mgr <- liftIO $ HTTP.newManager HTTP.defaultManagerSettings
    headerStore <- liftIO $ newWebBlockHeaderStore mgr wbhdb (logFunction logger)
    payloadStore <- liftIO $ newWebPayloadStore mgr webPact pdb (logFunction logger)
    (_, cutDb) <- allocate
        (startCutDb (defaultCutDbParams fxVersion 3_000_000) (logFunction logger) headerStore payloadStore (cutHashesTable rdb))
        stopCutDb
    liftIO $ do
        c0 <- _cut cutDb
        (_, phs) <- foldM (\(c, acc) h -> do
            when (h == succ snapshotHeight) $ forM_ fxChains $ \cid ->
                vacuumInto (sqliteDir </> chainDbFileName cid) (snapDir </> chainDbFileName cid)
            let transfer = h `elem` (pact4TransferHeights <> pact5TransferHeights)
            foldM (\(c', acc') cid -> do
                when transfer $ do
                    tx <- buildCwCmdNoParse fxVersion (transferCmd cid h)
                    mempoolInsert (mempools HM.! cid) UncheckedInsert (V.singleton tx)
                (c'', pwo) <- mineChain webPact cutDb pdb c' cid
                let txs = _payloadWithOutputsTransactions pwo
                when transfer $ do
                    unless (V.length txs == 1) $ throwIO $ userError $
                        "fixture: expected one transfer at height " <> show h <> " on chain " <> show cid
                    let out = _transactionOutputBytes (snd (V.head txs))
                    unless ("\"success\"" `B.isInfixOf` out) $ throwIO $ userError $
                        "fixture: the transfer failed at height " <> show h <> ": " <> show out
                return (c'', if cid == cid0 then (h, _payloadWithOutputsPayloadHash pwo) : acc' else acc')
                ) (c, acc) fxChains
            ) (c0, []) [1 .. finalHeight]
        case lookup firstTransferHeight phs of
            Just ph -> return ph
            Nothing -> throwIO $ userError "fixture: no payload at the first transfer height"

transferCmd :: ChainId -> BlockHeight -> CmdBuilder
transferCmd cid h = (defaultCmd cid)
    { _cbRPC = mkExec' "(coin.transfer \"sender00\" \"sender01\" 1.0)"
    , _cbSigners =
        [ mkEd25519Signer' sender00
            [ CapToken (QualifiedName "GAS" (ModuleName "coin" Nothing)) []
            , CapToken (QualifiedName "TRANSFER" (ModuleName "coin" Nothing))
                [PString "sender00", PString "sender01", PDecimal 1]
            ]
        ]
    , _cbNonce = Just ("inprocess fixture " <> toText cid <> " " <> T.pack (show h))
    }

-- | Mines one block on a chain on top of a cut and waits until the 'CutDb'
-- has it.
--
mineChain
    :: WebPactExecutionService
    -> CutDb RocksDbTable
    -> PayloadDb RocksDbTable
    -> Cut
    -> ChainId
    -> IO (Cut, PayloadWithOutputs)
mineChain webPact cutDb pdb c cid = do
    parent <- maybe (throwIO $ userError "fixture: no parent") (return . ParentHeader) $ HM.lookup cid (_cutMap c)
    nb <- throwIfNoHistory =<< _webPactNewBlock webPact cid noMiner NewBlockFill parent
    let pwo = newBlockToPayloadWithOutputs nb
    t :: Time Micros <- getCurrentTimeIntegral
    testMineWithPayloadHash (view cutDbWebBlockHeaderDb cutDb) (Nonce 0) t (_payloadWithOutputsPayloadHash pwo) cid c >>= \case
        Left e -> throwIO $ userError $ "fixture: mining failed: " <> show e
        Right (T2 h c') -> do
            addCutHashes cutDb (cutToCutHashes Nothing c')
                { _cutHashesHeaders = HM.singleton (view blockHash h) h
                , _cutHashesPayloads = HM.singleton (view blockPayloadHash h) (payloadWithOutputsToPayloadData pwo)
                }
            let await = do
                    cur <- _cut cutDb
                    unless (_cutHeight cur >= _cutHeight c') $ threadDelay 10_000 >> await
            await
            addNewPayload pdb (view blockHeight h) pwo
            return (c', pwo)

-- | A consistent copy of a live SQLite database.
--
vacuumInto :: FilePath -> FilePath -> IO ()
vacuumInto src dst = withSqlite src $ \db ->
    SQ.exec db ("VACUUM INTO '" <> T.pack dst <> "'")

withSqlite :: FilePath -> (SQ.Database -> IO a) -> IO a
withSqlite path = bracket (SQ.open (T.pack path)) SQ.close

-- | A coin account with a single row, written at genesis and never again.
--
singleRowAccount :: FilePath -> IO T.Text
singleRowAccount path = withSqlite path $ \db -> do
    st <- SQ.prepare db
        "SELECT rowkey FROM [coin_coin-table] GROUP BY rowkey HAVING COUNT(*) = 1 \
        \AND rowkey NOT IN ('sender00', 'sender01') ORDER BY MIN(txid), rowkey LIMIT 1"
    r <- SQ.step st >>= \case
        SQ.Row -> Just <$> SQ.columnText st 0
        SQ.Done -> return Nothing
    SQ.finalize st
    maybe (throwIO $ userError "fixture: no single-row coin account") return r

-- | Changes one byte of the row of a coin account: a digit of its balance.
--
changeBalanceByte :: FilePath -> T.Text -> IO ()
changeBalanceByte path acct = withSqlite path $ \db -> do
    st <- SQ.prepare db "SELECT rowdata FROM [coin_coin-table] WHERE rowkey = ?"
    SQ.bindText st 1 acct
    _ <- SQ.step st
    old <- SQ.columnBlob st 0
    SQ.finalize st
    let (pre, rest) = B.breakSubstring "balance" old
    i <- case B.findIndex (\w -> w >= 48 && w <= 57) rest of
        Nothing -> assertFailure ("no balance digit in the row: " <> show old)
        Just j -> return (B.length pre + j)
    let d = B.index old i
        d' = if d == 57 then 56 else d + 1
        new = B.take i old <> B.singleton d' <> B.drop (i + 1) old
    up <- SQ.prepare db "UPDATE [coin_coin-table] SET rowdata = ? WHERE rowkey = ?"
    SQ.bindBlob up 1 new
    SQ.bindText up 2 acct
    _ <- SQ.step up
    SQ.finalize up

-- -------------------------------------------------------------------------- --
-- Requests

headerReq :: BlockHeight -> Request
headerReq h = Request ClsHeader cid0 (Just h) "GET"
    (apiPrefix <> "/chain/0/header?minheight=" <> hText <> "&maxheight=" <> hText) Nothing
  where
    hText = T.pack (show (fromIntegral h :: Integer))

payloadReq :: BlockPayloadHash -> Request
payloadReq ph = Request ClsPayload cid0 (Just firstTransferHeight) "GET"
    (apiPrefix <> "/chain/0/payload/" <> toText ph) Nothing

-- | A @/local@ request at 'localTarget' for the coin details of an account.
--
localReq :: Int64 -> T.Text -> Request
localReq now acct = Request ClsLocal cid0 (Just localTarget) "POST"
    (apiPrefix <> "/chain/0/pact/api/v1/local?rewindDepth=" <> depthPlaceholder)
    (Just (localBody now ("(coin.details " <> T.pack (show acct) <> ")")))

localBody :: Int64 -> T.Text -> B.ByteString
localBody now code = BL.toStrict (encode command)
  where
    payload = BL.toStrict $ encode $ object
        [ "networkId" .= toText (_versionName fxVersion)
        , "payload" .= object [ "exec" .= object [ "code" .= code, "data" .= object [] ] ]
        , "signers" .= ([] :: [Value])
        , "meta" .= object
            [ "chainId" .= toText cid0
            , "sender" .= ("cw-compare" :: T.Text)
            , "gasLimit" .= (10_000 :: Int)
            , "gasPrice" .= (0.00000001 :: Double)
            , "ttl" .= (3_600 :: Int)
            , "creationTime" .= (now - 60)
            ]
        , "nonce" .= ("inprocess " <> code)
        ]
    command = object
        [ "hash" .= hashToText (hash payload)
        , "sigs" .= ([] :: [Value])
        , "cmd" .= T.decodeUtf8 payload
        ]

nowSeconds :: IO Int64
nowSeconds = do
    Time (TimeSpan us) :: Time Micros <- getCurrentTimeIntegral
    return (fromIntegral us `div` 1_000_000)

coverage :: Coverage
coverage = Coverage (M.fromList [(cid0, [(0, finalHeight)])])

specFor :: FilePath -> InProcessSpec
specFor dir = InProcessSpec
    { _ipDbDir = dir
    , _ipVersion = fxVersion
    , _ipPactChains = fxChains
    , _ipTuning = defaultTuning
    }

-- | POSTs a body to a side and returns the status and the body.
--
post :: HTTP.Manager -> Side -> T.Text -> B.ByteString -> IO (Int, B.ByteString)
post mgr side path body = do
    req <- HTTP.parseRequest (T.unpack (_sideBaseUrl side <> path))
    resp <- HTTP.httpLbs req
        { HTTP.method = "POST"
        , HTTP.requestBody = HTTP.RequestBodyBS body
        , HTTP.requestHeaders = [("Content-Type", "application/json")]
        } mgr
    return (fromEnum (HTTP.responseStatus resp), BL.toStrict (HTTP.responseBody resp))

cutHeightOf :: HTTP.Manager -> Side -> ChainId -> IO (Maybe BlockHeight)
cutHeightOf mgr side cid = do
    req <- HTTP.parseRequest (T.unpack (_sideBaseUrl side <> apiPrefix <> "/cut"))
    resp <- HTTP.httpLbs req mgr
    return $ do
        v <- decode (HTTP.responseBody resp)
        parseMaybe (withObject "cut" $ \o -> o .: "hashes" >>= (.: K.fromText (toText cid)) >>= (.: "height")) v

-- | The tip a side reports for @/local@ at @rewindDepth=0@ on chain 0.
--
localTip :: HTTP.Manager -> Side -> IO (Maybe Pin)
localTip mgr side = do
    now <- nowSeconds
    let (path, body) = tipProbe apiPrefix cid0 now
    (_, b) <- post mgr side path body
    return (answerPin b)

-- | The names and sizes of the files under a directory.
--
listing :: FilePath -> IO [(FilePath, Integer)]
listing dir = do
    es <- sort <$> listDirectory dir
    fmap concat $ forM es $ \e -> do
        isDir <- doesDirectoryExist (dir </> e)
        if isDir
        then map (\(n, s) -> (e </> n, s)) <$> listing (dir </> e)
        else (\s -> [(e, s)]) <$> getFileSize (dir </> e)

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = withResource buildFixture removeFixture $ \getFx -> testGroup "InProcess"
    [ testCase "same DB on both sides: header, payload and /local Same, report Pass" $ do
        fx <- getFx
        a <- copyDb fx "same-a"
        b <- copyDb fx "same-b"
        now <- nowSeconds
        let lreq = localReq now (_fxAccount fx)
            reqs = [headerReq localTarget, payloadReq (_fxPayloadHash fx), lreq]
        mgr <- HTTP.newManager HTTP.defaultManagerSettings
        (rep, (st, body)) <- withInProcessSide quietLogger (specFor a) $ \sa ->
            withInProcessSide quietLogger (specFor b) $ \sb -> (,)
                <$> runCompare quietLogger coverage sa sb reqs
                <*> post mgr sa (fillDepth (fromIntegral (finalHeight - localTarget)) lreq) (maybe "" id (_reqBody lreq))
        reportDiffCount rep @?= 0
        forM_ [ClsHeader, ClsPayload, ClsLocal] $ \cls ->
            assertEqual ("same 2xx for " <> show cls) 1 (reportSame2xx rep cls)
        reportVerdict rep @?= Pass
        -- the /local request reads the account at a Pact 4 height
        st @?= 200
        assertBool ("a successful read of the account: " <> show body) $
            "\"success\"" `B.isInfixOf` body && T.encodeUtf8 (_fxAccount fx) `B.isInfixOf` body

    , testCase "one SQLite rowdata byte changed on the test copy: a /local request at a Pact 4 height Differs" $ do
        fx <- getFx
        a <- copyDb fx "byte-a"
        b <- copyDb fx "byte-b"
        changeBalanceByte (b </> "sqlite" </> chainDbFileName cid0) (_fxAccount fx)
        now <- nowSeconds
        let reqs = [headerReq localTarget, localReq now (_fxAccount fx)]
        rep <- withInProcessSide quietLogger (specFor a) $ \sa ->
            withInProcessSide quietLogger (specFor b) $ \sb ->
                runCompare quietLogger coverage sa sb reqs
        reportSame2xx rep ClsHeader @?= 1
        reportCount rep ClsLocal KindDiffer @?= 1
        reportVerdict rep @?= Fail

    , testCase "/local at rewindDepth 0 answers at the Pact tip when the headers extend past it" $ do
        fx <- getFx
        full <- copyDb fx "tip-full"
        behind <- copyDb fx "tip-behind"
        removePathForcibly (behind </> "sqlite")
        copyTree (_fxSnapshotSqlite fx) (behind </> "sqlite")
        mgr <- HTTP.newManager HTTP.defaultManagerSettings
        (fullTip, fullCut) <- withInProcessSide quietLogger (specFor full) $ \s ->
            (,) <$> localTip mgr s <*> cutHeightOf mgr s cid0
        (behindTip, behindCut) <- withInProcessSide quietLogger (specFor behind) $ \s ->
            (,) <$> localTip mgr s <*> cutHeightOf mgr s cid0
        fullCut @?= Just finalHeight
        behindCut @?= Just finalHeight
        case (fullTip, behindTip) of
            (Just pf, Just pb) -> do
                assertEqual "the Pact tip, not the header tip"
                    (_pinHeight pf - (finalHeight - snapshotHeight)) (_pinHeight pb)
                assertBool "below the headers" (_pinHeight pb < finalHeight)
            _ -> assertFailure $ "no pin: " <> show (fullTip, behindTip)

    , testCase "the source RocksDB is not written" $ do
        fx <- getFx
        a <- copyDb fx "ro-a"
        b <- copyDb fx "ro-b"
        before <- listing (b </> "rocksDb")
        assertBool "the source has SST files" (any ((== ".sst") . takeExtension . fst) before)
        now <- nowSeconds
        let reqs = [headerReq localTarget, payloadReq (_fxPayloadHash fx), localReq now (_fxAccount fx)]
        rep <- withInProcessSide quietLogger (specFor a) $ \sa ->
            withInProcessSide quietLogger (specFor b) $ \sb ->
                runCompare quietLogger coverage sa sb reqs
        reportVerdict rep @?= Pass
        afterRun <- listing (b </> "rocksDb")
        afterRun @?= before
    ]
