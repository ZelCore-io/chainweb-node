{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

-- |
-- Module: Chainweb.StorageTools.Compare.InProcess
--
-- The in-process test side of @cw-compare@: chainweb's service API, served on
-- 127.0.0.1 from the databases of a node, without running a node.
--
-- What is read-only, and how:
--
-- * The source RocksDB is opened with 'withReadOnlyRocksDbTuned'; RocksDB
--   refuses every write to it. Nothing feeds the header or payload stores: no
--   P2P, no mining, a no-op mempool.
--
-- * The cut-hashes table of the 'CutDb' lives in a temporary RocksDB, seeded
--   with the highest cut of the source, and the 'CutDb' is started read-only.
--
-- * The Pact services open the SQLite files under @sqlite@ the way a node
--   does, read-write, but are never asked to validate or create a block; they
--   answer @/local@, @/poll@ and @/spv@ requests. Each Pact chain must have its
--   SQLite file: a missing file is an error, not a new database.
--
module Chainweb.StorageTools.Compare.InProcess
( InProcessSpec(..)
, withInProcessSide

-- * Parts
, resolveDbDir
, inProcessPactConfig
, inProcessCutDbParams
, inProcessRate
) where

import Control.Exception (bracket, throwIO)
import Control.Monad

import Data.Function ((&))
import Data.List (sort)
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import qualified Data.Text as T

import qualified Network.HTTP.Client as HTTP
import qualified Network.Wai.Handler.Warp as Warp

import System.Directory (doesDirectoryExist, doesFileExist)
import System.FilePath ((</>))
import System.LogLevel

-- internal modules

import Chainweb.ChainId
import Chainweb.Chainweb.Configuration
import Chainweb.Cut (unsafeMkCut)
import Chainweb.Cut.CutHashes (CutHashes, cutToCutHashes)
import Chainweb.CutDB
import Chainweb.Logger
import Chainweb.Mempool.Consensus (mkMempoolConsensus)
import Chainweb.Mempool.Mempool (MempoolBackend, noopMempool)
import Chainweb.Pact.Backend.Types (IntraBlockPersistence(..))
import Chainweb.Pact.Backend.Utils (chainDbFileName)
import Chainweb.Pact.RestAPI.Server (PactServerData(..))
import Chainweb.Pact.Service.PactInProcApi (withPactService)
import Chainweb.Pact.Types (PactServiceConfig(..))
import qualified Chainweb.Pact4.Transaction as Pact4
import Chainweb.Payload.PayloadStore (PayloadDb)
import Chainweb.Payload.PayloadStore.RocksDB (newPayloadDb)
import Chainweb.Payload.RestAPI (defaultServicePayloadBatchLimit)
import Chainweb.RestAPI (ChainwebServerDbs(..), HeaderStream(..), serviceApiApplication)
import Chainweb.Storage.Table (Casify, casInsert)
import Chainweb.Storage.Table.RocksDB
import Chainweb.Storage.Table.RocksDB.Tuning (RocksDbTuning)
import Chainweb.StorageTools.Compare.Types (Side(..))
import Chainweb.StorageTools.Http (newLimiter)
import Chainweb.Sync.WebBlockHeaderStore (newWebBlockHeaderStore, newWebPayloadStore)
import Chainweb.Utils (sshow, toText)
import Chainweb.Version
import Chainweb.Version.Guards (maxBlockGasLimit)
import Chainweb.WebBlockHeaderDB (WebBlockHeaderDb, getWebBlockHeaderDb, initWebBlockHeaderDb)
import Chainweb.WebPactExecutionService (PactExecutionService, mkPactExecutionService, mkWebPactExecutionService)

-- -------------------------------------------------------------------------- --
-- Specification

-- | The databases an in-process side serves.
--
data InProcessSpec = InProcessSpec
    { _ipDbDir :: FilePath
        -- ^ the database directory of a node: the directory that holds
        -- @rocksDb@ and @sqlite@, or its parent (see 'resolveDbDir')
    , _ipVersion :: ChainwebVersion
    , _ipPactChains :: [ChainId]
        -- ^ the chains whose Pact service is started, each on its SQLite file
        -- under @sqlite@. The other chains serve headers, payloads and cuts,
        -- but no Pact API.
    , _ipTuning :: RocksDbTuning
        -- ^ the tuning the source RocksDB is opened with
    }

-- | The directory that holds @rocksDb@ and @sqlite@: the given directory, or
-- @0@ under it, the layout of a node's database directory
-- (@\<database-directory\>/0/rocksDb@).
--
resolveDbDir :: FilePath -> IO FilePath
resolveDbDir dir = doesDirectoryExist (dir </> "rocksDb") >>= \case
    True -> return dir
    False -> doesDirectoryExist (dir </> "0" </> "rocksDb") >>= \case
        True -> return (dir </> "0")
        False -> throwIO $ userError $
            "in-process side: no rocksDb directory in " <> dir <> " or in " <> (dir </> "0")

-- -------------------------------------------------------------------------- --
-- Parameters

-- | The permits per second of the limiter of an in-process side. The side is
-- local, so the limiter does not throttle in practice.
--
inProcessRate :: Double
inProcessRate = 100_000

-- | The Pact service configuration of a node with the default configuration
-- of the version, except that the database is never reset and the full
-- history is not required (a node may have compacted its SQLite files).
--
inProcessPactConfig :: ChainwebVersion -> PactServiceConfig
inProcessPactConfig v = PactServiceConfig
    { _pactReorgLimit = _configReorgLimit conf
    , _pactPreInsertCheckTimeout = _configPreInsertCheckTimeout conf
    , _pactQueueSize = _configPactQueueSize conf
    , _pactResetDb = False
    , _pactAllowReadsInLocal = _configAllowReadsInLocal conf
    , _pactUnlimitedInitialRewind = False
    , _pactNewBlockGasLimit = maybe id min maxGasLimit (_configBlockGasLimit conf)
    , _pactLogGas = _configLogGas conf
    , _pactModuleCacheLimit = _configModuleCacheLimit conf
    , _pactEnableLocalTimeout = _configEnableLocalTimeout conf
    , _pactFullHistoryRequired = False
    , _pactPersistIntraBlockWrites =
        if _configFullHistoricPactState conf
        then PersistIntraBlockWrites
        else DoNotPersistIntraBlockWrites
    , _pactTxTimeLimit = Nothing
    }
  where
    conf = defaultChainwebConfiguration v
    maxGasLimit = fromIntegral <$> maxBlockGasLimit v maxBound maxBound

-- | The 'CutDb' parameters of a node, read-only: the 'CutDb' writes nothing,
-- not even to its temporary cut-hashes table.
--
inProcessCutDbParams :: ChainwebVersion -> CutDbParams
inProcessCutDbParams v = (defaultCutDbParams v (_cutFetchTimeout (_configCuts conf)))
    { _cutDbParamsReadOnly = True
    }
  where
    conf = defaultChainwebConfiguration v

-- -------------------------------------------------------------------------- --
-- In-process side

-- | Serves chainweb's service API on 127.0.0.1, on an ephemeral port, from the
-- databases in '_ipDbDir', and runs the computation with a 'Side' for it.
--
withInProcessSide :: Logger l => l -> InProcessSpec -> (Side -> IO a) -> IO a
withInProcessSide logger0 spec act = do
    dir <- resolveDbDir (_ipDbDir spec)
    forM_ pactChains $ \cid -> do
        unless (HS.member cid (chainIds v)) $ throwIO $ userError $
            "in-process side: chain " <> T.unpack (toText cid) <> " is not a chain of " <> show (_versionName v)
        let f = dir </> "sqlite" </> chainDbFileName cid
        doesFileExist f >>= \ok -> unless ok $ throwIO $ userError $
            "in-process side: no SQLite file for chain " <> T.unpack (toText cid) <> ": " <> f
    withReadOnlyRocksDbTuned (dir </> "rocksDb") modernDefaultOptions (_ipTuning spec) $ \src -> do
        -- 'initWebBlockHeaderDb' writes the genesis headers only if they are
        -- missing; a node database has them.
        wbhdb <- initWebBlockHeaderDb src v
        let pdb = newPayloadDb src
        withTempRocksDb "cw-compare-cuts" $ \cutRdb -> do
            let cutTable = cutHashesTable cutRdb
            seedCut v logger wbhdb (cutHashesTable src) cutTable
            mempool <- noopMempool
            withPacts v logger wbhdb pdb mempool (dir </> "sqlite") pactChains $ \pacts -> do
                mgr <- HTTP.newManager HTTP.defaultManagerSettings
                let webPact = mkWebPactExecutionService (HM.fromList pacts)
                headerStore <- newWebBlockHeaderStore mgr wbhdb (logFunction logger)
                payloadStore <- newWebPayloadStore mgr webPact pdb (logFunction logger)
                bracket
                    (startCutDb (inProcessCutDbParams v) (logFunction logger) headerStore payloadStore cutTable)
                    stopCutDb $ \cutDb -> do
                    bhdbs <- forM allChains $ \cid -> (cid,) <$> getWebBlockHeaderDb wbhdb cid
                    let dbs = ChainwebServerDbs
                            { _chainwebServerCutDb = Just cutDb
                            , _chainwebServerBlockHeaderDbs = bhdbs
                            , _chainwebServerMempools = [ (cid, mempool) | cid <- allChains ]
                            , _chainwebServerPayloadDbs = [ (cid, pdb) | cid <- allChains ]
                            , _chainwebServerPeerDbs = []
                            }
                        pactData =
                            [ (cid, PactServerData cutDb mempool logger pex) | (cid, pex) <- pacts ]
                        app = serviceApiApplication v dbs pactData Nothing
                            (HeaderStream False) Nothing defaultServicePayloadBatchLimit
                    Warp.withApplicationSettings warpSettings (return app) $ \port -> do
                        lim <- newLimiter inProcessRate Nothing
                        act Side
                            { _sideName = "test (in-process)"
                            , _sideBaseUrl = "http://127.0.0.1:" <> sshow port
                            , _sideLimiter = lim
                            }
  where
    v = _ipVersion spec
    logger = addLabel ("component", "cw-compare-in-process") logger0
    allChains = sort (HS.toList (chainIds v))
    pactChains = HS.toList (HS.fromList (_ipPactChains spec))

    warpSettings = Warp.defaultSettings
        & Warp.setServerName "Chainweb Service API"
        & Warp.setOnException (\_ e -> when (Warp.defaultShouldDisplayException e) $
            logFunctionText logger Debug ("in-process side: " <> sshow e))

-- | Copies the highest cut of the source into the cut-hashes table of the
-- 'CutDb', the cut a node would start with ('readHighestCutHeaders'').
--
seedCut
    :: Logger l
    => ChainwebVersion
    -> l
    -> WebBlockHeaderDb
    -> Casify RocksDbTable CutHashes
    -> Casify RocksDbTable CutHashes
    -> IO ()
seedCut v logger wbhdb src dst = do
    hm <- readHighestCutHeaders' v (logFunctionText logger) wbhdb src
    casInsert dst (cutToCutHashes Nothing (unsafeMkCut v hm))

-- | Starts a Pact service on each chain, on its SQLite file, the way a node
-- does ('withPactService').
--
withPacts
    :: Logger l
    => ChainwebVersion
    -> l
    -> WebBlockHeaderDb
    -> PayloadDb RocksDbTable
    -> MempoolBackend Pact4.UnparsedTransaction
    -> FilePath
    -> [ChainId]
    -> ([(ChainId, PactExecutionService)] -> IO a)
    -> IO a
withPacts v logger wbhdb pdb mempool sqliteDir cids0 k = go cids0 []
  where
    go [] acc = k (reverse acc)
    go (cid : rest) acc = do
        bhdb <- getWebBlockHeaderDb wbhdb cid
        mpc <- mkMempoolConsensus mempool bhdb (Just pdb)
        let l = addLabel ("chain", toText cid) logger
        withPactService v cid l Nothing mpc bhdb pdb sqliteDir (inProcessPactConfig v) $ \q ->
            go rest ((cid, mkPactExecutionService q) : acc)
