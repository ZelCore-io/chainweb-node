{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Test.Utils.Storage
-- License: MIT
-- Stability: experimental
--
-- Storage options for the tests, from the environment variable
-- @CHAINWEB_TEST_STORAGE@, a comma separated list of:
--
-- * @no-merkle-trees@: payload stores do not write the Merkle trees.
-- * @zstd-dict@: RocksDB instances are opened with the tuning of
--   @--payload-compression zstd-dict@.
-- * @pact-split@: the Pact history store is @split@.
-- * @pact-shadow@: the Pact history store is @shadow@.
-- * @freeze-depth=N@: the freeze depth is @N@ (at least 1).
--
-- Unset or empty, the tests use the defaults of chainweb-node 3.2.1. An
-- unknown item is an error, so that a typo does not silently test the
-- defaults.
--
-- Where the Pact history store reaches:
--
-- * the nodes of "Chainweb.Test.Utils" and "Chainweb.Test.MultiNode"
--   ('withTestPactStorage');
-- * the Pact services of "Chainweb.Test.Pact5.Utils" ('withRunPactService')
--   and of the Pact 5 @PactServiceTest@ and @TransactionExecTest@ suites, the
--   queued Pact service of "Chainweb.Test.Pact4.Utils"
--   ('withPactTestBlockDb') and its queue-less web Pact services
--   ('withWebPactExecutionService', 'withWebPactExecutionServiceCompaction')
--   ('testChainStorageResource', 'acquireTestChainStorage',
--   'withTestChainStorage').
--
-- A split or shadow layout needs a directory (the hot file and the cold
-- store), so in split and shadow these use a fresh temporary directory per
-- chain in place of the temporary or in-memory SQLite file of the default.
-- In shadow, a chain whose split side broke while the test ran fails the
-- test at its end ('checkShadowNotBroken'), so that a shadow run cannot pass
-- on the legacy store alone.
--
-- These stay on a single SQLite connection in every mode, because the test
-- owns that connection or builds the checkpointer itself:
-- 'Chainweb.Test.Pact4.Utils.withPactTestBlockDb'' and
-- 'Chainweb.Test.Pact4.Utils.withPactCtxSQLite' (the connection comes from
-- the caller), the Pact 4 and Pact 5 checkpointer suites, @ModuleCacheOnRestart@,
-- @Pact4.SQLite@, and the compaction and twice-replay cases that open
-- 'Chainweb.Pact.Backend.Utils.withSqliteDb' directly.
--
module Chainweb.Test.Utils.Storage
( TestStorageOption(..)
, testStorageOptions
, parseTestStorageOptions
, testPayloadDbConfig
, testStorageTuning
, testPactHistoryConfig
, testPactHistoryConfigFor
, withTestPactStorage
, testSplitStorage
, acquireTestChainStorage
, acquireSplitChainStorageFor
, testChainStorageResource
, withTestChainStorage
, testStorageLogger
, checkShadowNotBroken
) where

import Control.Concurrent.Async (async, cancel, race, wait, waitCatch)
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad (when)
import Control.Monad.IO.Class (liftIO)
import Data.IORef
import Control.Monad.Trans.Resource (ResourceT, allocate)
import System.Directory (removeDirectoryRecursive)
import System.FilePath ((</>))
import System.IO.Temp (createTempDirectory, getCanonicalTemporaryDirectory, withSystemTempDirectory)

import qualified Data.Text as T
import Data.Word (Word64)

import System.Environment (lookupEnv)

import Text.Read (readMaybe)

-- internal modules

import Chainweb.Logger (GenericLogger, genericLogger)
import Chainweb.Pact.Backend.Store.Node
import Chainweb.Pact.Backend.Store.Shadow (ShadowEnv(..))
import Chainweb.ChainId (ChainId)
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Version (ChainwebVersion)
import System.LogLevel (LogLevel(..))

import Chainweb.BlockHeight (BlockHeight(..))
import Chainweb.Chainweb.Configuration (PayloadCompression(..), payloadCompressionTuning)
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Payload.PayloadStore
import Chainweb.Storage.Table.RocksDB.Tuning (RocksDbTuning)

-- -------------------------------------------------------------------------- --
-- Test Storage Options

data TestStorageOption
    = TestNoMerkleTrees
    | TestZstdDict
    | TestPactSplit
    | TestPactShadow
    | TestFreezeDepth BlockHeight
    deriving (Show, Eq)

testStorageOptions :: IO [TestStorageOption]
testStorageOptions = lookupEnv "CHAINWEB_TEST_STORAGE" >>= \case
    Nothing -> return []
    Just s -> either (ioError . userError) return
        $ parseTestStorageOptions (T.pack s)

-- | Parses the value of @CHAINWEB_TEST_STORAGE@. An unknown item, a freeze
-- depth that is not a whole number of at least 1, and both @pact-split@ and
-- @pact-shadow@ are errors.
--
parseTestStorageOptions :: T.Text -> Either String [TestStorageOption]
parseTestStorageOptions s = do
    opts <- traverse parseItem
        $ filter (not . T.null)
        $ T.strip <$> T.splitOn "," s
    if TestPactSplit `elem` opts && TestPactShadow `elem` opts
    then Left "CHAINWEB_TEST_STORAGE: pact-split and pact-shadow exclude each other"
    else return opts
  where
    parseItem = \case
        "no-merkle-trees" -> Right TestNoMerkleTrees
        "zstd-dict" -> Right TestZstdDict
        "pact-split" -> Right TestPactSplit
        "pact-shadow" -> Right TestPactShadow
        x | Just n <- T.stripPrefix "freeze-depth=" x -> case readMaybe @Word64 (T.unpack n) of
            Just d | d >= 1 -> Right (TestFreezeDepth (BlockHeight d))
            _ -> Left $ "CHAINWEB_TEST_STORAGE: freeze-depth must be a whole number of at least 1, got "
                <> show n
        x -> Left $ "CHAINWEB_TEST_STORAGE: unknown item "
            <> show x <> "; known items: no-merkle-trees, zstd-dict, pact-split, pact-shadow, freeze-depth=N"

-- | The payload store configuration for the tests.
--
testPayloadDbConfig :: IO PayloadDbConfig
testPayloadDbConfig = do
    opts <- testStorageOptions
    return $ PayloadDbConfig
        { _payloadDbConfigStoreTrees = if TestNoMerkleTrees `elem` opts
            then DoNotStoreMerkleTrees
            else StoreMerkleTrees
        }

-- | The RocksDB tuning for the tests, to be applied before a database is
-- opened: that of @--payload-compression zstd-dict@ with @zstd-dict@,
-- otherwise that of the default @snappy@, which changes no option.
--
testStorageTuning :: IO RocksDbTuning
testStorageTuning = do
    opts <- testStorageOptions
    return $ payloadCompressionTuning $ if TestZstdDict `elem` opts
        then PayloadZstdDict
        else PayloadSnappy

-- | The Pact history configuration for the tests: 'defaultPactHistoryConfig'
-- with the store and the freeze depth of @CHAINWEB_TEST_STORAGE@.
--
testPactHistoryConfig :: IO PactHistoryConfig
testPactHistoryConfig = testPactHistoryConfigFor <$> testStorageOptions

testPactHistoryConfigFor :: [TestStorageOption] -> PactHistoryConfig
testPactHistoryConfigFor = foldl apply defaultPactHistoryConfig
  where
    apply c = \case
        TestPactSplit -> c { _phStore = HistorySplit }
        TestPactShadow -> c { _phStore = HistoryShadow }
        TestFreezeDepth d -> c { _phFreezeDepth = d }
        _ -> c

-- | The Pact history storage of a test node on the directory @dir@: with
-- @pact-split@ or @pact-shadow@, @dir@ is the root (@<db-dir>/0@) of a split
-- or shadow layout (its cold store in @dir/pactCold@), with a 100 ms freezer
-- poll and freeze steps of one height; otherwise the SQLite files of
-- chainweb-node 3.2.1 directly in @dir@ ('sqliteOnlyStorage').
--
withTestPactStorage :: ChainwebVersion -> FilePath -> (PactStorage -> IO a) -> IO a
withTestPactStorage v dir k = do
    cfg0 <- testPactHistoryConfig
    withTestPactStorageFor cfg0 v dir k

-- | 'withTestPactStorage' with the given configuration in place of that of
-- @CHAINWEB_TEST_STORAGE@.
withTestPactStorageFor :: PactHistoryConfig -> ChainwebVersion -> FilePath -> (PactStorage -> IO a) -> IO a
withTestPactStorageFor cfg0 v dir k =
    case _phStore cfg0 of
        HistorySqlite -> sqliteOnlyStorage dir >>= k
        _ -> withPactStorage (genericLogger Error (\_ -> return ())) v
            cfg0 { _phFreezeStepHeights = 1 } dir False
            $ \st -> k st { _pstFreezerPollMicros = 100_000 }

-- | Whether @CHAINWEB_TEST_STORAGE@ selects @pact-split@ or @pact-shadow@.
testSplitStorage :: IO Bool
testSplitStorage = (/= HistorySqlite) . _phStore <$> testPactHistoryConfig

-- | The storage of one chain of a test Pact service and its release action.
-- In sqlite mode (the default): the connection that @openSqlite@ opens, in
-- sqlite mode ('sqliteChainStorage'), released with @closeSqlite@, as before.
-- In split and shadow: a fresh temporary directory with a split or shadow
-- layout ('withTestPactStorage', 'withChainStorage'), removed on release.
--
acquireTestChainStorage
    :: ChainwebVersion
    -> ChainId
    -> IO SQLiteEnv
    -> (SQLiteEnv -> IO ())
    -> IO (ChainStorage, IO ())
acquireTestChainStorage v cid openSqlite closeSqlite = testSplitStorage >>= \case
    False -> do
        sql <- openSqlite
        st <- sqliteChainStorage sql `onException` closeSqlite sql
        return (st, closeSqlite sql)
    True -> acquireSplitChainStorage v cid

acquireSplitChainStorage :: ChainwebVersion -> ChainId -> IO (ChainStorage, IO ())
acquireSplitChainStorage v cid = do
    cfg <- testPactHistoryConfig
    acquireSplitChainStorageFor cfg (return ()) v cid

-- | The split or shadow storage of 'acquireTestChainStorage' with the given
-- configuration. @onClosed@ runs once the chain's storage is closed. The
-- release action closes the storage and removes the directory also when
-- 'checkShadowNotBroken' fails, and then rethrows that failure.
acquireSplitChainStorageFor
    :: PactHistoryConfig
    -> IO ()
    -> ChainwebVersion
    -> ChainId
    -> IO (ChainStorage, IO ())
acquireSplitChainStorageFor cfg onClosed v cid = do
    tmp <- getCanonicalTemporaryDirectory
    dir <- createTempDirectory tmp "pact-storage"
    logs <- newIORef []
    (st, release) <- acquireCps (\k -> (withTestPactStorageFor cfg v (dir </> "0") $ \ps ->
            withChainStorage (testStorageLogger logs) ps cid False k) >> onClosed)
        `onException` removeDirectoryRecursive dir
    return (st, (checkShadowNotBroken logs st `finally` release) `finally` removeDirectoryRecursive dir)

-- | 'acquireTestChainStorage' as a resource. In sqlite mode the storage is
-- on the given SQLite resource, as before.
--
testChainStorageResource :: ChainwebVersion -> ChainId -> ResourceT IO SQLiteEnv -> ResourceT IO ChainStorage
testChainStorageResource v cid sqliteRes = liftIO testSplitStorage >>= \case
    False -> sqliteRes >>= liftIO . sqliteChainStorage
    True -> fst . snd <$> allocate (acquireSplitChainStorage v cid) snd

-- | 'acquireTestChainStorage' in bracket style. In sqlite mode the storage is
-- on the connection of @withSqlite@, as before.
--
withTestChainStorage
    :: ChainwebVersion
    -> ChainId
    -> ((SQLiteEnv -> IO b) -> IO b)
    -> (ChainStorage -> IO b)
    -> IO b
withTestChainStorage v cid withSqlite k = testSplitStorage >>= \case
    False -> withSqlite (\sql -> sqliteChainStorage sql >>= k)
    True -> withSystemTempDirectory "pact-storage" $ \dir -> do
        logs <- newIORef []
        withTestPactStorage v (dir </> "0") $ \ps ->
            withChainStorage (testStorageLogger logs) ps cid False $ \cst -> do
                r <- k cst
                checkShadowNotBroken logs cst
                return r

-- | The logger of a test chain's storage: keeps the records at 'Warn' and
-- above, for 'checkShadowNotBroken'.
testStorageLogger :: IORef [T.Text] -> GenericLogger
testStorageLogger logs = genericLogger Warn (\t -> atomicModifyIORef' logs (\ls -> (t : ls, ())))

-- | In shadow mode, fails when the split side of the chain broke while the
-- test ran. A broken split side makes the shadow store run on the legacy
-- store alone, so the test would pass without exercising the split store;
-- the failure carries the storage's log records (@ShadowBroken@ and the
-- differences that led to it).
checkShadowNotBroken :: IORef [T.Text] -> ChainStorage -> IO ()
checkShadowNotBroken logs cst = case _cstStore cst of
    ChainStoreShadow env -> do
        broken <- readIORef (_shBroken env)
        when broken $ do
            ls <- reverse <$> readIORef logs
            throwIO $ userError $ T.unpack $ T.unlines $
                "pact-shadow: the split side of the chain broke during the test, so the test ran on the legacy store alone. Storage log:"
                : take 20 ls
    _ -> return ()

-- | Holds a bracket open on a thread of its own until the release action
-- runs, for resources that are acquired and released separately
-- ('withResource', 'allocate').
--
acquireCps :: ((a -> IO ()) -> IO ()) -> IO (a, IO ())
acquireCps with = do
    got <- newEmptyMVar
    done <- newEmptyMVar
    t <- async $ with $ \x -> putMVar got x >> takeMVar done
    r <- race (waitCatch t) (takeMVar got) `onException` cancel t
    case r of
        Right x -> return (x, putMVar done () >> wait t)
        Left (Left e) -> throwIO e
        Left (Right ()) -> throwIO (userError "acquireCps: the bracket returned without a value")
