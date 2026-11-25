{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.Test.Pact.Store.SplitNode
-- License: MIT
-- Stability: experimental
--
-- The node wiring of the Pact history store (Task 27): 'withPactStorage',
-- 'withChainStorage', the freezer start and drain, and the refusals at start.
--
module Chainweb.Test.Pact.Store.SplitNode
( tests
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Exception
import Control.Lens ((&), (.~))
import Control.Monad

import Data.IORef
import System.LogLevel (LogLevel(..))
import Data.Maybe (isJust, isNothing)
import qualified Data.Text as T

import System.Directory
import System.FilePath ((<.>), (</>))
import System.IO.Temp (withSystemTempDirectory)

import Test.Tasty
import Test.Tasty.HUnit

-- internal modules

import Chainweb.Backup
import Chainweb.Logger (genericLogger)
import Chainweb.BlockHeader.Internal
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Pact.Backend.Store.Layout
import Chainweb.Pact.Backend.Store.Node
import Chainweb.Pact.Backend.Store.Shadow (ShadowEnv(..))
import Chainweb.Pact.Backend.Store.SQLite (sqlitePactStore, withSqliteErrorDetail)
import Chainweb.Pact.Backend.Utils (withSqliteDb)
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Storage.Table.RocksDB (withTempRocksDb)
import Chainweb.Test.Pact.Store.Freezer hiding (tests)
import Chainweb.Test.TestVersions (slowForkingCpmTestVersion)
import Chainweb.Test.Utils.Storage (checkShadowNotBroken, testStorageLogger)
import Chainweb.Test.Utils (petersen)
import Chainweb.Version (ChainwebVersion)
import Chainweb.Version.Testnet04 (testnet04)

import qualified Pact.Types.SQLite as Pact4

tests :: TestTree
tests = testGroup "Split store node"
    [ testCase "default mode: no pactCold directory, no freezer thread, cpStore answers as sqlitePactStore" defaultModeTest
    , testCase "split on testnet04 refuses at start, naming the version; nothing is created on disk" testnetTest
    , testCase "split start refuses when the frontier block is missing from BlockHistory, naming height and hash" frontierMissingTest
    , testCase "split start on a block header database older than the frontier refuses, naming frontier and target" olderBlockDbTest
    , testCase "--pact-history-store=sqlite on a trimmed sqlite-split refuses; state B starts after removing the marker" sqliteOnTrimmedTest
    , testCase "backup API and --reset-chain-databases refused in split and shadow" refusalsTest
    , testCase "only-sync-pact in split: freezers drained before Replayed" drainTest
    , testCase "shadow: a broken chain is persisted; a restart runs it on legacy alone, without a freezer" shadowBrokenTest
    , testCase "shadow: a failed write of the broken flag is logged at Error" shadowBrokenWriteFailsTest
    , testCase "restart at freeze depth 3: the freezer waits for the startup rewind, so a recent rewind is not refused" freezerAfterRewindTest
    , testCase "split start on a fresh directory: the hot file has no schema yet; the chain starts and the store runs" freshSplitTest
    , testCase "shadow start on a fresh directory: the split side does not break" freshShadowTest
    , testCase "a failed SQLite statement names SQLite's message, not only the error code" errorDetailTest
    , testCase "test storage: a shadow chain whose split side broke fails the test, with the storage log" shadowBrokenFailsTestTest
    ]

v :: ChainwebVersion
v = slowForkingCpmTestVersion petersen

parentAt :: Int -> ParentHeader
parentAt h = ParentHeader $ genesisBlockHeader v cid
    & blockHash .~ mkHash h 0
    & blockHeight .~ fromIntegral h

isStore :: ChainStore -> String
isStore = \case
    ChainStoreSqlite -> "sqlite"
    ChainStoreSplit _ -> "split"
    ChainStoreShadow _ -> "shadow"

-- | Runs a chain of the storage with its freezer started (as the Pact
-- service does once the checkpointer exists).
withChain :: PactStorage -> (ChainStorage -> IO a) -> IO a
withChain st k = withChainStorage quietLogger st cid False $ \cst -> do
    lock <- newMVar ()
    withChainFreezer cst (\io -> withMVar lock (const io)) (return False) (return Nothing) (\start -> start >> k cst)

defaultModeTest :: Assertion
defaultModeTest = withSystemTempDirectory "split-node" $ \tmp -> do
    let root = tmp </> "0"
    withPactStorage quietLogger v defaultPactHistoryConfig root False $ \st -> do
        assertBool "no cold store" (isNothing (_pstCold st))
        withChain st $ \cst -> do
            isStore (_cstStore cst) @?= "sqlite"
            assertBool "no freezer" (isNothing (_cstFreezer cst))
            drainChainFreezer cst >>= (@?= False)
            let store = chainStorePactStore (_cstSql cst) (_cstStore cst)
                plain = sqlitePactStore (_cstSql cst)
            _psInitSchema store
            commitAll (_cstSql cst) (block <$> [0 .. 3])
            join $ (@?=) <$> _psGetLatestBlock store <*> _psGetLatestBlock plain
            join $ (@?=) <$> _psReadNewest store "coin_table" "alice" Nothing <*> _psReadNewest plain "coin_table" "alice" Nothing
            _psGetLatestBlock store >>= (@?= Just (3, mkHash 3 0))
    doesDirectoryExist (root </> coldDirName) >>= assertBool "no pactCold" . not
    doesDirectoryExist (root </> splitDirName) >>= assertBool "no sqlite-split" . not
    -- the tools' storage is the same store
    st <- sqliteOnlyStorage (root </> sqliteDirName)
    assertBool "sqliteOnlyStorage: no cold store" (isNothing (_pstCold st))
    _phStore (_pstHistory st) @?= HistorySqlite

testnetTest :: Assertion
testnetTest = withSystemTempDirectory "split-node" $ \tmp -> do
    let root = tmp </> "0"
    forM_ [HistorySplit, HistoryShadow] $ \m ->
        try (withPactStorage quietLogger testnet04 defaultPactHistoryConfig { _phStore = m } root False (\_ -> return ())) >>= \case
            Left (HistoryStoreRefused t) -> assertBool ("names the version: " <> T.unpack t) ("testnet04" `T.isInfixOf` t)
            Left e -> assertFailure ("unexpected " <> show e)
            Right () -> assertFailure "expected a refusal"
    listDirectory tmp >>= (@?= [])

-- | A fixture frozen to the end (frontier 8), then the SQLite file rewound to
-- height 5 behind the split store's back: the frontier block is gone from
-- @BlockHistory@.
frontierMissingTest :: Assertion
frontierMissingTest = withFixture $ \root _twin -> do
    withPlainNode root $ \n -> do
        void $ runToEnd (_nEnv n)
        void $ _psRewindDbTo (sqlitePactStore (_nPact n)) (Just (parentAt 5))
    withPactStorage quietLogger v testConfig root False $ \st ->
        try (withChainStorage quietLogger st cid False (\_ -> return ())) >>= \case
            Left (SplitStartRefused t) -> do
                assertBool ("names BlockHistory: " <> T.unpack t) ("BlockHistory" `T.isInfixOf` t)
                assertBool ("names the height: " <> T.unpack t) ("height 8" `T.isInfixOf` t)
                assertBool ("names the hash: " <> T.unpack t) (T.pack (show (mkHash 8 0)) `T.isInfixOf` t)
            Left e -> assertFailure ("unexpected " <> show e)
            Right () -> assertFailure "expected a refusal"

-- | The startup rewind to a block below the frontier (the block header
-- database is older than the frontier) is refused by the chain's store with
-- 'RewindBelowFrontier', whose message names the frontier and the target;
-- the Pact service stops the node with it ('exitOnRewindBelowFrontier').
olderBlockDbTest :: Assertion
olderBlockDbTest = withFixture $ \root _twin -> do
    withPlainNode root $ \n -> void $ runToEnd (_nEnv n)
    withPactStorage quietLogger v testConfig root False $ \st ->
        withChainStorage quietLogger st cid False $ \cst -> do
            isStore (_cstStore cst) @?= "split"
            let store = chainStorePactStore (_cstSql cst) (_cstStore cst)
            try (_psRewindDbTo store (Just (parentAt 3))) >>= \case
                Left (e :: SomeException) -> do
                    let t = T.pack (displayException e)
                    assertBool ("names the frontier: " <> T.unpack t) ("frontier at height 8" `T.isInfixOf` t)
                    assertBool ("names the target: " <> T.unpack t) ("to end txid 40" `T.isInfixOf` t)
                Right _ -> assertFailure "expected a refusal"
            _psGetLatestBlock store >>= (@?= Just (fromIntegral tipHeight, mkHash tipHeight 0))

sqliteOnTrimmedTest :: Assertion
sqliteOnTrimmedTest = do
    withFixture $ \root _twin -> do
        withPlainNode root $ \n -> void $ runToEnd (_nEnv n)
        try (withPactStorage quietLogger v defaultPactHistoryConfig root False (\_ -> return ())) >>= \case
            Left (TrimmedDirInSqliteMode d) -> d @?= root </> splitDirName
            Left e -> assertFailure ("unexpected " <> show e)
            Right () -> assertFailure "expected a refusal"
    -- state B: sqlite with a marker, nothing trimmed
    withFixture $ \root _twin -> do
        void $ writeSplitMarker (root </> sqliteDirName) cid 3
        withPactStorage quietLogger v defaultPactHistoryConfig root False $ \st ->
            withChain st $ \cst ->
                _psGetLatestBlock (chainStorePactStore (_cstSql cst) (_cstStore cst))
                    >>= (@?= Just (fromIntegral tipHeight, mkHash tipHeight 0))
        readSplitMarker (root </> sqliteDirName) >>= assertBool "the marker is removed" . isNothing

refusalsTest :: Assertion
refusalsTest = forM_ [HistorySplit, HistoryShadow] $ \m -> withFixture $ \root _twin -> do
    try (withPactStorage quietLogger v testConfig { _phStore = m } root True (\_ -> return ())) >>= \case
        Left (ResetRefused m') -> m' @?= m
        Left e -> assertFailure ("unexpected " <> show e)
        Right () -> assertFailure "expected ResetRefused"
    try (refuseReset testConfig { _phStore = m } True) >>= (@?= Left (ResetRefused m))
    doesFileExist (root </> sqliteDirName </> "pact-v1-chain-0.sqlite") >>= assertBool "nothing removed"
    withTempRocksDb "split-node-backup" $ \rdb -> withSystemTempDirectory "backup" $ \bdir -> do
        let env = BackupEnv
                { _backupRocksDb = rdb
                , _backupDir = bdir
                , _backupPactDbDir = root </> sqliteDirName
                , _backupPactHistory = m
                , _backupChainIds = mempty
                , _backupLogger = quietLogger
                }
        try (makeBackup env (BackupOptions "b1" True)) >>= (@?= Left (BackupRefused m))
        listDirectory bdir >>= (@?= [])

-- | The drain that @--only-sync-pact@ runs on every synced chain before it
-- returns 'Replayed': it stops the freezer thread and freezes to the end.
drainTest :: Assertion
drainTest = withFixture $ \root _twin -> do
    let cfg = testConfig
    withPactStorage quietLogger v cfg root False $ \st -> do
        assertBool "a cold store" (isJust (_pstCold st))
        withChain st { _pstFreezerPollMicros = 3_600_000_000 } $ \cst -> do
            isStore (_cstStore cst) @?= "split"
            Just cf <- return (_cstFreezer cst)
            drainChainFreezer cst >>= (@?= True)
            _ccVerified (_cfCold cf) >>= (@?= Just finalFrontier)
    -- after the chain stopped, no drain is left
    withPactStorage quietLogger v cfg { _phFreezerEnabled = False } root False $ \st ->
        withChain st $ \cst -> drainChainFreezer cst >>= (@?= False)

shadowBrokenTest :: Assertion
shadowBrokenTest = withFixture $ \root _twin -> do
    let cfg = testConfig { _phStore = HistoryShadow }
        brokenFile = root </> splitDirName </> shadowBrokenFileName cid
    withPactStorage quietLogger v cfg root False $ \st -> withChain st $ \cst -> case _cstStore cst of
        ChainStoreShadow env -> do
            assertBool "a freezer" (isJust (_cstFreezer cst))
            -- what the shadow store runs on the first split-side failure
            _shOnBroken env
            doesFileExist brokenFile >>= assertBool "persisted"
        _ -> assertFailure "expected shadow"
    withPactStorage quietLogger v cfg root False $ \st -> withChain st $ \cst -> case _cstStore cst of
        ChainStoreShadow env -> do
            readIORef (_shBroken env) >>= assertBool "starts broken"
            assertBool "no freezer" (isNothing (_cstFreezer cst))
            -- legacy answers alone
            _psGetLatestBlock (chainStorePactStore (_cstSql cst) (_cstStore cst))
                >>= (@?= Just (fromIntegral tipHeight, mkHash tipHeight 0))
        _ -> assertFailure "expected shadow"

-- | The broken flag cannot be written (a directory is in the way of its
-- temporary file): the write is logged at Error and the chain goes on.
shadowBrokenWriteFailsTest :: Assertion
shadowBrokenWriteFailsTest = withFixture $ \root _twin -> do
    let cfg = testConfig { _phStore = HistoryShadow }
        brokenFile = root </> splitDirName </> shadowBrokenFileName cid
    logs <- newIORef []
    let logger = genericLogger Warn (\t -> modifyIORef' logs (t :))
    withPactStorage quietLogger v cfg root False $ \st ->
        withChainStorage logger st cid False $ \cst -> case _cstStore cst of
            ChainStoreShadow env -> do
                createDirectoryIfMissing True (brokenFile <.> "tmp")
                _shOnBroken env
                doesFileExist brokenFile >>= assertBool "not persisted" . not
                ls <- readIORef logs
                assertBool ("logged at Error: " <> show ls) $ any
                    (\t -> "could not write the broken flag" `T.isInfixOf` t && "[Error]" `T.isInfixOf` t) ls
            _ -> assertFailure "expected shadow"

-- | A restart at freeze depth 3 whose startup rewind goes back to a recent
-- block (height 6 of tip 12). A freezer that ran from the stale SQLite tip
-- before the rewind would advance the frontier to height 8 and the rewind
-- would be refused ('RewindBelowFrontier'). The freezer waits for the start
-- action, which the Pact service runs after 'initializeLatestBlock'.
freezerAfterRewindTest :: Assertion
freezerAfterRewindTest = withFixture $ \root _twin -> do
    let cfg = testConfig { _phFreezeDepth = 3 }
    withPactStorage quietLogger v cfg root False $ \st ->
        withChainStorage quietLogger st { _pstFreezerPollMicros = 10_000 } cid False $ \cst -> do
            lock <- newMVar ()
            Just cf <- return (_cstFreezer cst)
            withChainFreezer cst (\io -> withMVar lock (const io)) (return False) (return Nothing) $ \start -> do
                -- the startup: the freezer would have taken many polls by now
                threadDelay 500_000
                _ccWritten (_cfCold cf) >>= (@?= Nothing)
                let store = chainStorePactStore (_cstSql cst) (_cstStore cst)
                void $ _psRewindDbTo store (Just (parentAt 6))
                _psGetLatestBlock store >>= (@?= Just (6, mkHash 6 0))
                -- after the rewind the freezer runs, below the new tip
                start
                drainChainFreezer cst >>= (@?= True)
                _ccVerified (_cfCold cf) >>= \f -> assertBool ("frontier below the tip: " <> show f)
                    (maybe False ((<= 3) . _frHeight) f)

-- | A split node on a fresh directory, as every Pact service test under
-- @CHAINWEB_TEST_STORAGE=pact-split@ starts: the hot file is new, so it has
-- no @BlockHistory@ until the checkpointer creates the schema. The start
-- checks run before that and must read such a file as empty (they failed
-- with @Database error: ErrorError@, SQLite's "no such table").
freshSplitTest :: Assertion
freshSplitTest = withSystemTempDirectory "split-node" $ \tmp -> do
    let root = tmp </> "0"
    withPactStorage quietLogger v testConfig root False $ \st ->
        withChainStorage quietLogger st cid False $ \cst -> do
            isStore (_cstStore cst) @?= "split"
            let store = chainStorePactStore (_cstSql cst) (_cstStore cst)
            _psInitSchema store
            _psGetLatestBlock store >>= (@?= Nothing)
            forM_ (block <$> [0 .. 3]) $ \b ->
                _psCommitBlock store b >>= either (assertFailure . show) return
            _psGetLatestBlock store >>= (@?= Just (3, mkHash 3 0))

-- | The same fresh start in shadow mode: the split side must not break, so
-- that a shadow run compares the split store rather than the legacy one
-- alone.
freshShadowTest :: Assertion
freshShadowTest = withSystemTempDirectory "split-node" $ \tmp -> do
    let root = tmp </> "0"
    logs <- newIORef []
    let logger = genericLogger Warn (\t -> modifyIORef' logs (t :))
    withPactStorage quietLogger v testConfig { _phStore = HistoryShadow } root False $ \st ->
        withChainStorage logger st cid False $ \cst -> case _cstStore cst of
            ChainStoreShadow env -> do
                let store = chainStorePactStore (_cstSql cst) (_cstStore cst)
                _psInitSchema store
                forM_ (block <$> [0 .. 3]) $ \b ->
                    _psCommitBlock store b >>= either (assertFailure . show) return
                _psGetLatestBlock store >>= (@?= Just (3, mkHash 3 0))
                ls <- readIORef logs
                readIORef (_shBroken env) >>= assertBool ("split side not broken: " <> show ls) . not
                assertBool ("no ShadowBroken record: " <> show ls) $
                    not (any ("ShadowBroken" `T.isInfixOf`) ls)
            _ -> assertFailure "expected shadow"

-- | The Pact 4 SQLite helpers raise @Database error: ErrorError@ and drop
-- SQLite's message; 'withSqliteErrorDetail' puts it back.
errorDetailTest :: Assertion
errorDetailTest = withSystemTempDirectory "split-node" $ \tmp ->
    withSqliteDb cid quietLogger tmp False $ \db ->
        try (withSqliteErrorDetail db (Pact4.qry_ db "SELECT x FROM NoSuchTable" [Pact4.RInt])) >>= \case
            Left (e :: IOException) -> do
                let t = displayException e
                assertBool ("keeps the code: " <> t) ("Database error: ErrorError" `T.isInfixOf` T.pack t)
                assertBool ("names SQLite's message: " <> t) ("no such table: NoSuchTable" `T.isInfixOf` T.pack t)
            Right _ -> assertFailure "expected an error"

-- | The Pact service suites under @CHAINWEB_TEST_STORAGE=pact-shadow@ check
-- every chain at its end ('checkShadowNotBroken'): a broken split side runs
-- the chain on the legacy store alone, so the suite would pass without
-- testing the split store.
shadowBrokenFailsTestTest :: Assertion
shadowBrokenFailsTestTest = withSystemTempDirectory "split-node" $ \tmp -> do
    let root = tmp </> "0"
    logs <- newIORef []
    withPactStorage quietLogger v testConfig { _phStore = HistoryShadow } root False $ \st ->
        withChainStorage (testStorageLogger logs) st cid False $ \cst -> case _cstStore cst of
            ChainStoreShadow env -> do
                _psInitSchema (chainStorePactStore (_cstSql cst) (_cstStore cst))
                checkShadowNotBroken logs cst
                -- a split-side write fails: the hot file loses BlockHistory
                Just cf <- return (_cstFreezer cst)
                Pact4.exec_ (_cfHot cf) "DROP TABLE BlockHistory"
                _psCommitBlock (chainStorePactStore (_cstSql cst) (_cstStore cst)) (block 0)
                    >>= either (assertFailure . show) return
                readIORef (_shBroken env) >>= assertBool "broken"
                try (checkShadowNotBroken logs cst) >>= \case
                    Left (e :: IOException) -> do
                        let t = T.pack (displayException e)
                        assertBool ("names the break: " <> T.unpack t) ("split side of the chain broke" `T.isInfixOf` t)
                        assertBool ("carries ShadowBroken: " <> T.unpack t) ("ShadowBroken" `T.isInfixOf` t)
                    Right () -> assertFailure "expected the check to fail"
            _ -> assertFailure "expected shadow"
