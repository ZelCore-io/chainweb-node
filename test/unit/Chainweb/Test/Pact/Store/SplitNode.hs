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
import Control.Lens ((&), (.~), view)
import Control.Monad

import qualified Data.ByteString as B
import Data.IORef
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text.Encoding as T
import System.LogLevel (LogLevel(..))
import Data.Maybe (isJust, isNothing)
import qualified Data.Text as T

import System.Directory
import System.FilePath ((<.>), (</>))
import System.IO.Temp (withSystemTempDirectory)

import Test.Tasty
import Test.Tasty.HUnit

import qualified Pact.Interpreter as P4 (PactDbEnv(..))
import qualified Pact.Types.Runtime as P4
import qualified Pact.Core.Guards as P5 (KeySetName(..))
import qualified Pact.Core.Names as P5 (RowKey(..))
import qualified Pact.Core.Persistence as P5 (Domain(..), PactDb(..))

-- internal modules

import Chainweb.Backup
import Chainweb.BlockHeaderDB (Configuration(..), initBlockHeaderDb, closeBlockHeaderDb)
import Chainweb.BlockHeaderDB.Internal (unsafeInsertBlockHeaderDb)
import Chainweb.Logger (GenericLogger)
import Chainweb.Pact.Backend.Store.Split (SplitStoreError(..))
import Chainweb.Pact.Backend.Types (Checkpointer(..), IntraBlockPersistence(..))
import qualified Chainweb.Pact.PactService.Checkpointer.Internal as Checkpointer
import Chainweb.Pact.PactService (withPactServiceStorage)
import Chainweb.Pact.Types (RunnableBlock, defaultModuleCacheLimit, testPactServiceConfig)
import qualified Chainweb.Pact4.Backend.ChainwebPactDb as Pact4
import Chainweb.Payload.PayloadStore.InMemory (newPayloadDb)
import qualified Chainweb.Test.Pact.Store.Characterization as C
import Chainweb.Test.Pact.Store.Characterization (Op(..), ScriptStorage(..))
import Chainweb.Test.Utils.Storage (TestStorageOption(..), acquireSplitChainStorageFor, testPactHistoryConfigFor)
import Chainweb.Utils (sshow)
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
    , testCase "test storage: the release closes the storage also when the shadow check fails" releaseAfterFailedCheckTest
    , testCase "restart with the block header database below the frontier: initializeLatestBlock, exitOnRewindBelowFrontier, the fatal-error handler gets the refusal unwrapped" restartBelowFrontierTest
    , testGroup "W5"
        [ testCase "Pact 5: fork point above the written frontier; /local at every height equals a sqlite-mode twin fed the same blocks" (w5Test W5P5)
        , testCase "Pact 4: same, with intra-block versions (PersistIntraBlockWrites)" (w5Test W5P4)
        , testCase "fork point below the written frontier: RewindBelowFrontier; the node keeps serving its tip" belowFrontierForkTest
        , testCase "split goldens: pact-store-pact4-history, -missing-table-errors, -lookup-txs, -reads reproduce byte for byte (no --accept), depth 1" splitGoldensTest
        , testCase "full-history check: getEarliestBlock is genesis after trimming" fullHistoryTest
        , testCase "shadow mode: a Pact 4 and a Pact 5 run log zero ShadowDifference" shadowW5Test
        ]
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

-- | The release of a split or shadow test storage ('acquireSplitChainStorageFor',
-- under 'Chainweb.Test.Utils.Storage.testChainStorageResource' and
-- 'Chainweb.Test.Utils.Storage.acquireTestChainStorage') when the shadow
-- check fails: the test fails with the check's error, and the storage is
-- still closed.
releaseAfterFailedCheckTest :: Assertion
releaseAfterFailedCheckTest = do
    closed <- newIORef False
    (cst, release) <- acquireSplitChainStorageFor (testPactHistoryConfigFor [TestPactShadow])
        (writeIORef closed True) v cid
    case _cstStore cst of
        ChainStoreShadow env -> do
            let store = chainStorePactStore (_cstSql cst) (_cstStore cst)
            _psInitSchema store
            Just cf <- return (_cstFreezer cst)
            Pact4.exec_ (_cfHot cf) "DROP TABLE BlockHistory"
            _psCommitBlock store (block 0) >>= either (assertFailure . show) return
            readIORef (_shBroken env) >>= assertBool "broken"
        _ -> assertFailure "expected shadow"
    try release >>= \case
        Left (e :: IOException) -> assertBool ("the check's error: " <> displayException e)
            ("split side of the chain broke" `T.isInfixOf` T.pack (displayException e))
        Right () -> assertFailure "expected the shadow check to fail the release"
    readIORef closed >>= assertBool "the storage is closed although the check failed"

-- -------------------------------------------------------------------------- --
-- W5 (spec 10.2): the Characterization script on a split store at a small
-- freeze depth, the freezer drained after every block, with a reorg.

data W5Pact = W5P4 | W5P5
    deriving Show

-- | Blocks 7 to 9 of the main chain, after 'C.scenario'.
mainExtra :: [[(B.ByteString, [Op])]]
mainExtra =
    [ [ C.tx 7 0 [Put "t1" "a" 16] ]
    , [ C.tx 8 0 [Put "t2" "x" 17, Ks "ks2"] ]
    , [ C.tx 9 0 [Put "t1" "b" 18], C.tx 9 1 [Put "t1" "b" 19] ]
    ]

-- | A fork of five blocks from parent height 7 (heights 8 to 12): it writes
-- keys whose versions are frozen, creates a table, and writes one key twice
-- in a block.
forkTxs :: [[(B.ByteString, [Op])]]
forkTxs =
    [ [ C.tx 8 5 [Create "t3", Put "t3" "f" 20, Put "t1" "zz" 21] ]
    , [ C.tx 9 5 [Put "t1" "a" 22], C.tx 9 6 [Put "t1" "a" 23] ]
    , [ C.tx 10 5 [Ks "ks1", Put "t2" "y" 24] ]
    , [ C.tx 11 5 [Put "t3" "f" 25], C.tx 11 6 [Put "t1" "C" 26] ]
    , [ C.tx 12 5 [Put "t1" "new" 27] ]
    ]

forkPoint :: Int
forkPoint = 7

-- | Children of a header, with block hashes that differ from the main chain.
forkFrom :: BlockHeader -> [BlockHeader]
forkFrom = tail . iterate next
  where
    next q = q
        & blockHash .~ mkHash (fromIntegral (view blockHeight q) + 1) 0xf0
        & blockParent .~ view blockHash q
        & blockHeight .~ view blockHeight q + 1

w5Headers :: W5Pact -> [BlockHeader]
w5Headers = \case
    W5P4 -> C.p4Headers
    W5P5 -> C.p5Headers

w5With :: W5Pact -> ScriptStorage -> (Checkpointer GenericLogger -> ChainStorage -> IO a) -> IO a
w5With = \case
    W5P4 -> \s -> C.withCpOn s PersistIntraBlockWrites C.v4
    W5P5 -> \s -> C.withCpOn s DoNotPersistIntraBlockWrites C.v5

w5Block :: W5Pact -> BlockHeader -> [(B.ByteString, [Op])] -> RunnableBlock GenericLogger ()
w5Block = \case
    W5P4 -> C.p4Block
    W5P5 -> C.p5Block

w5Tables :: [Text]
w5Tables = ["t1", "t2", "t3"]

w5Keys :: [Text]
w5Keys = ["a", "b", "C", "B", "zz", "k9", "k10", "x", "y", "f", "new", "nokey"]

-- | What @/local@ sees on top of a parent: every key of every table, the
-- keys of every table and the keysets; in Pact 4 also the history API
-- (@txids@ and @getTxLog@).
probeAt :: W5Pact -> Checkpointer GenericLogger -> BlockHeader -> IO [Text]
probeAt W5P4 cp ph = C.p4Read cp ph $ \env -> do
    let P4.PactDbEnv pdb mv = Pact4._cpPactDbEnv env
    rows <- forM [ (t, k) | t <- w5Tables, k <- w5Keys ] $ \(t, k) ->
        C.probe ("read " <> t <> " " <> k) $ P4._readRow pdb (C.ut4 t) (P4.RowKey k) mv
    keys <- forM w5Tables $ \t -> C.probe ("keys " <> t) $ P4._keys pdb (C.ut4 t) mv
    kss <- forM ["ks1", "ks2"] $ \k ->
        C.probe ("read keyset " <> k) $ P4._readRow pdb P4.KeySets (fromString (T.unpack k)) mv
    hist <- forM w5Tables $ \t -> try (P4._txids pdb (P4.TableName t) (P4.TxId 0) mv) >>= \case
        Left (e :: SomeException) -> return ["txids " <> t <> ": error: " <> T.pack (displayException e)]
        Right ids -> (("txids " <> t <> ": " <> sshow ids) :) <$> forM ids (\i ->
            C.probe ("getTxLog " <> t <> " " <> sshow i) $ P4._getTxLog pdb (C.ut4 t) i mv)
    return (rows <> keys <> kss <> concat hist)
probeAt W5P5 cp ph = C.p5Read cp ph $ \pdb -> do
    rows <- forM [ (t, k) | t <- w5Tables, k <- w5Keys ] $ \(t, k) ->
        C.probe5 ("read " <> t <> " " <> k) $ P5._pdbRead pdb (P5.DUserTables (C.tn5 t)) (P5.RowKey k)
    keys <- forM w5Tables $ \t -> C.probe5 ("keys " <> t) $ P5._pdbKeys pdb (P5.DUserTables (C.tn5 t))
    kss <- forM ["ks1", "ks2"] $ \k ->
        C.probe5 ("read keyset " <> k) $ P5._pdbRead pdb P5.DKeySets (P5.KeySetName k Nothing)
    return (rows <> keys <> kss)

-- | The written frontier of a chain's cold store, if it has one.
writtenFrontier :: ChainStorage -> IO (Maybe Frontier)
writtenFrontier cst = maybe (return Nothing) (_ccWritten . _cfCold) (_cstFreezer cst)

-- | Feeds the main chain (heights 0 to 9) and then the fork from parent
-- height 'forkPoint' (heights 8 to 12) one block at a time; after each block
-- probes every height of the current branch. Returns the probes and the
-- written frontier after each block; @check@ runs at the end.
w5Run
    :: W5Pact
    -> ScriptStorage
    -> (ChainStorage -> IO ())
    -> IO ([Text], [Maybe Frontier])
w5Run pv s check = w5With pv s $ \cp cst -> do
    rs <- forM (zip3 parents (mainH <> forkH) (take 10 (C.scenario <> mainExtra) <> forkTxs) `zip` branches) $
        \((parent, hdr, txs), branch) -> do
            C.extend cp parent [w5Block pv hdr txs]
            out <- forM branch $ \ph ->
                (("== parent " <> sshow (view blockHeight ph) <> " " <> sshow (view blockHash ph)) :)
                    <$> probeAt pv cp ph
            f <- writtenFrontier cst
            return (("=== after block " <> sshow (view blockHeight hdr)) : concat out, f)
    check cst
    return (concatMap fst rs, snd <$> rs)
  where
    mainH = take 10 (w5Headers pv)
    forkH = take 5 (forkFrom (mainH !! forkPoint))
    parents = Nothing : (Just <$> init mainH) <> (Just (mainH !! forkPoint) : (Just <$> init forkH))
    branches = [ take i mainH | i <- [1 .. 10] ] <> [ take (forkPoint + 1) mainH <> take j forkH | j <- [1 .. 5] ]

assertSameLines :: String -> [Text] -> [Text] -> Assertion
assertSameLines what want got = case [ i | (i, a, b) <- zip3 [0 :: Int ..] want got, a /= b ] of
    i : _ -> assertFailure $ what <> ": line " <> show i <> " differs\n  want: " <> T.unpack (want !! i)
        <> "\n  got:  " <> T.unpack (got !! i)
    [] -> length got @?= length want

w5Test :: W5Pact -> Assertion
w5Test pv = do
    (want, _) <- w5Run pv ScriptSqlite (\_ -> return ())
    (got, fs) <- w5Run pv (ScriptSplit 3) (\_ -> return ())
    let beforeFork = fs !! 9
        final = last fs
    assertBool ("before the fork the frontier is above genesis and below the fork point: " <> show beforeFork)
        (maybe False (\f -> _frHeight f > 0 && _frHeight f < fromIntegral forkPoint) beforeFork)
    assertBool ("at the end the frontier is on the fork: " <> show final)
        (maybe False ((> fromIntegral forkPoint) . _frHeight) final)
    assertSameLines (show pv <> ": split against sqlite") want got

-- | A fork from below the written frontier is refused; the chain keeps its
-- tip, answers as before, and takes the next block.
belowFrontierForkTest :: Assertion
belowFrontierForkTest = w5With W5P5 (ScriptSplit 3) $ \cp cst -> do
    let mainH = take 10 C.p5Headers
        tipH = last mainH
    forM_ (zip3 (Nothing : (Just <$> init mainH)) mainH (take 10 (C.scenario <> mainExtra))) $ \(p, h, txs) ->
        C.extend cp p [C.p5Block h txs]
    Just f <- writtenFrontier cst
    assertBool ("frontier above height 2: " <> show f) (_frHeight f > 2)
    before <- probeAt W5P5 cp tipH
    try (C.extend cp (Just (mainH !! 2)) [C.p5Block (head (forkFrom (mainH !! 2))) (head forkTxs)]) >>= \case
        Left (RewindBelowFrontier c _ f') -> do
            c @?= cid
            f' @?= f
        Right () -> assertFailure "expected RewindBelowFrontier"
    _psGetLatestBlock (cpStore cp) >>= (@?= Just (9, view blockHash tipH))
    probeAt W5P5 cp tipH >>= assertSameLines "the tip after the refusal" before
    let next = head (forkFrom tipH)
    C.extend cp (Just tipH) [C.p5Block next [C.tx 10 0 [Put "t1" "a" 30]]]
    _psGetLatestBlock (cpStore cp) >>= (@?= Just (10, view blockHash next))

-- | The goldens that only read reproduce byte for byte on a split store at
-- freeze depth 1, so most of their reads cross into the cold store.
splitGoldensTest :: Assertion
splitGoldensTest = forM_ goldens $ \(name, gen) -> do
    want <- B.readFile ("test/golden/" <> name <> "-expected.txt")
    got <- T.encodeUtf8 . T.unlines <$> gen (ScriptSplit 1)
    when (got /= want) $
        assertSameLines name (T.lines (T.decodeUtf8 want)) (T.lines (T.decodeUtf8 got))
            >> assertFailure (name <> ": the bytes differ")
  where
    goldens =
        [ ("pact-store-pact4-history", C.pact4History)
        , ("pact-store-missing-table-errors", C.missingTableErrors)
        , ("pact-store-lookup-txs", C.lookupTxs)
        , ("pact-store-reads", C.readsAtHeights)
        ]

-- | After trimming, the earliest block is still genesis, so a node with
-- @--full-history-required@ starts ('_pactFullHistoryRequired').
fullHistoryTest :: Assertion
fullHistoryTest = do
    twinRows <- w5With W5P4 ScriptSqlite $ \cp cst -> do
        feedMain cp
        length . concatMap snd <$> dumpTables (_cstSql cst) ["SYS:KeySets"]
    w5With W5P4 (ScriptSplit 3) $ \cp cst -> do
        feedMain cp
        Just f <- writtenFrontier cst
        assertBool ("frontier above genesis: " <> show f) (_frHeight f > 3)
        hotRows <- length . concatMap snd <$> dumpTables (_cstSql cst) ["SYS:KeySets"]
        assertBool ("trimmed: " <> show (hotRows, twinRows)) (hotRows < twinRows)
        Checkpointer.getEarliestBlock (cpStore cp)
            >>= (@?= Just (0, view blockHash (head C.p4Headers)))
  where
    mainH = take 10 C.p4Headers
    feedMain cp = forM_ (zip3 (Nothing : (Just <$> init mainH)) mainH (take 10 (C.scenario <> mainExtra))) $
        \(p, h, txs) -> C.extend cp p [C.p4Block h txs]

-- | The W5 script in shadow mode: the split side answers as the legacy one
-- everywhere, including across the reorg.
shadowW5Test :: Assertion
shadowW5Test = forM_ [W5P4, W5P5] $ \pv -> do
    logs <- newIORef []
    let logger = genericLogger Warn (\t -> modifyIORef' logs (t :))
        check cst = case _cstStore cst of
            ChainStoreShadow env -> do
                readIORef (_shBroken env) >>= assertBool (show pv <> ": split side not broken") . not
                readIORef (_shDifferences env) >>= (@?= 0)
            _ -> assertFailure "expected shadow"
    (want, _) <- w5Run pv ScriptSqlite (\_ -> return ())
    (got, fs) <- w5Run pv (ScriptShadow 3 logger) check
    assertBool ("the split side froze: " <> show (last fs)) (maybe False ((> fromIntegral forkPoint) . _frHeight) (last fs))
    assertSameLines (show pv <> ": shadow against sqlite") want got
    ls <- readIORef logs
    assertBool (show pv <> ": no ShadowDifference: " <> show (take 5 ls)) $
        not (any ("ShadowDifference" `T.isInfixOf`) ls)

-- | A restart whose block header database is older than the frontier: the
-- startup rewind of the Pact service ('initializeLatestBlock') is refused
-- with 'RewindBelowFrontier', 'exitOnRewindBelowFrontier' hands it to
-- '_psOnFatalError' (here the default, which logs it and throws
-- @ReorgLimitExceeded@), and the message that reaches the handler is the
-- refusal's own. The SQLite tip is untouched.
restartBelowFrontierTest :: Assertion
restartBelowFrontierTest = withSystemTempDirectory "split-node" $ \tmp -> do
    let root = tmp </> "0"
        cfg = testConfig { _phFreezeDepth = 3, _phFreezeStepHeights = 1 }
        mainH = take 10 C.p4Headers
    refusal <- withPactStorage quietLogger C.v4 cfg root False $ \st ->
        withChainStorage quietLogger st cid False $ \cst -> do
            lock <- newMVar ()
            withChainFreezer cst (\io -> withMVar lock (const io)) (return False) (return Nothing) $ \_ -> do
                cp <- Checkpointer.initCheckpointerResourcesWith defaultModuleCacheLimit
                    (_cstSql cst) (_cstStore cst) PersistIntraBlockWrites quietLogger C.v4 cid
                forM_ (zip3 (Nothing : (Just <$> init mainH)) mainH (take 10 (C.scenario <> mainExtra))) $ \(p, h, txs) -> do
                    C.extend cp p [C.p4Block h txs]
                    void $ drainChainFreezer cst
                Just f <- writtenFrontier cst
                assertBool ("frontier above height 1: " <> show f) (_frHeight f > 1)
                -- the refusal the restart must report, with its target
                try (_psRewindDbTo (cpStore cp) (Just (ParentHeader (mainH !! 1)))) >>= \case
                    Left (e :: SplitStoreError) -> return e
                    Right _ -> assertFailure "expected RewindBelowFrontier"
    logs <- newIORef []
    let logger = genericLogger Info (\t -> modifyIORef' logs (t :))
    withTempRocksDb "split-node-headers" $ \rdb -> do
        -- the block header database knows blocks 0 and 1 only
        bhdb <- initBlockHeaderDb (Configuration (head mainH) rdb)
        unsafeInsertBlockHeaderDb bhdb (mainH !! 1)
        pdb <- newPayloadDb
        withPactStorage quietLogger C.v4 cfg root False $ \st ->
            withChainStorage quietLogger st cid False $ \cst -> do
                r <- try $ withPactServiceStorage C.v4 cid logger Nothing bhdb pdb cst (return False)
                    testPactServiceConfig (return ())
                case r of
                    -- 'defaultOnFatalError' throws @ReorgLimitExceeded (show pex <> "\n" <> msg)@
                    Left (e :: SomeException) -> do
                        let t = T.pack (show e)
                            msg = T.pack (displayException refusal)
                        assertBool ("thrown by the default fatal-error handler: " <> T.unpack t)
                            ("reorg limit exceeded: \n" `T.isPrefixOf` t)
                        assertBool ("the handler got the refusal's message unwrapped: " <> T.unpack t)
                            (("\n" <> msg) `T.isSuffixOf` t)
                        ls <- readIORef logs
                        assertBool "the handler logged it at Error" $
                            any (\l -> "[Error]" `T.isInfixOf` l && msg `T.isInfixOf` l) ls
                    Right _ -> assertFailure "expected the Pact service to stop"
                _psGetLatestBlock (chainStorePactStore (_cstSql cst) (_cstStore cst))
                    >>= (@?= Just (9, view blockHash (last mainH)))
        closeBlockHeaderDb bhdb
