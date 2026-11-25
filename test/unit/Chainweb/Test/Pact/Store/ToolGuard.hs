{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.Test.Pact.Store.ToolGuard
-- License: MIT
-- Stability: experimental
--
-- The guard of the tools that read the Pact SQLite files directly
-- ('refuseNonSqliteLayout'): compaction, @pact-diff@, the grand hash opener
-- and @db-checksum@ refuse a split directory before its first trim, a trimmed
-- split directory and a shadow directory, from every form of path they are
-- handed. A directory of @--pact-history-store sqlite@ passes and is not
-- touched. The in-process side of @cw-compare@ is tested in
-- @storage-tools-tests@.
--
module Chainweb.Test.Pact.Store.ToolGuard
( tests
) where

import Control.Exception
import Control.Monad
import qualified Data.ByteString as B
import Data.List (sort)
import qualified Data.Text as T
import System.Directory
import System.FilePath
import System.IO.Temp (withSystemTempDirectory)
import System.LogLevel
import Test.Tasty
import Test.Tasty.HUnit

import qualified Pact.Types.SQLite as SQL

import Chainweb.ChainId
import Chainweb.Graph (singletonChainGraph)
import Chainweb.Logger (GenericLogger, genericLogger)
import qualified Chainweb.Pact.Backend.Compaction as Compaction
import Chainweb.Pact.Backend.PactState (withChainDb)
import qualified Chainweb.Pact.Backend.PactState.Diff as Diff
import Chainweb.Pact.Backend.PactState.GrandHash.Utils (withConnections)
import Chainweb.Pact.Backend.Store.Layout
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils (chainDbFileName, withSqliteDb)
import Chainweb.Test.TestVersions (barebonesTestVersion)
import Chainweb.Test.Utils.Storage (NonSqliteFixture(..), makeNonSqliteFixtures)
import Chainweb.Version (ChainwebVersion)

tests :: TestTree
tests = testGroup "sqlite-only tool guard"
    [ testCase "the guard refuses each layout (split before its first trim, trimmed split, shadow) from the root, the sqlite or sqlite-split directory, a SQLite file; the message names the layout and P5; shadow points to the legacy copy"
        guardTest
    , testCase "a sqlite-mode directory (<db>/0/sqlite, a test node's directory, a missing one) passes the guard and is not touched, byte for byte"
        plainTest
    , testCase "compaction: compact and the shared opener withChainDb refuse each layout; nothing is created"
        compactionTest
    , testCase "pact-diff refuses each layout on either side; two sqlite-mode directories still diff"
        pactDiffTest
    , testCase "grand hash: the shared opener withConnections refuses each layout"
        grandHashTest
    , testCase "db-checksum: the guard on its SQLite file refuses each layout"
        dbChecksumTest
    ]

v :: ChainwebVersion
v = barebonesTestVersion singletonChainGraph

cid :: ChainId
cid = unsafeChainId 0

quiet :: GenericLogger
quiet = genericLogger Error (\_ -> return ())

withFixtures :: ([NonSqliteFixture] -> IO a) -> IO a
withFixtures k = withSystemTempDirectory "tool-guard" $ \base ->
    makeNonSqliteFixtures base >>= k

-- | The layout 'refuseNonSqliteLayout' must find for a fixture.
expectedLayout :: NonSqliteFixture -> NonSqliteLayout
expectedLayout fx = case _nsfName fx of
    "split-untrimmed" -> SplitNotTrimmed root
    "split-trimmed" -> SplitTrimmed root
    "shadow" -> ShadowLayout root
    n -> error ("unknown fixture " <> n)
  where
    root = _nsfDbDir fx </> "0"

-- | Runs the action and checks that it fails with 'SqliteOnlyTool' for the
-- fixture's layout, naming the tool.
assertRefused :: String -> T.Text -> NonSqliteFixture -> IO a -> Assertion
assertRefused what tool fx act = try act >>= \case
    Left (SqliteOnlyTool t _ found) -> do
        t @?= tool
        found @?= expectedLayout fx
    Left e -> assertFailure $ what <> " (" <> _nsfName fx <> "): another error: " <> show e
    Right _ -> assertFailure $ what <> " (" <> _nsfName fx <> "): not refused"

-- | All files under a directory with their bytes.
snapshot :: FilePath -> IO [(FilePath, B.ByteString)]
snapshot d = doesDirectoryExist d >>= \case
    False -> return []
    True -> do
        es <- sort <$> listDirectory d
        fmap concat $ forM es $ \e -> do
            let p = d </> e
            doesDirectoryExist p >>= \case
                True -> map (\(f, b) -> (e </> f, b)) <$> snapshot p
                False -> (\b -> [(e, b)]) <$> B.readFile p

guardTest :: Assertion
guardTest = withFixtures $ \fxs -> forM_ fxs $ \fx -> do
    let root = _nsfDbDir fx </> "0"
        paths = [root, root </> sqliteDirName, _nsfSqliteDir fx, _nsfFile fx]
    forM_ paths $ \p -> do
        before <- snapshot (_nsfDbDir fx)
        try (refuseNonSqliteLayout "tool-x" p) >>= \case
            Left err@(SqliteOnlyTool "tool-x" p' found) -> do
                p' @?= p
                found @?= expectedLayout fx
                let msg = pactLayoutErrorMessage err
                    has t = assertBool (_nsfName fx <> ", " <> p <> ": the message has " <> show t <> ": " <> T.unpack msg)
                        (t `T.isInfixOf` msg)
                has "tool-x refuses"
                has "--pact-history-store sqlite until P5"
                if _nsfName fx == "shadow"
                    then do
                        has "shadow Pact history layout"
                        has (T.pack (root </> sqliteDirName) <> " is the complete legacy copy")
                    else do
                        has "split Pact history layout"
                        has "storage-export"
            Left e -> assertFailure $ _nsfName fx <> ", " <> p <> ": another error: " <> show e
            Right () -> assertFailure $ _nsfName fx <> ", " <> p <> ": not refused"
        afterSnap <- snapshot (_nsfDbDir fx)
        assertBool (_nsfName fx <> ": the guard changed nothing") (before == afterSnap)
    -- a split directory whose first-trim rename did not finish: the marker
    -- is in sqlite (state B)
    withSystemTempDirectory "tool-guard-b" $ \db -> do
        let rootB = db </> "0"
        createDirectoryIfMissing True (rootB </> sqliteDirName)
        _ <- writeSplitMarker (rootB </> sqliteDirName) cid 1
        nonSqliteLayoutAt (rootB </> sqliteDirName) >>= (@?= Just (SplitRenameInterrupted rootB))
    -- a copied directory under another name that carries the marker
    withSystemTempDirectory "tool-guard-m" $ \d -> do
        _ <- writeSplitMarker d cid 1
        nonSqliteLayoutAt d >>= (@?= Just (MarkedDir d))

plainTest :: Assertion
plainTest = withSystemTempDirectory "tool-guard-plain" $ \db -> do
    let root = db </> "0"
        sqliteDir = root </> sqliteDirName
        nodeDir = db </> "node-1"
    createDirectoryIfMissing True (root </> "rocksDb")
    createDirectoryIfMissing True sqliteDir
    createDirectoryIfMissing True nodeDir
    -- real SQLite files with a row each, as a node of 3.2.1 keeps them
    forM_ [sqliteDir, nodeDir] $ \d -> withSqliteDb cid quiet d False $ \sql ->
        writeRow sql
    before <- snapshot db
    forM_ [root, sqliteDir, sqliteDir </> chainDbFileName cid, nodeDir, nodeDir </> chainDbFileName cid, db </> "missing"] $ \p -> do
        nonSqliteLayoutAt p >>= (@?= Nothing)
        refuseNonSqliteLayout "tool-x" p
    afterSnap <- snapshot db
    assertBool "byte for byte unchanged" (before == afterSnap)
    -- the shared opener still opens it, and reads the row
    forM_ [sqliteDir, nodeDir] $ \d -> withChainDb cid quiet d $ \_ sql -> readRows sql >>= (@?= 1)
  where
    writeRow :: SQLiteEnv -> IO ()
    writeRow sql = do
        SQL.exec_ sql "CREATE TABLE t (k INTEGER)"
        SQL.exec_ sql "INSERT INTO t VALUES (1)"
    readRows :: SQLiteEnv -> IO Int
    readRows sql = SQL.qry_ sql "SELECT COUNT(*) FROM t" [SQL.RInt] >>= \case
        [[SQL.SInt n]] -> return (fromIntegral n)
        r -> assertFailure ("unexpected " <> show r)

compactionTest :: Assertion
compactionTest = withFixtures $ \fxs -> forM_ fxs $ \fx -> do
    let to = _nsfDbDir fx <> "-compacted"
        cfg = Compaction.Config
            { Compaction.chainwebVersion = v
            , Compaction.fromDir = _nsfDbDir fx
            , Compaction.toDir = to
            , Compaction.concurrent = Compaction.SingleChain
            , Compaction.logDir = _nsfDbDir fx <> "-logs"
            , Compaction.compactRocksDb = True
            , Compaction.compactPactState = True
            , Compaction.compactTransactionIndex = False
            , Compaction.storeMerkleTrees = True
            }
    before <- snapshot (_nsfDbDir fx)
    assertRefused "compact" "compact" fx (Compaction.compact cfg)
    doesDirectoryExist to >>= assertBool "no target created" . not
    doesDirectoryExist (Compaction.logDir cfg) >>= assertBool "no log directory created" . not
    forM_ [_nsfDbDir fx </> "0", _nsfDbDir fx </> "0" </> sqliteDirName, _nsfSqliteDir fx] $ \p ->
        assertRefused "withChainDb" "This Pact database tool" fx (withChainDb cid quiet p $ \_ _ -> return ())
    afterSnap <- snapshot (_nsfDbDir fx)
    assertBool "the source is unchanged" (before == afterSnap)

pactDiffTest :: Assertion
pactDiffTest = withFixtures $ \fxs -> withSystemTempDirectory "tool-guard-diff" $ \tmp -> do
    let plainA = tmp </> "a"
        plainB = tmp </> "b"
        cfg a b = Diff.PactDiffConfig
            { Diff.firstDbDir = a
            , Diff.secondDbDir = b
            , Diff.chainwebVersion = v
            , Diff.target = 0
            , Diff.logDir = tmp </> "logs"
            }
    mapM_ (createDirectoryIfMissing True) [plainA, plainB]
    forM_ fxs $ \fx -> forM_ [_nsfDbDir fx </> "0" </> sqliteDirName, _nsfSqliteDir fx] $ \p -> do
        assertRefused "pact-diff, first" "pact-diff" fx (Diff.pactDiff (cfg p plainB))
        assertRefused "pact-diff, second" "pact-diff" fx (Diff.pactDiff (cfg plainA p))
    -- two sqlite-mode directories: no refusal (the chains have no files, so
    -- the diff is empty)
    Diff.pactDiff (cfg plainA plainB)

grandHashTest :: Assertion
grandHashTest = withFixtures $ \fxs -> forM_ fxs $ \fx ->
    forM_ [_nsfDbDir fx </> "0" </> sqliteDirName, _nsfSqliteDir fx] $ \p ->
        assertRefused "withConnections" "The grand hash tool" fx
            (withConnections quiet p [cid] $ \_ -> return ())

-- | @db-checksum@ is an executable; its 'work' calls the guard on its
-- @--sqlite-file@ first, which is what this checks.
dbChecksumTest :: Assertion
dbChecksumTest = withFixtures $ \fxs -> forM_ fxs $ \fx ->
    assertRefused "db-checksum" "db-checksum" fx (refuseNonSqliteLayout "db-checksum" (_nsfFile fx))
