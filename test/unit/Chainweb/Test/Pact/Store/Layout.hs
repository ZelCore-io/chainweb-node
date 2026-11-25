{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Test.Pact.Store.Layout
--
-- Tests for the directory layout of the Pact history, the split marker and
-- the refusals (Task 20).
--
module Chainweb.Test.Pact.Store.Layout
( tests
) where

import Control.Exception
import Control.Monad

import Data.Aeson (eitherDecodeFileStrict)
import Data.IORef
import qualified Data.Text as T
import Database.SQLite3.Direct (Utf8(..))

import System.Directory
import System.FilePath
import System.IO.Temp (withSystemTempDirectory)
import System.LogLevel

import Test.Tasty
import Test.Tasty.HUnit

import qualified Pact.Types.SQLite as SQL

-- internal modules

import Chainweb.ChainId
import Chainweb.Logger (GenericLogger, genericLogger)
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Pact.Backend.Store.Layout
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils

tests :: TestTree
tests = testGroup "Pact layout"
    [ testCase "every cell of the mode x directory-state table gives its outcome"
        tableTest
    , testCase "ensureSplitDir: marker written before the rename; second call is a no-op"
        ensureSplitDirTest
    , testCase "crash after the marker, before the rename (state B): split finishes it, sqlite removes the marker"
        stateBTest
    , testCase "rename with an open WAL connection: commit before and after, close, reopen at sqlite-split: all rows, integrity_check ok"
        openConnectionRenameTest
    , testCase "a connection opened at the new path while the old one is open sees the old one's commits"
        twoPathsTest
    , testCase "reset with split or shadow is refused; the message names the mode"
        resetTest
    ]

-- -------------------------------------------------------------------------- --
-- Utils

cid0 :: ChainId
cid0 = unsafeChainId 0

withRoot :: (FilePath -> IO a) -> IO a
withRoot act = withSystemTempDirectory "pact-layout" $ \tmp -> do
    let root = tmp </> "0"
    createDirectory root
    act root

-- | A logger that keeps the messages at Warn and above.
--
withLogger :: (GenericLogger -> IO [T.Text] -> IO a) -> IO a
withLogger act = do
    ref <- newIORef []
    let l = genericLogger Warn (\t -> modifyIORef' ref (t :))
    act l (reverse <$> readIORef ref)

sqliteDir, splitDir, coldDir :: FilePath -> FilePath
sqliteDir r = r </> sqliteDirName
splitDir r = r </> splitDirName
coldDir r = r </> coldDirName

markerIn :: FilePath -> FilePath
markerIn d = d </> splitMarkerFileName

-- | A directory with one file in it, so that a test can see it survive.
--
mkDataDir :: FilePath -> IO ()
mkDataDir d = do
    createDirectoryIfMissing True d
    writeFile (d </> "pact-v1-chain-0.sqlite") (takeFileName d)

mkMarker :: FilePath -> IO ()
mkMarker d = void $ writeSplitMarker d cid0 42

-- -------------------------------------------------------------------------- --
-- The table

data State = A | ACold | B | CCold | CNoCold | AandC | D
    deriving (Show, Eq, Enum, Bounded)

setup :: FilePath -> State -> IO ()
setup r = \case
    A -> mkDataDir (sqliteDir r)
    ACold -> mkDataDir (sqliteDir r) >> createDirectory (coldDir r)
    B -> mkDataDir (sqliteDir r) >> mkMarker (sqliteDir r)
    CCold -> mkDataDir (splitDir r) >> mkMarker (splitDir r) >> createDirectory (coldDir r)
    CNoCold -> mkDataDir (splitDir r) >> mkMarker (splitDir r)
    AandC -> do
        mkDataDir (sqliteDir r)
        mkDataDir (splitDir r) >> mkMarker (splitDir r)
        createDirectory (coldDir r)
    D -> mkDataDir (splitDir r) >> createDirectory (coldDir r)

data Outcome
    = Uses
        { _oSqlite :: FilePath -> FilePath
        , _oLegacy :: Maybe (FilePath -> FilePath)
        , _oCold :: Maybe (FilePath -> FilePath)
        , _oWarning :: Maybe T.Text
        }
    | Fails (FilePath -> PactLayoutError)

expected :: PactHistoryStore -> State -> Outcome
expected HistorySqlite = \case
    A -> Uses sqliteDir Nothing Nothing Nothing
    ACold -> Uses sqliteDir Nothing Nothing (Just "delete it to roll back")
    B -> Uses sqliteDir Nothing Nothing (Just "Removed the split format marker")
    CCold -> Fails (TrimmedDirInSqliteMode . splitDir)
    CNoCold -> Fails (TrimmedDirInSqliteMode . splitDir)
    AandC -> Uses sqliteDir Nothing Nothing (Just "is left over")
    D -> Fails (SplitDirWithoutMarker . splitDir)
expected HistorySplit = \case
    A -> Uses sqliteDir Nothing (Just coldDir) Nothing
    ACold -> Uses sqliteDir Nothing (Just coldDir) Nothing
    B -> Uses splitDir Nothing (Just coldDir) Nothing
    CCold -> Uses splitDir Nothing (Just coldDir) Nothing
    CNoCold -> Fails (MissingColdStore . coldDir)
    AandC -> Fails (\r -> AmbiguousSqliteDirs (sqliteDir r) (splitDir r))
    D -> Fails (SplitDirWithoutMarker . splitDir)
expected HistoryShadow = \case
    A -> Uses splitDir (Just sqliteDir) (Just coldDir) Nothing
    ACold -> Uses splitDir (Just sqliteDir) (Just coldDir) Nothing
    B -> Fails (ShadowNeedsLegacy . sqliteDir)
    CCold -> Fails (ShadowNeedsLegacy . sqliteDir)
    CNoCold -> Fails (ShadowNeedsLegacy . sqliteDir)
    AandC -> Uses splitDir (Just sqliteDir) (Just coldDir) Nothing
    D -> Fails (SplitDirWithoutMarker . splitDir)

tableTest :: Assertion
tableTest = forM_ [minBound .. maxBound] $ \mode -> forM_ [minBound .. maxBound] $ \st ->
    withRoot $ \r -> withLogger $ \logger getLogs -> do
        setup r st
        let cell = show mode <> " x " <> show st
        res <- try $ resolvePactLayout logger mode False r
        logs <- getLogs
        case (expected mode st, res) of
            (Fails e, Left err) -> do
                assertEqual cell (e r) err
                let msg = pactLayoutErrorMessage err
                assertBool (cell <> ": message names no directory: " <> T.unpack msg)
                    (any (\d -> T.pack d `T.isInfixOf` msg) [sqliteDir r, splitDir r, coldDir r])
            (Fails e, Right l) -> do
                d <- currentSqliteDir l
                assertFailure $ cell <> ": expected " <> show (e r) <> ", got the layout at " <> d
            (Uses{}, Left err) ->
                assertFailure $ cell <> ": failed with " <> show err
            (Uses s legacy cold warning, Right l) -> do
                d <- currentSqliteDir l
                assertEqual (cell <> ": sqlite dir") (s r) d
                assertEqual (cell <> ": legacy dir") (($ r) <$> legacy) (_plLegacyDir l)
                assertEqual (cell <> ": cold dir") (($ r) <$> cold) (_plColdDir l)
                assertEqual (cell <> ": mode") mode (_plMode l)
                -- the data survives where the layout points
                doesFileExist (d </> "pact-v1-chain-0.sqlite") >>= \case
                    True -> return ()
                    False -> unless (mode == HistoryShadow && st `elem` [A, ACold]) $
                        assertFailure (cell <> ": the data file is missing in " <> d)
                case warning of
                    Nothing -> assertEqual (cell <> ": warnings") [] logs
                    Just w -> assertBool (cell <> ": no warning with " <> show w <> ": " <> show logs)
                        (any (w `T.isInfixOf`) logs)
                -- the split side carries the marker, the legacy side none
                when (takeFileName d == splitDirName) $
                    readSplitMarker d >>= assertBool (cell <> ": no marker in " <> d) . (/= Nothing)
                when (takeFileName d == sqliteDirName) $
                    readSplitMarker d >>= assertEqual (cell <> ": marker in " <> d) Nothing
                forM_ (_plLegacyDir l) $ \lg ->
                    readSplitMarker lg >>= assertEqual (cell <> ": marker in the legacy dir") Nothing
                forM_ (_plColdDir l) $ \c -> when (mode == HistoryShadow) $
                    doesDirectoryExist c >>= assertBool (cell <> ": shadow did not create " <> c)
                -- a second start in the same mode gives the same layout; in
                -- between, the node has opened its cold store
                forM_ (_plColdDir l) $ createDirectoryIfMissing True
                l' <- resolvePactLayout logger mode False r
                currentSqliteDir l' >>= assertEqual (cell <> ": second start") d

-- -------------------------------------------------------------------------- --
-- ensureSplitDir

ensureSplitDirTest :: Assertion
ensureSplitDirTest = withRoot $ \r -> withLogger $ \logger _ -> do
    mkDataDir (sqliteDir r)
    l <- resolvePactLayout logger HistorySplit False r
    currentSqliteDir l >>= (@?= sqliteDir r)

    ensureSplitDir logger l (unsafeChainId 3) 1234
    currentSqliteDir l >>= (@?= splitDir r)
    doesDirectoryExist (sqliteDir r) >>= assertBool "sqlite still exists" . not
    readFile (splitDir r </> "pact-v1-chain-0.sqlite") >>= (@?= sqliteDirName)
    m <- eitherDecodeFileStrict (markerIn (splitDir r)) >>= either fail return
    _smFormat m @?= "chainweb-pact-split"
    _smVersion m @?= 1
    _smFirstTrimChain m @?= unsafeChainId 3
    _smFirstTrimHeight m @?= 1234
    doesFileExist (markerIn (splitDir r) <.> "tmp") >>= assertBool "temporary marker left" . not

    -- the second call changes nothing, even with another chain
    ensureSplitDir logger l (unsafeChainId 5) 99
    m' <- eitherDecodeFileStrict (markerIn (splitDir r)) >>= either fail return
    m' @?= m
    currentSqliteDir l >>= (@?= splitDir r)

    -- a restart in split mode sees state C; sqlite mode refuses it
    createDirectory (coldDir r)
    l2 <- resolvePactLayout logger HistorySplit False r
    currentSqliteDir l2 >>= (@?= splitDir r)
    try (resolvePactLayout logger HistorySqlite False r)
        >>= either ((@?= TrimmedDirInSqliteMode (splitDir r))) (const $ assertFailure "sqlite mode opened sqlite-split")

    -- shadow mode: the split side exists from the start, so the call is a no-op
    withRoot $ \r2 -> do
        mkDataDir (sqliteDir r2)
        ls <- resolvePactLayout logger HistoryShadow False r2
        before <- eitherDecodeFileStrict @SplitMarker (markerIn (splitDir r2)) >>= either fail return
        ensureSplitDir logger ls cid0 7
        afterM <- eitherDecodeFileStrict (markerIn (splitDir r2)) >>= either fail return
        afterM @?= before
        doesDirectoryExist (sqliteDir r2) >>= assertBool "shadow lost the legacy dir"

    -- sqlite mode never trims
    withRoot $ \r3 -> do
        mkDataDir (sqliteDir r3)
        lq <- resolvePactLayout logger HistorySqlite False r3
        try (ensureSplitDir logger lq cid0 7) >>= \case
            Left (_ :: IOException) -> return ()
            Right () -> assertFailure "ensureSplitDir ran in sqlite mode"
        doesDirectoryExist (sqliteDir r3) >>= assertBool "sqlite mode renamed"

-- -------------------------------------------------------------------------- --
-- State B

stateBTest :: Assertion
stateBTest = withLogger $ \logger getLogs -> do
    -- split finishes the rename at start
    withRoot $ \r -> do
        mkDataDir (sqliteDir r)
        mkMarker (sqliteDir r)
        l <- resolvePactLayout logger HistorySplit False r
        currentSqliteDir l >>= (@?= splitDir r)
        doesDirectoryExist (sqliteDir r) >>= assertBool "sqlite still exists" . not
        readSplitMarker (splitDir r) >>= assertBool "marker lost" . (/= Nothing)
        readFile (splitDir r </> "pact-v1-chain-0.sqlite") >>= (@?= sqliteDirName)
    -- sqlite removes the marker and logs it
    withRoot $ \r -> do
        mkDataDir (sqliteDir r)
        mkMarker (sqliteDir r)
        l <- resolvePactLayout logger HistorySqlite False r
        currentSqliteDir l >>= (@?= sqliteDir r)
        doesFileExist (markerIn (sqliteDir r)) >>= assertBool "marker kept" . not
        doesDirectoryExist (splitDir r) >>= assertBool "sqlite-split created" . not
        logs <- getLogs
        assertBool ("not logged: " <> show logs) (any ("Removed the split format marker" `T.isInfixOf`) logs)
    -- a marker this binary does not understand is refused
    withRoot $ \r -> do
        mkDataDir (sqliteDir r)
        writeFile (markerIn (sqliteDir r)) "{\"format\":\"chainweb-pact-split\",\"version\":2,\"firstTrimChain\":0,\"firstTrimHeight\":1,\"created\":\"x\"}"
        try (resolvePactLayout logger HistorySplit False r) >>= \case
            Left (UnknownSplitMarker f _) -> f @?= markerIn (sqliteDir r)
            Left e -> assertFailure $ "wrong error: " <> show e
            Right _ -> assertFailure "accepted a version 2 marker"

-- -------------------------------------------------------------------------- --
-- Open connections

dbFile :: FilePath -> FilePath
dbFile d = d </> chainDbFileName cid0

openDb :: FilePath -> IO SQLiteEnv
openDb f = openSQLiteConnection f chainwebPragmas

insertRow :: SQLiteEnv -> Int -> IO ()
insertRow db i = SQL.exec' db "INSERT INTO t (k, v) VALUES (?, ?)"
    [SQL.SInt (fromIntegral i), SQL.SText (Utf8 "value")]

countRows :: SQLiteEnv -> IO Int
countRows db = SQL.qry_ db "SELECT count(*) FROM t" [SQL.RInt] >>= \case
    [[SQL.SInt n]] -> return (fromIntegral n)
    r -> assertFailure ("count: " <> show r)

integrityOk :: SQLiteEnv -> IO ()
integrityOk db = SQL.qry_ db "PRAGMA integrity_check" [SQL.RText] >>= \case
    [[SQL.SText (Utf8 "ok")]] -> return ()
    r -> assertFailure ("integrity_check: " <> show r)

journalMode :: SQLiteEnv -> IO ()
journalMode db = SQL.qry_ db "PRAGMA journal_mode" [SQL.RText] >>= \case
    [[SQL.SText (Utf8 "wal")]] -> return ()
    r -> assertFailure ("journal_mode: " <> show r)

openConnectionRenameTest :: Assertion
openConnectionRenameTest = withRoot $ \r -> withLogger $ \logger _ -> do
    l <- resolvePactLayout logger HistorySplit False r
    createDirectoryIfMissing True (sqliteDir r)
    db <- withSqliteDirLock l (openDb . dbFile)
    journalMode db
    SQL.exec_ db "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)"
    forM_ [1 .. 100] $ insertRow db
    ensureSplitDir logger l cid0 10
    -- commits after the rename, through the open connection
    forM_ [101 .. 200] $ insertRow db
    SQL.exec_ db "BEGIN" >> forM_ [201 .. 300] (insertRow db) >> SQL.exec_ db "COMMIT"
    countRows db >>= (@?= 300)
    closeSQLiteConnection db
    -- nothing was recreated at the old path
    doesDirectoryExist (sqliteDir r) >>= assertBool "sqlite exists again" . not
    db' <- withSqliteDirLock l $ \d -> do
        d @?= splitDir r
        openDb (dbFile d)
    countRows db' >>= (@?= 300)
    integrityOk db'
    closeSQLiteConnection db'

twoPathsTest :: Assertion
twoPathsTest = withRoot $ \r -> withLogger $ \logger _ -> do
    l <- resolvePactLayout logger HistorySplit False r
    createDirectoryIfMissing True (sqliteDir r)
    old <- withSqliteDirLock l (openDb . dbFile)
    SQL.exec_ old "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)"
    forM_ [1 .. 10] $ insertRow old
    ensureSplitDir logger l cid0 10
    new <- withSqliteDirLock l (openDb . dbFile)
    countRows new >>= (@?= 10)
    -- commits through the old path are seen at the new one, and back
    forM_ [11 .. 20] $ insertRow old
    countRows new >>= (@?= 20)
    forM_ [21 .. 30] $ insertRow new
    countRows old >>= (@?= 30)
    closeSQLiteConnection old
    countRows new >>= (@?= 30)
    integrityOk new
    closeSQLiteConnection new
    db <- openDb (dbFile (splitDir r))
    countRows db >>= (@?= 30)
    integrityOk db
    closeSQLiteConnection db

-- -------------------------------------------------------------------------- --
-- Reset

resetTest :: Assertion
resetTest = withLogger $ \logger _ -> do
    forM_ [HistorySplit, HistoryShadow] $ \mode -> withRoot $ \r -> do
        mkDataDir (sqliteDir r)
        try (resolvePactLayout logger mode True r) >>= \case
            Left e -> do
                e @?= ResetRefused mode
                let msg = pactLayoutErrorMessage e
                assertBool ("message does not name the mode: " <> T.unpack msg)
                    (("--pact-history-store " <> pactHistoryStoreToText mode) `T.isInfixOf` msg)
            Right _ -> assertFailure $ "reset accepted with " <> show mode
        -- nothing was touched
        doesDirectoryExist (sqliteDir r) >>= assertBool "sqlite removed"
        doesDirectoryExist (splitDir r) >>= assertBool "sqlite-split created" . not
    -- sqlite mode keeps the reset of 3.2.1 (startSqliteDb removes the directory)
    withRoot $ \r -> do
        mkDataDir (sqliteDir r)
        l <- resolvePactLayout logger HistorySqlite True r
        currentSqliteDir l >>= (@?= sqliteDir r)
    -- the backup refusal names the mode too
    forM_ [HistorySplit, HistoryShadow] $ \mode ->
        assertBool "backup message" $ pactHistoryStoreToText mode
            `T.isInfixOf` pactLayoutErrorMessage (BackupRefused mode)
