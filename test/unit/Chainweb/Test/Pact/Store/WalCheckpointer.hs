{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Test.Pact.Store.WalCheckpointer
-- License: MIT
-- Stability: experimental
--
-- The background WAL checkpointer: a thread on its own connection
-- checkpoints, the Pact connection's commits keep only a backstop; the WAL
-- stays bounded, and restarts after a reader that pinned it; a clean stop
-- checkpoints the WAL to empty; the polls never drop SQLite's shm locks.
--
module Chainweb.Test.Pact.Store.WalCheckpointer
( tests
) where

import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, try)
import Control.Monad

import qualified Data.ByteString as B
import Data.Maybe (fromMaybe)
import Data.Text (Text)

import GHC.Clock (getMonotonicTime)

import System.Directory (doesFileExist, getFileSize)
import Foreign.C.Types (CInt(..))

import System.Exit (ExitCode(..))
import System.FilePath
import System.IO (IOMode(..), SeekMode(..), withBinaryFile)
import System.IO.Temp (withSystemTempDirectory)
import System.LogLevel (LogLevel)
import System.Posix.IO (LockRequest(..), OpenMode(..), defaultFileFlags, getLock, openFd)
import System.Posix.Process (ProcessStatus(..), forkProcess, getProcessStatus)

import Test.Tasty
import Test.Tasty.HUnit

import qualified Pact.Types.SQLite as P4SQL

-- internal modules

import Chainweb.Pact.Backend.Store.Config
import Chainweb.Pact.Backend.Store.WalCheckpointer
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils (chainwebPragmas, withSQLiteConnection)

tests :: TestTree
tests = testGroup "WAL checkpointer"
    [ testCase "the thread checkpoints, the writer's autocheckpoint is only the backstop (restartFrames), and the WAL stays bounded" backgroundTest
    , testCase "a reader pinning the WAL: the WAL grows while it reads, the writer does not wait, and the WAL restarts once the reader ends" pinnedTest
    , testCase "stop: the final checkpoint leaves an empty WAL, the writer's autocheckpoint is back, the rows are in the database file" stopTest
    , testCase "the polls keep SQLite's shm locks: another process still sees the dead-man switch lock held (and a descriptor closed on the -shm file would drop it)" shmLockTest
    , testCase "journal_size_limit: a WAL that a reader pinned past the limit shrinks back to it after the restart" journalSizeLimitTest
    , testCase "liveness: alive with a recent checkpoint call while it runs, not alive after it stops" livenessTest
    ]

quiet :: LogLevel -> Text -> IO ()
quiet _ _ = return ()

fastConfig :: WalCheckpointConfig
fastConfig = defaultWalCheckpointConfig
    { _wccPollMs = 10
    , _wccIntervalMs = 200
    , _wccSoftFrames = 200
    , _wccRestartFrames = 400
    , _wccJournalSizeLimitBytes = 512 * 1024
    }

withDb :: (FilePath -> SQLiteEnv -> IO a) -> IO a
withDb k = withSystemTempDirectory "wal-ckpt" $ \tmp -> do
    let path = tmp </> "pact-v1-chain-0.sqlite"
    withSQLiteConnection path chainwebPragmas $ \db -> do
        P4SQL.exec_ db "CREATE TABLE t (k INTEGER PRIMARY KEY, v BLOB)"
        k path db

-- | One transaction of @n@ rows of 2 KB.
writeRows :: SQLiteEnv -> Int -> Int -> IO ()
writeRows db from n = do
    P4SQL.exec_ db "BEGIN"
    forM_ [from .. from + n - 1] $ \i ->
        P4SQL.exec' db "INSERT OR REPLACE INTO t VALUES (?, ?)" [P4SQL.SInt (fromIntegral i), P4SQL.SBlob (B.replicate 2000 0x61)]
    P4SQL.exec_ db "COMMIT"

autocheckpoint :: SQLiteEnv -> IO Int
autocheckpoint db = P4SQL.qry_ db "PRAGMA wal_autocheckpoint" [P4SQL.RInt] >>= \case
    [[P4SQL.SInt n]] -> return (fromIntegral n)
    r -> fail (show r)

walBytes :: FilePath -> IO Integer
walBytes path = doesFileExist (path <> "-wal") >>= \case
    True -> getFileSize (path <> "-wal")
    False -> return 0

statsOf :: WalCheckpointer -> Text -> IO WalStats
statsOf cp label = fromMaybe (error "no stats") . lookup label <$> walCheckpointStats cp

-- | Polls until the condition holds, up to 10 s.
eventually :: IO String -> String -> IO Bool -> IO ()
eventually ctx msg cond = go (100 :: Int)
  where
    go 0 = ctx >>= \c -> assertFailure ("timed out: " <> msg <> "; " <> c)
    go n = cond >>= \case
        True -> return ()
        False -> threadDelay 100_000 >> go (n - 1)

ctxOf :: WalCheckpointer -> FilePath -> IO String
ctxOf cp path = do
    st <- walCheckpointStats cp
    w <- walBytes path
    return $ "stats " <> show st <> ", WAL bytes " <> show w

backgroundTest :: Assertion
backgroundTest = withDb $ \path db ->
    withWalCheckpointer quiet fastConfig $ \cp ->
        withWalCheckpoint cp "chain 0" db (\f -> f path) $ do
            autocheckpoint db >>= (@?= 400)
            forM_ [0 .. 599] $ \i -> writeRows db (i * 2) 2 >> threadDelay 2_000
            eventually (ctxOf cp path) "a background checkpoint" $ (> 0) . _wstFrames <$> statsOf cp "chain 0"
            st <- statsOf cp "chain 0"
            assertBool ("checkpoints: " <> show st) (_wstCheckpoints st >= 1)
            -- the WAL restarted behind the checkpoints (when a checkpoint
            -- ended between two transactions, or at the backstop): its high
            -- water is near the backstop, far below the frames written
            assertBool ("WAL high water: " <> show st) (_wstWalFramesMax st < 800)
            assertBool ("frames: " <> show st) (_wstFrames st > 2 * _wstWalFramesMax st)
            assertBool ("duration: " <> show st) (_wstMaxMs st >= 0 && _wstTotalMs st >= _wstMaxMs st)

-- | The WAL's length in frames, from a PASSIVE checkpoint on a connection
-- of its own.
walLength :: FilePath -> IO Int
walLength path = withSQLiteConnection path [] $ \c ->
    P4SQL.qry_ c "PRAGMA wal_checkpoint(PASSIVE)" [P4SQL.RInt, P4SQL.RInt, P4SQL.RInt] >>= \case
        [[_, P4SQL.SInt n, _]] -> return (fromIntegral n)
        r -> fail (show r)

pinnedTest :: Assertion
pinnedTest = withDb $ \path db ->
    withWalCheckpointer quiet fastConfig $ \cp ->
        withWalCheckpoint cp "chain 0" db (\f -> f path) $
            withSQLiteConnection path chainwebPragmas $ \reader -> do
                writeRows db 0 10
                -- the reader's snapshot pins the WAL: no checkpoint past it,
                -- no restart
                P4SQL.exec_ reader "BEGIN"
                _ <- P4SQL.qry_ reader "SELECT count(*) FROM t" [P4SQL.RInt]
                t0 <- getMonotonicTime
                forM_ [1 .. 150] $ \i -> writeRows db (i * 10) 10 >> threadDelay 1_000
                t1 <- getMonotonicTime
                eventually (ctxOf cp path) "the thread saw the WAL past the backstop" $
                    (> 0) . _wstOverRestart <$> statsOf cp "chain 0"
                n <- walLength path
                assertBool ("the WAL grows while the reader reads: " <> show n) (n >= 400)
                -- the writer does not wait for the checkpointer: 150 commits
                -- of 10 rows and 1 ms pauses
                assertBool ("writer time " <> show (t1 - t0)) (t1 - t0 < 5)
                P4SQL.exec_ reader "COMMIT"
                -- once the reader ends, the next commits restart the WAL
                eventually (ctxOf cp path) "the WAL restarted" $ do
                    writeRows db 200_000 1
                    (< 400) <$> walLength path
                eventually (ctxOf cp path) "a restart seen by the thread" $
                    (> 0) . _wstRestarts <$> statsOf cp "chain 0"

stopTest :: Assertion
stopTest = withDb $ \path db -> do
    (n, st) <- withWalCheckpointer quiet fastConfig { _wccSoftFrames = 1_000_000, _wccRestartFrames = 1_000_000, _wccIntervalMs = 3_600_000 } $ \cp -> do
        withWalCheckpoint cp "chain 0" db (\f -> f path) $
            forM_ [0 .. 99] $ \i -> writeRows db (i * 5) 5
        -- the thread checkpointed nothing during the run (thresholds out of
        -- reach); the stop did
        (,) <$> walCheckpointerActive cp <*> statsOf cp "chain 0"
    n @?= 0
    assertEqual "final checkpoint busy" (Just False) (_wstFinalBusy st)
    walBytes path >>= (@?= 0)
    autocheckpoint db >>= (@?= 1000)
    -- a new connection reads every row from the database file
    withSQLiteConnection path chainwebPragmas $ \db2 ->
        P4SQL.qry_ db2 "SELECT count(*) FROM t" [P4SQL.RInt] >>= (@?= [[P4SQL.SInt 500]])

-- | Review finding I1 of 8eb132960: the thread read the wal-index header
-- by opening and closing the @-shm@ file. Closing any descriptor of a file
-- drops every POSIX record lock the process holds on it: SQLite's shm locks
-- (the dead-man switch, DMS, a shared lock on byte 128 of the @-shm@ file,
-- held by every connection while it has the file open; the read marks; the
-- WAL write lock). Another process then finds DMS free, takes itself for
-- the first user, and resets the wal-index under the node. The check runs
-- in a forked child: @F_GETLK@ reports only another process's locks. It
-- runs against the SQLite the node links (the amalgamation of
-- @direct-sqlite@, classic @fcntl@ locks), not a system library. A control
-- on a second file shows that the check sees a dropped lock here.
shmLockTest :: Assertion
shmLockTest = do
    withDb $ \path db -> do
        writeRows db 0 10
        dmsHeld path >>= assertEqual "before: the writer holds DMS" (Just True)
        withWalCheckpointer quiet fastConfig $ \cp ->
            withWalCheckpoint cp "chain 0" db (\f -> f path) $ do
                -- many polls (10 ms), with and without frames pending
                forM_ [1 .. 40] $ \i -> writeRows db (i * 10) 10 >> threadDelay 5_000
                threadDelay 200_000
                eventually (ctxOf cp path) "a background checkpoint" $ (> 0) . _wstFrames <$> statsOf cp "chain 0"
                dmsHeld path >>= assertEqual "after the polls: DMS still held" (Just True)
                writeRows db 1000 1
                threadDelay 100_000
                dmsHeld path >>= assertEqual "after more commits and polls: DMS still held" (Just True)
    -- control: one open and close of the -shm file in this process
    withDb $ \path db -> do
        writeRows db 0 1
        dmsHeld path >>= assertEqual "control: held at first" (Just True)
        withBinaryFile (path <> "-shm") ReadMode $ \h -> void (B.hGet h 136)
        dmsHeld path >>= assertEqual "control: a closed descriptor of the -shm file drops DMS" (Just False)

-- | Whether another process sees a lock on the DMS byte of the file's
-- @-shm@ ('Nothing': the check itself failed).
dmsHeld :: FilePath -> IO (Maybe Bool)
dmsHeld path = do
    pid <- forkProcess $ do
        r <- try @SomeException $ do
            fd <- openFd (path <> "-shm") ReadOnly defaultFileFlags
            getLock fd (WriteLock, AbsoluteSeek, 128, 1)
        -- _exit: no atexit handlers in the child (the parent's C++
        -- libraries' destructors would join threads the child does not have)
        c_exit $ case r of
            Right (Just _) -> 0
            Right Nothing -> 1
            Left _ -> 2
    getProcessStatus True False pid >>= \case
        Just (Exited ExitSuccess) -> return (Just True)
        Just (Exited (ExitFailure 1)) -> return (Just False)
        _ -> return Nothing

foreign import ccall unsafe "_exit"
    c_exit :: CInt -> IO ()

journalSizeLimitTest :: Assertion
journalSizeLimitTest = withDb $ \path db ->
    withWalCheckpointer quiet fastConfig $ \cp ->
        withWalCheckpoint cp "chain 0" db (\f -> f path) $
            withSQLiteConnection path chainwebPragmas $ \reader -> do
                writeRows db 0 10
                P4SQL.exec_ reader "BEGIN"
                _ <- P4SQL.qry_ reader "SELECT count(*) FROM t" [P4SQL.RInt]
                forM_ [1 .. 150] $ \i -> writeRows db (i * 10) 10
                pinned <- walBytes path
                assertBool ("the WAL grew past the limit: " <> show pinned) (pinned > 2 * 512 * 1024)
                P4SQL.exec_ reader "COMMIT"
                eventually (ctxOf cp path) "the WAL file shrank back to the limit" $ do
                    writeRows db 200_000 1
                    (<= 512 * 1024) <$> walBytes path

livenessTest :: Assertion
livenessTest = withDb $ \path db -> do
    cp' <- withWalCheckpointer quiet fastConfig $ \cp -> do
        withWalCheckpoint cp "chain 0" db (\f -> f path) $ do
            writeRows db 0 10
            eventually (ctxOf cp path) "a recent checkpoint call" $ do
                l <- walCheckpointerLiveness cp
                return $ _wclAlive l && maybe False (< 1) (_wclLastCheckpointAgeSeconds l)
            -- idle: the calls go on (nothing to copy)
            threadDelay 1_500_000
            l <- walCheckpointerLiveness cp
            assertBool ("idle: " <> show l) (_wclAlive l && maybe False (< 1) (_wclLastCheckpointAgeSeconds l))
        return cp
    l <- walCheckpointerLiveness cp'
    assertBool ("stopped: " <> show l) (not (_wclAlive l))
