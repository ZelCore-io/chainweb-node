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
import Control.Concurrent.Async (waitCatch, withAsync)
import Control.Exception (SomeException, try)
import Control.Monad

import qualified Data.ByteString as B
import Data.IORef
import Data.Maybe (fromMaybe, isJust, isNothing)
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
    , testCase "a checkpoint before each trim chunk: under back-to-back commits with the thread idle, the WAL stays far under the backstop (it reaches it without), and is skipped while the WAL is short" trimCheckpointTest
    , testCase "the two phases before a trim chunk: under the lock only while the WAL is long; a pinned reader leaves frames and the chunk goes on; the thread at the file gives Busy without a wait, under a concurrent writer; the freezer's calls do not refresh the liveness" twoPhaseTest
    , testCase "the first phase is skipped while the thread's schedule covers the file (right after its poll found a short WAL), and runs when the thread is overdue or the WAL was long" skipRuleTest
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

-- | Back-to-back commits (blocks) with a trim chunk after each three, the
-- thread asleep (its first poll ten minutes away: the file's first PASSIVE
-- is due at the registration, and the first phase's calls then schedule
-- the next ten minutes later). With 'checkpointBeforeLock' and
-- 'checkpointUnderLock' before each chunk (the freezer holds the writer's
-- lock for the second), every chunk's write restarts the WAL: the WAL
-- never comes near the backstop, so no commit checkpoints inside it.
-- Without, the WAL grows to the backstop and a commit runs it.
trimCheckpointTest :: Assertion
trimCheckpointTest = do
    -- an unregistered writer
    withDb $ \_ db -> withWalCheckpointer quiet idleCfg $ \cp -> do
        checkpointBeforeLock cp db >>= (@?= Nothing)
        checkpointUnderLock cp db >>= (@?= Nothing)

    without <- run False
    with <- run True
    let pageFrame = 4096 + 24
        backstopBytes = fromIntegral (_wccRestartFrames idleCfg * pageFrame) :: Integer
        (wBytes, _) = without
        (cBytes, results) = with
    assertBool ("without: the WAL reached the backstop, " <> show wBytes <> " bytes") (wBytes >= backstopBytes)
    -- a few rounds' frames at most: a chunk whose first phase was skipped
    -- (the WAL short at the last call) leaves the WAL to the next chunk
    assertBool ("with: the WAL stayed under 3/4 of the backstop, " <> show cBytes <> " bytes; " <> show results) (4 * cBytes < 3 * backstopBytes)
    -- every chunk had a checkpoint that copied every frame, and the WAL it
    -- found (the frames since the last chunk's restart) is short
    length results @?= rounds
    forM_ results $ \(outside, under) -> do
        case outside of
            Just (WriterCheckpointRan _ l complete) -> do
                assertBool "complete" complete
                assertBool ("WAL frames before the chunk " <> show l) (4 * l < 3 * _wccRestartFrames idleCfg)
            Just WriterCheckpointSkipped -> return ()
            r -> assertFailure ("not run: " <> show r)
        -- the first phase drained a WAL under a quarter of the backstop:
        -- nothing to do under the lock (a first phase that met the thread
        -- can leave a long WAL to the second)
        case (outside, under) of
            (Just (WriterCheckpointRan _ l _), r@(Just (WriterCheckpointRan {}))) | l < underLockFrames idleCfg ->
                assertFailure ("under the lock after a short WAL: " <> show r)
            _ -> return ()
    -- the first phase follows the skip rule: the thread never polls, so
    -- the first chunk's call is overdue (due at the registration) and runs;
    -- each later one is not due, and runs exactly when the call before it
    -- found the WAL at or past soft
    let firsts = fst <$> results
        expected prev = case prev of
            Just (WriterCheckpointRan _ l _) | l < _wccSoftFrames idleCfg -> False
            _ -> True
        ran = \case
            Just (WriterCheckpointRan {}) -> True
            _ -> False
    assertBool ("the first chunk's first phase ran: " <> show results) (ran (head firsts))
    forM_ (zip firsts (drop 1 firsts)) $ \(prev, cur) ->
        assertBool ("the skip rule after " <> show prev <> ": " <> show cur) (ran cur == expected prev)
    -- no PASSIVE under the lock in most rounds: skipped, or the thread at
    -- the file (under load the first phase's call reschedules the thread
    -- soon, and a slow PASSIVE of it holds the file)
    assertBool ("most chunks ran no second phase: " <> show results)
        (2 * length [ () | (_, Just r) <- results, r `elem` [WriterCheckpointSkipped, WriterCheckpointBusy] ] >= rounds)
  where
    rounds = 20 :: Int
    idleCfg = fastConfig
        { _wccPollMs = 600_000
        , _wccIntervalMs = 3_600_000
        , _wccSoftFrames = 100
        , _wccRestartFrames = 2000
        , _wccJournalSizeLimitBytes = 1024 * 1024 * 1024
        }
    run trimCkpt = withDb $ \path db -> withWalCheckpointer quiet idleCfg $ \cp ->
        withWalCheckpoint cp "t" db (\f -> f path) $ do
            rs <- forM [0 .. rounds - 1] $ \r -> do
                -- three block commits of about 60 frames each
                forM_ [0 .. 2] $ \b -> writeRows db (100_000 + r * 1000 + b * 200) 120
                -- the trim chunk, under the writer's lock
                res <- if trimCkpt
                    then (,) <$> checkpointBeforeLock cp db <*> checkpointUnderLock cp db
                    else return (Nothing, Nothing)
                writeRows db (r * 10) 10
                return res
            st <- statsOf cp "t"
            -- the thread stayed asleep: nothing copied by it
            unless trimCkpt $ _wstCheckpoints st @?= 0
            w <- walBytes path
            return (w, rs)

-- | The two phases ('checkpointBeforeLock', 'checkpointUnderLock') with a
-- reader that pins the WAL, and with the thread at the file under a
-- concurrent writer.
twoPhaseTest :: Assertion
twoPhaseTest = do
    -- 0. the freezer's calls say nothing of the thread: with the thread
    -- asleep (its first poll ten minutes away), they run, and the thread
    -- still has no checkpoint call of its own
    withDb $ \path db -> withWalCheckpointer quiet pinCfg $ \cp ->
      withWalCheckpoint cp "t" db (\f -> f path) $ do
        forM_ [0 .. 3 :: Int] $ \b -> writeRows db (b * 200) 200
        checkpointBeforeLock cp db >>= \case
            Just (WriterCheckpointRan {}) -> return ()
            r -> assertFailure ("first phase: " <> show r)
        _ <- checkpointUnderLock cp db
        l <- walCheckpointerLiveness cp
        assertBool ("liveness: " <> show l) (_wclAlive l && isNothing (_wclLastCheckpointAgeSeconds l))

    -- 1. a pinned reader, the thread asleep (its first poll ten minutes
    -- away: no call is Busy), the first phase never skipped
    withDb $ \path db -> withWalCheckpointer quiet pinCfg $ \cp ->
      withWalCheckpoint cp "t" db (\f -> f path) $ do
        withSQLiteConnection path chainwebPragmas $ \reader -> do
            writeRows db 0 10
            P4SQL.exec_ reader "BEGIN"
            _ <- P4SQL.qry_ reader "SELECT count(*) FROM t" [P4SQL.RInt]
            -- a long WAL behind the reader's snapshot: over a quarter of
            -- the backstop
            forM_ [0 .. 7 :: Int] $ \b -> writeRows db (1000 + b * 200) 200
            o1 <- checkpointBeforeLock cp db
            l1 <- case o1 of
                Just (WriterCheckpointRan _ l complete) -> do
                    assertBool "pinned: frames left" (not complete)
                    return l
                r -> assertFailure ("first phase, pinned: " <> show r)
            assertBool ("a long WAL: " <> show l1) (l1 >= underLockFrames idleCfg)
            -- long: the second phase runs, and is kept from finishing too;
            -- the chunk goes on (a write does not wait)
            checkpointUnderLock cp db >>= \case
                Just (WriterCheckpointRan _ l complete) -> do
                    assertBool "pinned under the lock: frames left" (not complete)
                    l @?= l1
                r -> assertFailure ("second phase, pinned: " <> show r)
            writeRows db 5000 10
            P4SQL.exec_ reader "COMMIT"
        -- the reader is gone: the first phase copies every frame, a block
        -- commit lands between the phases, and the second runs since the
        -- last known length is long; it copies that commit's frames
        o2 <- checkpointBeforeLock cp db
        case o2 of
            Just (WriterCheckpointRan _ l complete) -> do
                assertBool "complete" complete
                assertBool ("long: " <> show l) (l >= underLockFrames idleCfg)
            r -> assertFailure ("first phase: " <> show r)
        writeRows db 6000 10
        checkpointUnderLock cp db >>= \case
            Just (WriterCheckpointRan _ _ complete) -> assertBool "under the lock: complete" complete
            r -> assertFailure ("second phase: " <> show r)
        -- the commit after the first phase restarted the WAL: the next
        -- first phase finds it short, and the second skips without I/O
        writeRows db 7000 10
        o3 <- checkpointBeforeLock cp db
        case o3 of
            Just (WriterCheckpointRan _ l complete) -> do
                assertBool "complete" complete
                assertBool ("restarted: " <> show l) (l < underLockFrames idleCfg)
            r -> assertFailure ("first phase, after the restart: " <> show r)
        checkpointUnderLock cp db >>= (@?= Just WriterCheckpointSkipped)
        st <- statsOf cp "t"
        _wstErrors st @?= 0
        _wstOverRestart st @?= 0

    -- 2. the thread at the file all the time (a PASSIVE every poll) under a
    -- concurrent writer: a call that finds it there returns Busy at once
    withDb $ \path db -> withWalCheckpointer quiet busyCfg $ \cp ->
      withWalCheckpoint cp "t" db (\f -> f path) $ do
        stop <- newIORef False
        let writer i = readIORef stop >>= \done -> unless done $
                writeRows db (i * 50 `mod` 20_000) 50 >> writer (i + 1)
        withAsync (writer (0 :: Int)) $ \w -> do
            t0 <- getMonotonicTime
            -- up to 10 s, until a call finds the thread at the file
            let go acc = do
                    a <- getMonotonicTime
                    r <- checkpointBeforeLock cp db
                    let acc' = r : acc
                    if r == Just WriterCheckpointBusy || a - t0 > 10 then return acc' else go acc'
            rs <- go []
            t1 <- getMonotonicTime
            writeIORef stop True
            waitCatch w >>= \case
                Left e -> assertFailure ("the writer failed: " <> show e)
                Right () -> return ()
            assertBool ("a Busy within " <> show (t1 - t0) <> " s, " <> show (length rs) <> " calls")
                (Just WriterCheckpointBusy `elem` rs)
            assertBool "no failure" (Just WriterCheckpointFailed `notElem` rs)
        st <- statsOf cp "t"
        _wstErrors st @?= 0
  where
    idleCfg = fastConfig
        { _wccPollMs = 20
        , _wccIntervalMs = 3_600_000
        , _wccSoftFrames = 100
        , _wccRestartFrames = 2000
        , _wccJournalSizeLimitBytes = 1024 * 1024 * 1024
        }
    pinCfg = idleCfg { _wccPollMs = 600_000, _wccSoftFrames = 0 }
    busyCfg = fastConfig
        { _wccPollMs = 1
        , _wccIntervalMs = 1
        , _wccSoftFrames = 1
        , _wccRestartFrames = 100_000
        , _wccJournalSizeLimitBytes = 1024 * 1024 * 1024
        }

-- | The first phase's skip rule ('checkpointBeforeLock'): skipped, with no
-- I/O, while the file's next PASSIVE is not yet due and the last call found
-- the WAL under '_wccSoftFrames'; run when the thread is overdue, or when
-- the last call found the WAL at or past soft.
skipRuleTest :: Assertion
skipRuleTest = do
    -- right after the thread's poll found a short WAL (its first poll finds
    -- it empty and schedules the next an hour later): skipped, both phases
    withDb $ \path db -> withWalCheckpointer quiet idleCfg $ \cp ->
      withWalCheckpoint cp "t" db (\f -> f path) $ do
        eventually (ctxOf cp path) "the thread's first poll"
            (isJust . _wclLastCheckpointAgeSeconds <$> walCheckpointerLiveness cp)
        st0 <- statsOf cp "t"
        writeRows db 0 10
        retryBusy (checkpointBeforeLock cp db) >>= (@?= Just WriterCheckpointSkipped)
        retryBusy (checkpointUnderLock cp db) >>= (@?= Just WriterCheckpointSkipped)
        -- no checkpoint call: the stats' time of all calls did not move
        st1 <- statsOf cp "t"
        _wstTotalMs st1 @?= _wstTotalMs st0

    -- the thread overdue (asleep, its first poll ten minutes away: the
    -- file's first PASSIVE is due at the registration): run; then not due
    -- and the WAL short: skipped
    withDb $ \path db -> withWalCheckpointer quiet sleepCfg $ \cp ->
      withWalCheckpoint cp "t" db (\f -> f path) $ do
        writeRows db 0 10
        checkpointBeforeLock cp db >>= \case
            Just (WriterCheckpointRan _ l complete) -> do
                assertBool "complete" complete
                assertBool ("short: " <> show l) (l < _wccSoftFrames sleepCfg)
            r -> assertFailure ("overdue: " <> show r)
        writeRows db 100 10
        checkpointBeforeLock cp db >>= (@?= Just WriterCheckpointSkipped)

    -- the WAL long at the last call: run although not due, until a call
    -- finds it short
    withDb $ \path db -> withWalCheckpointer quiet sleepCfg $ \cp ->
      withWalCheckpoint cp "t" db (\f -> f path) $ do
        forM_ [0 .. 3 :: Int] $ \b -> writeRows db (b * 200) 200
        l1 <- checkpointBeforeLock cp db >>= \case
            Just (WriterCheckpointRan _ l _) -> return l
            r -> assertFailure ("overdue: " <> show r)
        assertBool ("long: " <> show l1) (l1 >= _wccSoftFrames sleepCfg)
        -- the next PASSIVE is now ten minutes away
        writeRows db 5000 10
        checkpointBeforeLock cp db >>= \case
            Just (WriterCheckpointRan _ l complete) -> do
                assertBool "complete" complete
                assertBool ("restarted, short: " <> show l) (l < _wccSoftFrames sleepCfg)
            r -> assertFailure ("long at the last call: " <> show r)
        writeRows db 6000 10
        checkpointBeforeLock cp db >>= (@?= Just WriterCheckpointSkipped)
        st <- statsOf cp "t"
        _wstErrors st @?= 0
  where
    -- (Busy when a call meets the thread's poll, which takes the file's
    -- lock even when nothing is due: again)
    retryBusy act = act >>= \case
        Just WriterCheckpointBusy -> threadDelay 1000 >> retryBusy act
        r -> return r
    idleCfg = fastConfig
        { _wccPollMs = 20
        , _wccIntervalMs = 3_600_000
        , _wccSoftFrames = 100
        , _wccRestartFrames = 2000
        , _wccJournalSizeLimitBytes = 1024 * 1024 * 1024
        }
    sleepCfg = idleCfg { _wccPollMs = 600_000 }
