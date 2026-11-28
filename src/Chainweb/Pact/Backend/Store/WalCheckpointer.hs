{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.WalCheckpointer
-- License: MIT
-- Stability: experimental
--
-- A background checkpointer for the Pact SQLite files (WAL mode).
--
-- With SQLite's autocheckpoint (1000 pages), the commit that takes the WAL
-- past 1000 pages copies the WAL into the database file and syncs both,
-- inside that commit: a block commit or a trim chunk pays tens to hundreds
-- of milliseconds now and then (gate B run 2: the worst trim chunk of every
-- chain was 97 to 99.7 % commit). Here one node-wide thread checkpoints
-- every registered file on a connection of its own, and the writer's
-- autocheckpoint moves up to '_wccRestartFrames', a backstop that normal
-- operation does not reach:
--
-- * Every '_wccPollMs' the thread looks at each file whose next checkpoint
--   is due, and runs @wal_checkpoint(PASSIVE)@ on its own connection. The
--   result, the WAL's length in frames and the frames checkpointed, is all
--   it reads: it never opens the @-shm@ (or @-wal@) file itself. Closing
--   any descriptor of a file drops every POSIX record lock the process
--   holds on it, SQLite's shm locks included (the dead-man switch at byte
--   128 of the @-shm@ file, the read marks, the WAL write lock): another
--   process would then take itself for the file's first user, reset the
--   wal-index under the node's readers and run WAL recovery. A PASSIVE
--   with nothing pending tries the checkpoint lock, finds every frame
--   copied and returns: no I/O, no sync.
--
--   The next checkpoint of a file is due when '_wccSoftFrames' frames are
--   expected to be pending, from the rate the WAL grew at between the last
--   two checkpoints, and at the latest '_wccIntervalMs' after the last one,
--   but not before the next poll. A checkpoint that found the WAL restarted
--   since the last one does not know the rate (the frames since the
--   restart do not span the time since the last call): the next one is
--   due at the next poll. A checkpoint that found nothing to copy
--   is followed by one at the next poll at or after '_wccIntervalMs' since
--   the last one that copied (the first frames after a quiet time are
--   copied within '_wccPollMs' of their commit). A PASSIVE that a reader
--   kept from finishing is followed by one at the interval. PASSIVE takes
--   no lock that a writer or reader waits for: commits go on during it.
-- * The WAL stays bounded. SQLite restarts the WAL from its start at the
--   first write of a transaction that began with every frame checkpointed.
--   At the tip, with seconds between blocks, the thread's checkpoint is
--   done long before the next block, so the WAL restarts every time and no
--   commit checkpoints. While transactions follow each other closely (a
--   replay, the trim), a background checkpoint may end only while the next
--   transaction runs, and the WAL then keeps growing. The writer's
--   autocheckpoint at '_wccRestartFrames' bounds it: the commit that takes
--   the WAL there checkpoints, which copies only the frames written since
--   the thread's last checkpoint (about one poll), and the next transaction
--   restarts the WAL. So a commit pays a small checkpoint once per
--   '_wccRestartFrames' frames at most, against a full 1000-page checkpoint
--   once per 1000 frames before. The backstop is SQLite's own mechanism, so
--   it holds when the thread is slow or dead.
--
--   The freezer keeps the backstop out of its trim chunks in two phases,
--   both PASSIVE on this thread's connection and schedule, neither waiting
--   for the thread. First, before it takes the Pact connection's lock,
--   'checkpointBeforeLock' copies the bulk of the WAL and syncs while
--   block commits go on (skipped while the thread's schedule covers the
--   file: its next PASSIVE is not yet due and the last call found fewer
--   than '_wccSoftFrames' frames; so it runs only when the thread is
--   overdue, Busy-stalled or behind). It
--   leaves the frames committed while it runs, or that a reader pins, and
--   the WAL then does not restart at the chunk's write. (A commit after a
--   complete first phase restarts the WAL itself.) So second, under the lock (no Pact
--   transaction open), 'checkpointUnderLock' copies what is left, only
--   while the WAL is long: the last known length (that first call's, or
--   the thread's if later) at or over a quarter of '_wccRestartFrames'.
--   That PASSIVE copies every frame unless a reader pins some, so the
--   chunk's write restarts the WAL and its commit does not run the
--   backstop; a shorter WAL is far enough from the backstop that a chunk
--   cannot reach it, and the next chunk checks again. The lock is so held
--   for a checkpoint about once per quarter backstop of frames, and then
--   only for the frames written since the first phase.
--
--   Considered and rejected: a RESTART or TRUNCATE checkpoint from the
--   thread at a hard limit. It takes the write lock and waits for readers
--   while holding it, and the Pact connections have no busy timeout: a
--   block commit in that window fails with @SQLITE_BUSY@. Also rejected: an
--   autocheckpoint of 0 with the thread alone, since the WAL then grows
--   without bound in a replay (measured: no restart in a write loop).
--
--   A reader pins the WAL: no checkpoint passes the snapshot of an open
--   read transaction, and no restart happens while it reads, whichever
--   connection checkpoints (SQLite's autocheckpoint is PASSIVE too). The WAL
--   then grows for as long as the reader reads (a freezer step read, a
--   backup), as it does with SQLite's autocheckpoint; it is logged at Warn
--   past twice '_wccRestartFrames', and restarts after the reader ends.
-- * A file's registration ends ('withWalCheckpoint') with a final TRUNCATE
--   checkpoint (the writer is done then; the busy timeout is
--   '_wccFinalBusyMs'), the writer's autocheckpoint back at 1000 pages, and
--   its stats logged. The thread stops with 'withWalCheckpointer'.
-- * Disk: the @-wal@ file keeps its high water. While the thread serves a
--   file, the writer's @journal_size_limit@ is '_wccJournalSizeLimitBytes'
--   (64 MiB, the backstop's size at 4 KiB pages): the first commit after a
--   WAL restart truncates a longer file to it, so a WAL that a reader
--   pinned past the backstop shrinks back. At rest each served file's WAL
--   is at most that size: 64 MiB per chain in split mode, twice that in
--   shadow mode (hot and legacy), about 1.3 and 2.6 GB on a 20-chain node,
--   plus whatever a long reader pins for as long as it reads.
-- * Liveness ('walCheckpointerLiveness', in the freezer's stats): whether
--   the thread runs, and the time since its last successful checkpoint
--   call (a PASSIVE that found nothing to copy counts: the thread works).
--   The freezer's calls do not count: they say nothing of the thread.
--
-- The WAL semantics are unchanged, so crash safety is: a commit is durable
-- under the same rules (@synchronous = NORMAL@), and a crash before a
-- checkpoint leaves the frames in the WAL for recovery, as with the
-- autocheckpoint.
--
-- One thread serves all files (one or two per chain), one at a time: the
-- checkpoints' syncs do not compete with each other.
--
module Chainweb.Pact.Backend.Store.WalCheckpointer
( WalCheckpointer
, WalStats(..)
, withWalCheckpointer
, withWalCheckpoint
, walCheckpointStats
, walCheckpointerActive
, WalCheckpointerLiveness(..)
, walCheckpointerLiveness
, WriterCheckpoint(..)
, checkpointBeforeLock
, checkpointUnderLock
, underLockFrames
) where

import Control.Concurrent
import Control.Concurrent.Async (withAsync)
import Control.Exception
import Control.Monad

import Data.Aeson (object, (.=))
import qualified Data.Aeson as A
import qualified Data.ByteString.Lazy as BL
import Data.IORef
import qualified Data.Map.Strict as M
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import GHC.Clock (getMonotonicTime)

import System.Directory (doesFileExist, getFileSize)
import System.LogLevel (LogLevel(..))

import qualified Pact.Types.SQLite as P4SQL

import Database.SQLite3.Bindings.Types (CDatabase)
import Database.SQLite3.Direct (Database(..))

import Foreign.C.Types (CInt(..))
import Foreign.Ptr (Ptr)

-- internal modules

import Chainweb.Pact.Backend.Store.Config
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils (closeSQLiteConnection, openSQLiteConnection)

-- -------------------------------------------------------------------------- --
-- Types

-- | The node-wide checkpointer.
data WalCheckpointer = WalCheckpointer
    { _wcConfig :: !WalCheckpointConfig
    , _wcLog :: !(LogLevel -> Text -> IO ())
    , _wcFiles :: !(MVar (M.Map Int WalFile))
    , _wcNext :: !(IORef Int)
    , _wcFinished :: !(IORef (M.Map Text WalStats))
    , _wcAlive :: !(IORef Bool)
        -- ^ the thread runs
    , _wcLastOk :: !(IORef (Maybe Double))
        -- ^ the time of the thread's last checkpoint call that returned
    }

-- | Whether the thread runs, and the seconds since its last checkpoint call
-- that returned ('Nothing' before the first).
data WalCheckpointerLiveness = WalCheckpointerLiveness
    { _wclAlive :: !Bool
    , _wclLastCheckpointAgeSeconds :: !(Maybe Double)
    } deriving (Show, Eq)

data WalFile = WalFile
    { _wfLabel :: !Text
    , _wfAtPath :: !AtPath
    , _wfWriter :: !SQLiteEnv
    , _wfConn :: !SQLiteEnv
    , _wfLock :: !(MVar Bool)
        -- ^ held during a checkpoint; 'False' once closed
    , _wfStats :: !(IORef WalStats)
    , _wfLastCall :: !(IORef (Double, Int, Int))
        -- ^ the time of the last PASSIVE, the WAL's length in frames and
        -- the frames checkpointed it returned
    , _wfLastCopy :: !(IORef Double)
        -- ^ the time of the last PASSIVE that copied frames
    , _wfDue :: !(IORef Double)
        -- ^ when the next PASSIVE is due
    , _wfLastLog :: !(IORef Double)
    , _wfLastWarn :: !(IORef Double)
    }

-- | Runs an action with the file's current path, under whatever lock the
-- path needs (the rename lock of the SQLite directory).
newtype AtPath = AtPath (forall b. (FilePath -> IO b) -> IO b)

-- | A file's checkpoint stats since its registration.
data WalStats = WalStats
    { _wstCheckpoints :: !Int
        -- ^ PASSIVE checkpoints that copied frames
    , _wstFrames :: !Int
        -- ^ frames copied into the database file
    , _wstBusy :: !Int
        -- ^ PASSIVE checkpoints that left frames behind (a reader pinned them)
    , _wstTotalMs :: !Double
        -- ^ the time of all checkpoint calls, those with nothing to copy
        -- included
    , _wstMaxMs :: !Double
        -- ^ the longest checkpoint (PASSIVE or TRUNCATE)
    , _wstWalFramesMax :: !Int
        -- ^ the high water of the WAL's length in frames, as the PASSIVE
        -- checkpoints saw it
    , _wstWalBytesMax :: !Integer
        -- ^ the high water of the @-wal@ file's size
    , _wstRestarts :: !Int
        -- ^ WAL restarts seen (its length went down between two PASSIVE
        -- checkpoints)
    , _wstOverRestart :: !Int
        -- ^ PASSIVE checkpoints that saw the WAL at or past
        -- '_wccRestartFrames': the
        -- writer's backstop checkpoint was due, or a reader pinned the WAL
    , _wstErrors :: !Int
    , _wstFinalBusy :: !(Maybe Bool)
        -- ^ the final checkpoint at the end of the registration: whether it
        -- was kept from completing
    } deriving (Show, Eq)

emptyWalStats :: WalStats
emptyWalStats = WalStats 0 0 0 0 0 0 0 0 0 0 Nothing

walStatsJson :: Text -> WalStats -> A.Value
walStatsJson label s = object
    [ "WalCheckpointStats" .= object
        [ "file" .= label
        , "checkpoints" .= _wstCheckpoints s
        , "frames" .= _wstFrames s
        , "busy" .= _wstBusy s
        , "totalMs" .= _wstTotalMs s
        , "maxMs" .= _wstMaxMs s
        , "walFramesMax" .= _wstWalFramesMax s
        , "walBytesMax" .= _wstWalBytesMax s
        , "restarts" .= _wstRestarts s
        , "overRestart" .= _wstOverRestart s
        , "errors" .= _wstErrors s
        , "finalBusy" .= _wstFinalBusy s
        ]
    ]

-- -------------------------------------------------------------------------- --
-- Scheduler

-- | Starts the thread; it stops when the action returns. Registrations
-- ('withWalCheckpoint') must end before.
withWalCheckpointer
    :: (LogLevel -> Text -> IO ())
    -> WalCheckpointConfig
    -> (WalCheckpointer -> IO a)
    -> IO a
withWalCheckpointer logFn cfg k = do
    cp <- WalCheckpointer cfg logFn
        <$> newMVar M.empty
        <*> newIORef 0
        <*> newIORef M.empty
        <*> newIORef True
        <*> newIORef Nothing
    withAsync (loop cp `finally` writeIORef (_wcAlive cp) False) $ \_ -> k cp
  where
    loop cp = forever (pass cp) `catch` \(e :: SomeException) -> case fromException e of
        Just (_ :: SomeAsyncException) -> throwIO e
        Nothing -> died cp e

    pass cp = do
        threadDelay (1000 * _wccPollMs cfg)
        files <- readMVar (_wcFiles cp)
        forM_ files (tick cp)

    -- the thread is gone: the writers' backstop autocheckpoint bounds the WAL
    died _ e = logFn Error $ "WAL checkpointer: the thread failed (" <> T.pack (displayException e)
        <> "); the writers checkpoint at their backstop of " <> T.pack (show (_wccRestartFrames cfg)) <> " frames"

-- | The files registered now.
walCheckpointerActive :: WalCheckpointer -> IO Int
walCheckpointerActive cp = M.size <$> readMVar (_wcFiles cp)

-- | Whether the thread runs, and the time since its last checkpoint call
-- that returned.
walCheckpointerLiveness :: WalCheckpointer -> IO WalCheckpointerLiveness
walCheckpointerLiveness cp = do
    alive <- readIORef (_wcAlive cp)
    lastOk <- readIORef (_wcLastOk cp)
    now <- getMonotonicTime
    return $ WalCheckpointerLiveness alive ((now -) <$> lastOk)

-- | The stats of the registered files and of the files whose registration
-- ended (their final stats), by label.
walCheckpointStats :: WalCheckpointer -> IO [(Text, WalStats)]
walCheckpointStats cp = do
    live <- readMVar (_wcFiles cp) >>= mapM (\f -> (,) (_wfLabel f) <$> readIORef (_wfStats f)) . M.elems
    done <- readIORef (_wcFinished cp)
    return $ live ++ [ x | x@(l, _) <- M.toList done, l `notElem` (fst <$> live) ]

-- -------------------------------------------------------------------------- --
-- Registration

-- | Checkpoints the file of the given writer connection in the background
-- while the action runs. The writer's autocheckpoint is off in that time.
-- The path action gives the file's current path under the lock its opens
-- need; the checkpointer's connection is opened, and its WAL touched,
-- inside it.
withWalCheckpoint
    :: WalCheckpointer
    -> Text
        -- ^ a label for the logs and stats
    -> SQLiteEnv
        -- ^ the writer
    -> (forall b. (FilePath -> IO b) -> IO b)
    -> IO a
    -> IO a
withWalCheckpoint cp label writer atPath act = bracket register unregister (const act)
  where
    cfg = _wcConfig cp

    register = do
        conn <- atPath $ \p -> do
            c <- openSQLiteConnection p []
            flip onException (closeSQLiteConnection c) $ do
                P4SQL.exec_ c "PRAGMA synchronous = NORMAL"
                P4SQL.exec_ c (fromString ("PRAGMA busy_timeout = " <> show (_wccFinalBusyMs cfg)))
                -- SQLite opens the -wal file at the first access: now, while
                -- the path is current
                r <- passive c
                return (c, r)
        flip onException (closeSQLiteConnection (fst conn)) $ do
          now <- getMonotonicTime
          let (_, l, k') = snd conn
          f <- WalFile label (AtPath atPath) writer (fst conn)
              <$> newMVar True
              <*> newIORef emptyWalStats
              <*> newIORef (now, max 0 l, max 0 k')
              <*> newIORef now
              <*> newIORef now
              <*> newIORef now
              <*> newIORef 0
          i <- atomicModifyIORef' (_wcNext cp) (\n -> (n + 1, n))
          setJournalSizeLimit writer (_wccJournalSizeLimitBytes cfg)
          setAutocheckpoint writer (_wccRestartFrames cfg)
          modifyMVar_ (_wcFiles cp) (return . M.insert i f)
          return (i, f)

    unregister (i, f) = do
        modifyMVar_ (_wcFiles cp) (return . M.delete i)
        modifyMVar_ (_wfLock f) $ \_ -> do
            r <- try @SomeException (timedCheckpoint (_wfConn f) "TRUNCATE")
            modifyIORef' (_wfStats f) $ \s -> case r of
                Right ((busy, _, _), ms) -> s { _wstFinalBusy = Just busy, _wstMaxMs = max (_wstMaxMs s) ms, _wstTotalMs = _wstTotalMs s + ms }
                Left _ -> s { _wstFinalBusy = Just True, _wstErrors = _wstErrors s + 1 }
            void $ try @SomeException (setAutocheckpoint writer 1000)
            closeSQLiteConnection (_wfConn f)
            return False
        walBytes f
        s <- readIORef (_wfStats f)
        atomicModifyIORef' (_wcFinished cp) (\m -> (M.insert label s m, ()))
        logJson cp Info (walStatsJson label s)

-- -------------------------------------------------------------------------- --
-- One poll of one file

tick :: WalCheckpointer -> WalFile -> IO ()
tick cp f = handle onError $ withMVar (_wfLock f) $ \open -> when open $ do
    now <- getMonotonicTime
    due <- readIORef (_wfDue f)
    when (now >= due) $ checkpoint now
    lastLog <- readIORef (_wfLastLog f)
    when (elapsed now lastLog (1000 * _wccStatsLogSeconds cfg)) $ do
        writeIORef (_wfLastLog f) now
        s <- readIORef (_wfStats f)
        logJson cp Info (walStatsJson (_wfLabel f) s)
  where
    cfg = _wcConfig cp
    sec ms = fromIntegral ms / 1000 :: Double
    elapsed now t ms = (now - t) * 1000 >= fromIntegral ms

    checkpoint now = do
        (copied, _) <- passiveNow cp f now
        -- liveness: the thread's calls only
        writeIORef (_wcLastOk cp) (Just now)
        when (copied > 0) $ walBytes f

    onError (e :: SomeException) = case fromException e of
        Just (_ :: SomeAsyncException) -> throwIO e
        Nothing -> do
            modifyIORef' (_wfStats f) $ \s -> s { _wstErrors = _wstErrors s + 1 }
            now <- getMonotonicTime
            writeIORef (_wfDue f) (now + sec (_wccIntervalMs cfg))
            lastW <- readIORef (_wfLastWarn f)
            when (now - lastW >= 60) $ do
                writeIORef (_wfLastWarn f) now
                _wcLog cp Warn $ "WAL checkpointer: " <> _wfLabel f <> ": " <> T.pack (displayException e)

-- | One PASSIVE checkpoint of the file on the checkpointer's connection,
-- under '_wfLock': updates the file's schedule and stats. Returns the frames
-- it copied and its result.
passiveNow :: WalCheckpointer -> WalFile -> Double -> IO (Int, ((Bool, Int, Int), Double))
passiveNow cp f now = do
    r@((busy, nLog, nCkpt), ms) <- timedCheckpoint (_wfConn f) "PASSIVE"
    (t0, log0, ckpt0) <- readIORef (_wfLastCall f)
    lastCopy <- readIORef (_wfLastCopy f)
    if nLog < 0 || nCkpt < 0
    then do
        -- no WAL information (another checkpoint ran): try again at the
        -- next poll
        writeIORef (_wfDue f) (now + sec (_wccPollMs cfg))
        modifyIORef' (_wfStats f) $ \s -> s
            { _wstTotalMs = _wstTotalMs s + ms
            , _wstMaxMs = max (_wstMaxMs s) ms
            , _wstBusy = _wstBusy s + 1
            }
        return (0, r)
    else do
        let restarted = nLog < log0
            -- frames appended since the last call, and copied by this one
            written = if restarted then nLog else nLog - log0
            copied = max 0 (if restarted then nCkpt else nCkpt - ckpt0)
            stalled = busy || nCkpt < nLog
            lastCopy' = if copied > 0 then now else lastCopy
            rate = fromIntegral written / max 1e-3 (now - t0)
            next
                -- the WAL restarted since the last call: the frames since
                -- the restart over the time since that call (before the
                -- restart) would underestimate the rate, and a burst right
                -- after the restart would go unseen until a far due time.
                -- The rate is unknown: the next call, at the next poll,
                -- measures it
                | restarted = now + sec (_wccPollMs cfg)
                | stalled = now + sec (_wccIntervalMs cfg)
                | written <= 0 = max (now + sec (_wccPollMs cfg)) (lastCopy' + sec (_wccIntervalMs cfg))
                | otherwise = now + min (sec (_wccIntervalMs cfg))
                    (max (sec (_wccPollMs cfg)) (fromIntegral (_wccSoftFrames cfg) / rate))
        writeIORef (_wfLastCall f) (now, nLog, nCkpt)
        writeIORef (_wfLastCopy f) lastCopy'
        writeIORef (_wfDue f) next
        modifyIORef' (_wfStats f) $ \s -> s
            { _wstCheckpoints = _wstCheckpoints s + (if copied > 0 then 1 else 0)
            , _wstFrames = _wstFrames s + copied
            , _wstBusy = _wstBusy s + (if stalled then 1 else 0)
            , _wstTotalMs = _wstTotalMs s + ms
            , _wstMaxMs = max (_wstMaxMs s) ms
            , _wstWalFramesMax = max (_wstWalFramesMax s) nLog
            , _wstRestarts = _wstRestarts s + (if restarted then 1 else 0)
            , _wstOverRestart = _wstOverRestart s + (if nLog >= restartAt then 1 else 0)
            }
        lastW <- readIORef (_wfLastWarn f)
        when (nLog >= 2 * restartAt && now - lastW >= 60) $ do
            writeIORef (_wfLastWarn f) now
            _wcLog cp Warn $ "WAL checkpointer: the WAL of " <> _wfLabel f <> " is " <> T.pack (show nLog)
                <> " frames long (restart at " <> T.pack (show restartAt)
                <> "): a long read transaction keeps it from being checkpointed and restarted"
        return (copied, r)
  where
    cfg = _wcConfig cp
    restartAt = _wccRestartFrames cfg
    sec ms = fromIntegral ms / 1000 :: Double

-- -------------------------------------------------------------------------- --
-- A checkpoint before a write, on request

-- | What 'checkpointBeforeLock' or 'checkpointUnderLock' did.
data WriterCheckpoint
    = WriterCheckpointRan !Double !Int !Bool
        -- ^ a PASSIVE ran: its time in ms, the WAL's length in frames it
        -- found, and whether it copied every frame
    | WriterCheckpointSkipped
        -- ^ none was needed (no I/O)
    | WriterCheckpointBusy
        -- ^ the thread was checkpointing the file at that moment
    | WriterCheckpointFailed
        -- ^ the PASSIVE failed: counted in 'WalStats' errors and logged at
        -- Warn (at most once a minute per file); the caller goes on and
        -- the backstop holds
    deriving (Show, Eq)

-- | The first phase before a write of the given writer: a PASSIVE
-- checkpoint of its file now, on the checkpointer's connection, for a
-- caller that does not hold the writer's lock. It copies the bulk of the
-- WAL and syncs while the writer goes on. Skipped while the thread's
-- schedule covers the file: the file's next PASSIVE is not yet due
-- ('_wfDue') and the last call (the thread's or one of these) found fewer
-- than '_wccSoftFrames' frames. It runs only when the thread is overdue
-- (stalled on another file, or on a Busy), or the WAL was at or past soft
-- (the thread is behind): then it adds a drain the thread does not give,
-- and otherwise it would only double the thread's syncs. The decision
-- reads only the state the thread keeps, no I/O. It never waits for the
-- thread: when the thread checkpoints the file at that moment it returns
-- 'WriterCheckpointBusy'. Like the thread it never opens the @-shm@ or
-- @-wal@ file and never runs anything but PASSIVE. 'Nothing' when the
-- writer is not registered.
checkpointBeforeLock :: WalCheckpointer -> SQLiteEnv -> IO (Maybe WriterCheckpoint)
checkpointBeforeLock cp = writerCheckpoint cp $ \now due (_, nLog, _) ->
    now < due && nLog < _wccSoftFrames (_wcConfig cp)

-- | The second phase, for a caller that holds the writer's lock (no
-- transaction of the writer is open, and none begins during the call): a
-- PASSIVE checkpoint when the WAL's last known length (the first phase's,
-- or the thread's if later) is at least 'underLockFrames'; skipped, without
-- I/O, below. The PASSIVE copies what the first phase left (the frames
-- committed during or after it), every frame unless a reader pins some, so the
-- writer's next transaction restarts the WAL: its commit cannot reach the
-- '_wccRestartFrames' backstop and checkpoint inside it. The freezer calls
-- it before each trim chunk (back-to-back block commits keep the thread's
-- checkpoints from ever finding that moment). Busy, failure and
-- registration as 'checkpointBeforeLock'.
checkpointUnderLock :: WalCheckpointer -> SQLiteEnv -> IO (Maybe WriterCheckpoint)
checkpointUnderLock cp = writerCheckpoint cp $ \_ _ (_, nLog, _) ->
    nLog < underLockFrames (_wcConfig cp)

-- | The WAL length, in frames, from which 'checkpointUnderLock' runs: a
-- quarter of '_wccRestartFrames' (4,096 at the default 16,384).
underLockFrames :: WalCheckpointConfig -> Int
underLockFrames cfg = max 1 (_wccRestartFrames cfg `div` 4)

-- | A PASSIVE of the writer's file under its '_wfLock', unless the rule
-- (given the time, the file's next due PASSIVE and its last call) skips it.
writerCheckpoint
    :: WalCheckpointer
    -> (Double -> Double -> (Double, Int, Int) -> Bool)
    -> SQLiteEnv
    -> IO (Maybe WriterCheckpoint)
writerCheckpoint cp skip writer = do
    files <- readMVar (_wcFiles cp)
    case filter ((== writer) . _wfWriter) (M.elems files) of
        [] -> return Nothing
        f : _ -> mask $ \restore -> tryTakeMVar (_wfLock f) >>= \case
            Nothing -> return (Just WriterCheckpointBusy)
            Just False -> putMVar (_wfLock f) False >> return Nothing
            Just True -> (`finally` putMVar (_wfLock f) True) $ restore $ do
                now <- getMonotonicTime
                due <- readIORef (_wfDue f)
                lastCall <- readIORef (_wfLastCall f)
                if skip now due lastCall
                then return (Just WriterCheckpointSkipped)
                else handle (onError f) $ do
                    (_, ((busy, l, k), ms)) <- passiveNow cp f now
                    return $ Just $ WriterCheckpointRan ms (max 0 l) (not busy && k >= l)
  where
    -- counted and logged (rate-limited, as a failed tick is); the caller's
    -- write goes on and the backstop holds. Runs under '_wfLock', as the
    -- other updates of the stats do.
    onError f (e :: SomeException) = case fromException e of
        Just (_ :: SomeAsyncException) -> throwIO e
        Nothing -> do
            modifyIORef' (_wfStats f) $ \s -> s { _wstErrors = _wstErrors s + 1 }
            now <- getMonotonicTime
            lastW <- readIORef (_wfLastWarn f)
            when (now - lastW >= 60) $ do
                writeIORef (_wfLastWarn f) now
                _wcLog cp Warn $ "WAL checkpointer: a checkpoint before a write of " <> _wfLabel f
                    <> " failed: " <> T.pack (displayException e)
            return (Just WriterCheckpointFailed)

-- | The size of the @-wal@ file, from its metadata (@stat@): the file is
-- never opened here (see the module header).
walBytes :: WalFile -> IO ()
walBytes f = do
    let AtPath atPath = _wfAtPath f
    r <- try @IOException $ atPath $ \p -> do
        let w = p <> "-wal"
        e <- doesFileExist w
        if e then getFileSize w else return 0
    forM_ r $ \n -> modifyIORef' (_wfStats f) $ \s -> s { _wstWalBytesMax = max (_wstWalBytesMax s) n }

-- -------------------------------------------------------------------------- --
-- SQL

passive :: SQLiteEnv -> IO (Bool, Int, Int)
passive c = walCheckpoint c "PASSIVE"

-- | @PRAGMA wal_checkpoint(mode)@: (busy, frames in the WAL, frames
-- checkpointed).
walCheckpoint :: SQLiteEnv -> String -> IO (Bool, Int, Int)
walCheckpoint c mode = P4SQL.qry_ c (fromString ("PRAGMA wal_checkpoint(" <> mode <> ")"))
    [P4SQL.RInt, P4SQL.RInt, P4SQL.RInt] >>= \case
        [[P4SQL.SInt b, P4SQL.SInt l, P4SQL.SInt k]] -> return (b /= 0, fromIntegral l, fromIntegral k)
        r -> throwIO $ userError $ "wal_checkpoint: unexpected result " <> show r

timedCheckpoint :: SQLiteEnv -> String -> IO ((Bool, Int, Int), Double)
timedCheckpoint c mode = do
    t0 <- getMonotonicTime
    r <- walCheckpoint c mode
    t1 <- getMonotonicTime
    return (r, (t1 - t0) * 1000)

-- | @PRAGMA journal_size_limit@ on the connection: the size the first
-- commit after a WAL restart truncates a longer @-wal@ file to.
setJournalSizeLimit :: SQLiteEnv -> Int -> IO ()
setJournalSizeLimit c n = void $
    P4SQL.qry_ c (fromString ("PRAGMA journal_size_limit = " <> show n)) [P4SQL.RInt]

-- | @sqlite3_wal_autocheckpoint@ on the connection (the C call: no
-- statement, the connection's error state left alone).
setAutocheckpoint :: SQLiteEnv -> Int -> IO ()
setAutocheckpoint (Database db) n = do
    rc <- c_sqlite3_wal_autocheckpoint db (fromIntegral n)
    unless (rc == 0) $ throwIO $ userError $ "sqlite3_wal_autocheckpoint: error " <> show rc

foreign import ccall safe "sqlite3_wal_autocheckpoint"
    c_sqlite3_wal_autocheckpoint :: Ptr CDatabase -> CInt -> IO CInt

logJson :: WalCheckpointer -> LogLevel -> A.Value -> IO ()
logJson cp level v = _wcLog cp level (T.decodeUtf8 (BL.toStrict (A.encode v)))
