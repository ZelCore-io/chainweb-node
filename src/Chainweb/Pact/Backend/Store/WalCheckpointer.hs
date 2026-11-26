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
-- * Every '_wccPollMs' the thread reads the WAL index header (the first 136
--   bytes of the @-shm@ file: @mxFrame@ and @nBackfill@, SQLite's
--   documented wal-index format) without a lock. It runs
--   @wal_checkpoint(PASSIVE)@ when '_wccSoftFrames' frames are pending, or
--   when frames are pending and '_wccIntervalMs' passed since the last
--   checkpoint (only the latter after a PASSIVE that a reader kept from
--   finishing). PASSIVE takes no lock that a writer or reader waits for:
--   commits go on during it. When the header cannot be read, the thread
--   checkpoints by time alone.
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
) where

import Control.Concurrent
import Control.Concurrent.Async (withAsync)
import Control.Exception
import Control.Monad

import Data.Aeson (object, (.=))
import qualified Data.Aeson as A
import Data.Bits (shiftL, (.|.))
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.IORef
import qualified Data.Map.Strict as M
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import GHC.ByteOrder (ByteOrder(..), targetByteOrder)
import GHC.Clock (getMonotonicTime)

import System.Directory (doesFileExist, getFileSize)
import System.IO (IOMode(..), withBinaryFile)
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
    }

data WalFile = WalFile
    { _wfLabel :: !Text
    , _wfAtPath :: !AtPath
    , _wfWriter :: !SQLiteEnv
    , _wfConn :: !SQLiteEnv
    , _wfLock :: !(MVar Bool)
        -- ^ held during a checkpoint; 'False' once closed
    , _wfStats :: !(IORef WalStats)
    , _wfLast :: !(IORef Double)
        -- ^ the time of the last checkpoint
    , _wfLastMx :: !(IORef Int)
        -- ^ the WAL's length at the last poll
    , _wfLastLog :: !(IORef Double)
    , _wfLastWarn :: !(IORef Double)
    , _wfStalled :: !(IORef Bool)
        -- ^ the last PASSIVE checkpoint left frames behind: a reader pins
        -- them, so the next one waits for the interval
    }

-- | Runs an action with the file's current path, under whatever lock the
-- path needs (the rename lock of the SQLite directory).
newtype AtPath = AtPath (forall b. (FilePath -> IO b) -> IO b)

-- | A file's checkpoint stats since its registration.
data WalStats = WalStats
    { _wstCheckpoints :: !Int
        -- ^ PASSIVE checkpoints run
    , _wstFrames :: !Int
        -- ^ frames copied into the database file
    , _wstBusy :: !Int
        -- ^ PASSIVE checkpoints that left frames behind (a reader pinned them)
    , _wstTotalMs :: !Double
    , _wstMaxMs :: !Double
        -- ^ the longest checkpoint (PASSIVE or TRUNCATE)
    , _wstWalFramesMax :: !Int
        -- ^ the high water of the WAL's length in frames
    , _wstWalBytesMax :: !Integer
        -- ^ the high water of the @-wal@ file's size
    , _wstRestarts :: !Int
        -- ^ WAL restarts seen (its length went down between two polls)
    , _wstOverRestart :: !Int
        -- ^ polls that saw the WAL at or past '_wccRestartFrames': the
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
    withAsync (loop cp) $ \_ -> k cp
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
                void $ passive c
            return c
        flip onException (closeSQLiteConnection conn) $ do
          now <- getMonotonicTime
          f <- WalFile label (AtPath atPath) writer conn
              <$> newMVar True
              <*> newIORef emptyWalStats
              <*> newIORef now
              <*> newIORef 0
              <*> newIORef now
              <*> newIORef 0
              <*> newIORef False
          i <- atomicModifyIORef' (_wcNext cp) (\n -> (n + 1, n))
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
    hdr <- readHeader f
    lastC <- readIORef (_wfLast f)
    stalled <- readIORef (_wfStalled f)
    lastMx <- readIORef (_wfLastMx f)
    let elapsed t ms = (now - t) * 1000 >= fromIntegral ms
        interval = elapsed lastC (_wccIntervalMs cfg)
        restartAt = _wccRestartFrames cfg
    forM_ hdr $ \(mx, _) -> modifyIORef' (_wfStats f) $ \s -> s { _wstWalFramesMax = max (_wstWalFramesMax s) mx }
    case hdr of
        Just (mx, nb) -> do
            writeIORef (_wfLastMx f) mx
            modifyIORef' (_wfStats f) $ \s -> s
                { _wstRestarts = _wstRestarts s + (if mx < lastMx then 1 else 0)
                , _wstOverRestart = _wstOverRestart s + (if mx >= restartAt then 1 else 0)
                }
            when ((mx - nb >= _wccSoftFrames cfg && not stalled) || (mx > nb && interval)) $
                checkpoint now nb
            lastW <- readIORef (_wfLastWarn f)
            when (mx >= 2 * restartAt && now - lastW >= 60) $ do
                writeIORef (_wfLastWarn f) now
                _wcLog cp Warn $ "WAL checkpointer: the WAL of " <> _wfLabel f <> " is " <> T.pack (show mx)
                    <> " frames long (restart at " <> T.pack (show restartAt)
                    <> "): a long read transaction keeps it from being checkpointed and restarted"
        Nothing -> when interval $ checkpoint now 0
    lastLog <- readIORef (_wfLastLog f)
    when (elapsed lastLog (1000 * _wccStatsLogSeconds cfg)) $ do
        writeIORef (_wfLastLog f) now
        s <- readIORef (_wfStats f)
        logJson cp Info (walStatsJson (_wfLabel f) s)
  where
    cfg = _wcConfig cp

    checkpoint now nb = do
        ((busy, nLog, nCkpt), ms) <- timedCheckpoint (_wfConn f) "PASSIVE"
        writeIORef (_wfLast f) now
        writeIORef (_wfStalled f) (nCkpt < nLog)
        modifyIORef' (_wfStats f) $ \s -> s
            { _wstCheckpoints = _wstCheckpoints s + 1
            , _wstFrames = _wstFrames s + max 0 (nCkpt - nb)
            , _wstBusy = _wstBusy s + (if busy || nCkpt < nLog then 1 else 0)
            , _wstTotalMs = _wstTotalMs s + ms
            , _wstMaxMs = max (_wstMaxMs s) ms
            , _wstWalFramesMax = max (_wstWalFramesMax s) nLog
            }
        walBytes f

    onError (e :: SomeException) = case fromException e of
        Just (_ :: SomeAsyncException) -> throwIO e
        Nothing -> do
            modifyIORef' (_wfStats f) $ \s -> s { _wstErrors = _wstErrors s + 1 }
            now <- getMonotonicTime
            lastW <- readIORef (_wfLastWarn f)
            when (now - lastW >= 60) $ do
                writeIORef (_wfLastWarn f) now
                _wcLog cp Warn $ "WAL checkpointer: " <> _wfLabel f <> ": " <> T.pack (displayException e)

-- | @(mxFrame, nBackfill)@ from the wal-index header, or 'Nothing' when it
-- cannot be read or is not consistent (being written, or not initialised).
readHeader :: WalFile -> IO (Maybe (Int, Int))
readHeader f = do
    let AtPath atPath = _wfAtPath f
    r <- try @IOException $ atPath $ \p -> withBinaryFile (p <> "-shm") ReadMode (\h -> B.hGet h 136)
    return $ case r of
        Right h | B.length h == 136
                , B.index h 12 == 1
                , B.take 48 h == B.take 48 (B.drop 48 h)
                , let mx = u32 h 16
                , let nb = u32 h 96
                , nb <= mx
                -> Just (mx, nb)
        _ -> Nothing
  where
    -- native byte order, as SQLite writes it
    u32 bs o = case targetByteOrder of
        LittleEndian -> foldr (\i a -> a `shiftL` 8 .|. fromIntegral (B.index bs (o + i))) 0 [0 .. 3]
        BigEndian -> foldl (\a i -> a `shiftL` 8 .|. fromIntegral (B.index bs (o + i))) 0 [0 .. 3]

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
