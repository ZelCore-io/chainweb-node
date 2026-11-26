{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.Test.Pact.Store.WalCheckpointer
-- License: MIT
-- Stability: experimental
--
-- The background WAL checkpointer: a thread on its own connection
-- checkpoints, the Pact connection's commits keep only a backstop; the WAL
-- stays bounded, and restarts after a reader that pinned it; a clean stop
-- checkpoints the WAL to empty.
--
module Chainweb.Test.Pact.Store.WalCheckpointer
( tests
) where

import Control.Concurrent (threadDelay)
import Control.Monad

import qualified Data.ByteString as B
import Data.Maybe (fromMaybe)
import Data.Text (Text)

import GHC.Clock (getMonotonicTime)

import System.Directory (doesFileExist, getFileSize)
import System.FilePath
import System.IO.Temp (withSystemTempDirectory)
import System.LogLevel (LogLevel)

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
    ]

quiet :: LogLevel -> Text -> IO ()
quiet _ _ = return ()

fastConfig :: WalCheckpointConfig
fastConfig = defaultWalCheckpointConfig
    { _wccPollMs = 10
    , _wccIntervalMs = 200
    , _wccSoftFrames = 200
    , _wccRestartFrames = 400
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
