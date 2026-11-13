{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.StorageTools.Test.Http
--
-- Tests of the request limiter.
--
module Chainweb.StorageTools.Test.Http
( tests
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Exception (finally)
import Control.Monad

import GHC.Clock (getMonotonicTime)

import System.Directory (removeFile)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import System.Timeout (timeout)

import Test.Tasty
import Test.Tasty.HUnit

import Chainweb.StorageTools.Http

tests :: TestTree
tests = testGroup "Limiter"
    [ testCase "8 threads, 50 permits at 20 r/s take at least 2.4 s and under 4 s" $ do
        lim <- newLimiter 20 Nothing
        t0 <- getMonotonicTime
        -- 7 + 7 + 6 * 6 = 50 permits
        mapConcurrently_ (\n -> replicateM_ n (throttled lim (return ())))
            [7, 7, 6, 6, 6, 6, 6, 6]
        t1 <- getMonotonicTime
        let dt = t1 - t0
        assertBool ("took " <> show dt <> " s, expected at least 2.4 s") (dt >= 2.4)
        assertBool ("took " <> show dt <> " s, expected under 4 s") (dt < 4)
        n <- limiterPermits lim
        assertEqual "number of permits" 50 n

    , testCase "no permit while the pause file exists; resumes within 1 s of removal" $
        withSystemTempDirectory "limiter-pause" $ \dir -> do
            let pauseFile = dir </> "PAUSE"
            lim <- newLimiter 1_000 (Just pauseFile)
            -- without the pause file a permit is immediate
            r0 <- timeout 1_000_000 (throttled lim (return ()))
            assertEqual "permit without pause file" (Just ()) r0

            writeFile pauseFile "pause"
            a <- async (throttled lim (return ()))
            threadDelay 700_000
            paused <- poll a
            assertBool "no permit while the pause file exists" (null paused)

            removeFile pauseFile
            r1 <- timeout 1_000_000 (wait a)
            assertEqual "permit within 1 s of removing the pause file" (Just ()) r1

    , testCase "a pause file that appears while permits flow stops them within 1 s; they resume within 1 s of removal" $
        withSystemTempDirectory "limiter-pause" $ \dir -> do
            let pauseFile = dir </> "PAUSE"
            lim <- newLimiter 200 (Just pauseFile)
            worker <- async $ forever $ throttled lim (return ())
            flip finally (cancel worker) $ do
                threadDelay 300_000
                n0 <- limiterPermits lim
                assertBool ("permits flow without the pause file, got " <> show n0) (n0 >= 20)

                writeFile pauseFile "pause"
                threadDelay 1_000_000
                n1 <- limiterPermits lim
                threadDelay 500_000
                n2 <- limiterPermits lim
                assertEqual "no permit more than 1 s after the pause file appeared" n1 n2

                removeFile pauseFile
                threadDelay 1_000_000
                n3 <- limiterPermits lim
                assertBool ("permits resume within 1 s of removing the pause file, " <> show (n2, n3)) (n3 > n2)
    ]
