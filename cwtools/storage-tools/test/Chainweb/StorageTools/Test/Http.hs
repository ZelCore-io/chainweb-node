{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.StorageTools.Test.Http
--
-- Tests of the request limiter and of the retries of the service API.
--
module Chainweb.StorageTools.Test.Http
( tests
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async
import Control.Exception (finally, toException, try)
import Control.Monad

import Data.IORef
import qualified Data.Text as T

import GHC.Clock (getMonotonicTime)

import qualified Network.HTTP.Client as HTTP
import Network.HTTP.Types (status200, status429, status500, status502, status503, status504)
import Network.Wai (Application, responseLBS, responseRaw)
import Network.Wai.Handler.Warp (testWithApplication)

import Servant.Client (ClientError(..))

import System.Directory (removeFile)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import System.Timeout (timeout)

import Test.Tasty
import Test.Tasty.HUnit

import Chainweb.ChainId (unsafeChainId)
import Chainweb.StorageTools.Http
import Chainweb.Version.Mainnet (mainnet)

tests :: TestTree
tests = testGroup "Http"
    [ limiterTests
    , retryTests
    ]

limiterTests :: TestTree
limiterTests = testGroup "Limiter"
    [ testCase "8 threads, 50 permits at 20 r/s take 49 / 20 s plus at most 1.5 s" $ do
        let rate = 20 :: Double
            permits = 50 :: Int
            -- With a burst size of 1, N permits take at least (N - 1) / rate:
            -- 2.45 s. The 0.05 s below it only absorb clock rounding.
            lower = fromIntegral (permits - 1) / rate
            -- The slack above it is for thread wake-ups on a loaded machine
            -- (tasty runs the suite's tests in parallel). Each of the 8
            -- threads holds a reserved slot at an absolute time, so a wake-up
            -- less than 8 slots (0.4 s) late does not push later slots back;
            -- mostly the last permit's delay adds to the total. 1.5 s still
            -- fails a limiter that runs at half the rate, which needs 4.9 s.
            upper = lower + 1.5
        lim <- newLimiter rate Nothing
        t0 <- getMonotonicTime
        -- 7 + 7 + 6 * 6 = 50 permits
        mapConcurrently_ (\n -> replicateM_ n (throttled lim (return ())))
            [7, 7, 6, 6, 6, 6, 6, 6]
        t1 <- getMonotonicTime
        let dt = t1 - t0
        assertBool ("took " <> show dt <> " s, expected at least " <> show lower <> " s") (dt >= lower - 0.05)
        assertBool ("took " <> show dt <> " s, expected under " <> show upper <> " s") (dt < upper)
        n <- limiterPermits lim
        assertEqual "number of permits" (fromIntegral permits) n

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

    , testCase "permits reserved before the pause file appeared wait until it is removed" $
        withSystemTempDirectory "limiter-pause" $ \dir -> do
            let pauseFile = dir </> "PAUSE"
            -- at 1 r/s the first permit is immediate; each of the 4 threads
            -- then reserves one of the slots 1, 2, 3 and 4 s later and
            -- sleeps until its slot
            lim <- newLimiter 1 (Just pauseFile)
            t0 <- getMonotonicTime
            throttled lim (return ())
            workers <- replicateM 4 $ async $ throttled lim (return ())
            flip finally (mapM_ cancel workers) $ do
                threadDelay 300_000
                writeFile pauseFile "pause"
                -- sample the count until every reserved slot has passed
                let flat = do
                        n <- limiterPermits lim
                        assertEqual "no permit while the pause file exists" 1 n
                        t <- getMonotonicTime
                        when (t - t0 < 4.6) $ threadDelay 200_000 >> flat
                flat
                removeFile pauseFile
                -- the threads take new slots, one per second from now
                done <- timeout 6_000_000 (mapM_ wait workers)
                assertEqual "the reserved permits are granted after the removal" (Just ()) done
                assertEqual "number of permits" 5 =<< limiterPermits lim
    ]

-- -------------------------------------------------------------------------- --
-- Retries

-- | Serves the application on a loopback port and makes one header request
-- through 'runServiceApi', with a response timeout of 300 ms. Returns the
-- number of requests that reached the server and whether the call failed.
-- The application gets the number of the request, counted from 1.
--
requestsReceived :: (Int -> Application) -> IO (Int, Bool)
requestsReceived app = do
    counter <- newIORef (0 :: Int)
    let counted req respond = do
            n <- atomicModifyIORef' counter (\n -> (n + 1, n + 1))
            app n req respond
    testWithApplication (return counted) $ \port -> do
        mgr <- HTTP.newManager HTTP.defaultManagerSettings
            { HTTP.managerResponseTimeout = HTTP.responseTimeoutMicro 300_000 }
        lim <- newLimiter 1_000 Nothing
        let api = ServiceApi ("http://127.0.0.1:" <> T.pack (show port)) mainnet mgr lim
        r <- try (hasHeaderAtOrAbove api (unsafeChainId 0) 0)
        n <- readIORef counter
        return (n, either (\(_ :: ServiceApiError) -> True) (const False) r)

emptyPage :: Application
emptyPage _ respond = respond $ responseLBS status200
    [("Content-Type", "application/json")]
    "{\"limit\":0,\"items\":[],\"next\":null}"

retryTests :: TestTree
retryTests = testGroup "runServiceApi retries"
    [ testCase "a request that times out on the server is sent once" $ do
        r <- requestsReceived $ \_ _ respond -> do
            threadDelay 2_000_000
            respond (responseLBS status200 [] "")
        assertEqual "(requests received, failed)" (1, True) r

    , testCase "a request whose connection the server closes without an answer is sent once" $ do
        r <- requestsReceived $ \_ _ respond ->
            respond (responseRaw (\_ _ -> return ()) (responseLBS status500 [] ""))
        assertEqual "(requests received, failed)" (1, True) r

    , testCase "status 500 is final" $ do
        r <- requestsReceived $ \_ _ respond -> respond (responseLBS status500 [] "")
        assertEqual "(requests received, failed)" (1, True) r

    , testCase "status 502 is final" $ do
        r <- requestsReceived $ \_ _ respond -> respond (responseLBS status502 [] "")
        assertEqual "(requests received, failed)" (1, True) r

    , testCase "status 504 is final" $ do
        r <- requestsReceived $ \_ _ respond -> respond (responseLBS status504 [] "")
        assertEqual "(requests received, failed)" (1, True) r

    , testCase "status 429 and 503 are retried" $ do
        r <- requestsReceived $ \n req respond -> case n of
            1 -> respond (responseLBS status429 [] "")
            2 -> respond (responseLBS status503 [] "")
            _ -> emptyPage req respond
        assertEqual "(requests received, failed)" (3, False) r

    , testCase "of the transport errors only a connection that never opened is retried" $ do
        let transport = ConnectionError . toException . HTTP.HttpExceptionRequest HTTP.defaultRequest
        forM_
            [ ("ConnectionFailure", HTTP.ConnectionFailure (toException (userError "refused")), True)
            , ("ConnectionTimeout", HTTP.ConnectionTimeout, True)
            , ("ResponseTimeout", HTTP.ResponseTimeout, False)
            , ("ConnectionClosed", HTTP.ConnectionClosed, False)
            , ("NoResponseDataReceived", HTTP.NoResponseDataReceived, False)
            , ("IncompleteHeaders", HTTP.IncompleteHeaders, False)
            , ("InternalException", HTTP.InternalException (toException (userError "reset")), False)
            ] $ \(name, c, expected) ->
                assertEqual name expected (retriableClientError (transport c))
        assertEqual "not an HttpException" False
            (retriableClientError (ConnectionError (toException (userError "other"))))
    ]
