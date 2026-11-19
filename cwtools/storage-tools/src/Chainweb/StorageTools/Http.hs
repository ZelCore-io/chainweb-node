{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.StorageTools.Http
--
-- Read-only access to the service API of a chainweb node, for tools that
-- copy data from a node into a local database.
--
-- All requests of a host go through one 'Limiter', which every caller of that
-- host shares. The limiter honours a pause file.
--
module Chainweb.StorageTools.Http
( -- * Limiter
  Limiter
, newLimiter
, throttled
, limiterPermits
, pausePollMicros

  -- * Service API
, ServiceApi(..)
, newServiceApi
, ServiceApiError(..)
, runServiceApi
, retriableClientError

  -- * Requests
, headerPageLimit
, payloadBatchSize
, forHeaders
, hasHeaderAtOrAbove
, fetchPayloadsWithOutputs
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Exception (Exception, evaluate, fromException, throwIO)
import Control.Lens (view)
import Control.Monad

import qualified Data.HashMap.Strict as HM
import Data.IORef
import Data.List (sortOn)
import qualified Data.Text as T
import Data.Word

import GHC.Clock (getMonotonicTimeNSec)

import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Client.TLS as HTTP
import Network.HTTP.Types.Status (statusCode)

import Servant.Client

import System.Directory (doesFileExist)

-- internal modules

import Chainweb.BlockHeader
import Chainweb.BlockHeaderDB.RestAPI.Client (headersClient)
import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Payload
import Chainweb.Payload.RestAPI (BatchBody(..))
import Chainweb.Payload.RestAPI.Client (outputsBatchClient)
import Chainweb.Utils.Paging
import Chainweb.Utils (int)
import Chainweb.Version

-- -------------------------------------------------------------------------- --
-- Limiter

-- | A rate limit on requests, shared by all threads that use it.
--
data Limiter = Limiter
    { _limInterval :: !Word64
        -- ^ nanoseconds between two permits
    , _limPauseFile :: !(Maybe FilePath)
    , _limNext :: !(MVar Word64)
        -- ^ the monotonic time of the next free permit slot, in nanoseconds
    , _limPermits :: !(IORef Word64)
    }

-- | @newLimiter rate pauseFile@ is a limiter that grants @rate@ permits per
-- second across all threads.
--
-- The burst size is 1: a permit is granted at the earliest one interval
-- (@1 / rate@ seconds) after the previous permit, no matter how long the
-- limiter was idle. N permits therefore take at least @(N - 1) / rate@
-- seconds.
--
-- While the pause file exists, no permit is granted. A thread that waits for a
-- permit notices the removal of the file within 100 ms, and a permit that was
-- reserved before the file appeared is not granted until the file is gone.
--
newLimiter :: Double -> Maybe FilePath -> IO Limiter
newLimiter rate pauseFile
    | isNaN rate || isInfinite rate || rate <= 0 =
        ioError $ userError $ "Chainweb.StorageTools.Http.newLimiter: invalid rate: " <> show rate
    | otherwise = Limiter interval pauseFile <$> newMVar 0 <*> newIORef 0
  where
    interval = max 1 $ ceiling (1e9 / rate)

-- | The number of permits that were granted so far.
--
limiterPermits :: Limiter -> IO Word64
limiterPermits = readIORef . _limPermits

pausePollMicros :: Int
pausePollMicros = 100_000

isPaused :: Limiter -> IO Bool
isPaused lim = maybe (return False) doesFileExist (_limPauseFile lim)

waitWhilePaused :: Limiter -> IO ()
waitWhilePaused lim = do
    paused <- isPaused lim
    when paused $ do
        threadDelay pausePollMicros
        waitWhilePaused lim

acquire :: Limiter -> IO ()
acquire lim = do
    waitWhilePaused lim
    slot <- modifyMVar (_limNext lim) $ \next -> do
        now <- getMonotonicTimeNSec
        let s = max now next
        return (s + _limInterval lim, s)
    now <- getMonotonicTimeNSec
    when (slot > now) $
        threadDelay $ fromIntegral $ (slot - now + 999) `div` 1_000
    -- The pause file may have appeared while we waited for the slot.
    paused <- isPaused lim
    if paused
        then acquire lim
        else atomicModifyIORef' (_limPermits lim) (\n -> (n + 1, ()))

-- | Runs an action after a permit of the limiter was granted.
--
throttled :: Limiter -> IO a -> IO a
throttled lim act = acquire lim >> act

-- -------------------------------------------------------------------------- --
-- Service API

-- | A node's service API.
--
data ServiceApi = ServiceApi
    { _saBaseUrl :: !T.Text
        -- ^ for instance @https://example.org@ or @http://127.0.0.1:1848@
    , _saVersion :: !ChainwebVersion
    , _saManager :: !HTTP.Manager
    , _saLimiter :: !Limiter
        -- ^ the limiter of the host, shared by all callers of the host
    }

-- | A service API with a TLS capable connection manager. Every request is
-- throttled by the limiter.
--
-- The manager keeps http-client's default @managerRetryableException@: a
-- request is sent once more on a new connection when a pooled connection
-- closed before any response. 'ServiceApi' carries only idempotent reads,
-- never @/local@, so one such library resend is accepted. The manager of
-- the comparison runner, which does carry @/local@, never resends (see
-- 'Chainweb.StorageTools.Compare.Run.newCompareManager').
--
-- The manager is made with 'HTTP.newManager': 'HTTP.newTlsManagerWith' would
-- replace @managerRetryableException@ with http-client-tls's own, so the
-- default is kept by not using it.
--
newServiceApi :: T.Text -> ChainwebVersion -> Limiter -> IO ServiceApi
newServiceApi url v lim = do
    _ <- parseBaseUrl (T.unpack url)
    mgr <- HTTP.newManager HTTP.tlsManagerSettings
        { HTTP.managerResponseTimeout = HTTP.responseTimeoutMicro 120_000_000
        }
    return $ ServiceApi url v mgr lim

data ServiceApiError = ServiceApiError !T.Text !ClientError
    deriving Show

instance Exception ServiceApiError

-- | Runs a request. Each attempt takes a permit of the limiter. A failure
-- that 'retriableClientError' accepts is retried up to 5 times with a delay
-- of 1, 2, 4 and 8 seconds; all other errors are thrown.
--
runServiceApi :: forall a. ServiceApi -> ClientM a -> IO a
runServiceApi api m = do
    base <- parseBaseUrl (T.unpack $ _saBaseUrl api)
    let env = mkClientEnv (_saManager api) base
        go :: Int -> IO a
        go attempt = throttled (_saLimiter api) (runClientM m env) >>= \case
            Right a -> return a
            Left e
                | attempt < 5 && retriableClientError e -> backoff attempt >> go (attempt + 1)
                | otherwise -> throwIO $ ServiceApiError (_saBaseUrl api) e
    go 1
  where
    backoff attempt = threadDelay (1_000_000 * 2 ^ (attempt - 1))

-- | Whether a failed request may be sent again: only if the server never got
-- it, or answered that it did no work.
--
-- * A connection that could not be opened (@ConnectionFailure@,
--   @ConnectionTimeout@): the request was never sent.
-- * The status 429 or 503: the server refused the request.
--
-- Everything else is final. Servant wraps transport errors in
-- 'ConnectionError'; after a response timeout, a connection the server closed
-- or an incomplete answer, the server may have received the request and may
-- still be working on it, so sending it again would only add load to a node
-- that is already slow. A 500, 502 or 504 can likewise come after the work
-- was done.
--
retriableClientError :: ClientError -> Bool
retriableClientError = \case
    ConnectionError e -> case fromException e of
        Just (HTTP.HttpExceptionRequest _ c) -> case c of
            HTTP.ConnectionFailure _ -> True
            HTTP.ConnectionTimeout -> True
            _ -> False
        _ -> False
    FailureResponse _ r ->
        let c = statusCode (responseStatusCode r) in c == 429 || c == 503
    _ -> False

-- -------------------------------------------------------------------------- --
-- Requests

-- | The page size for block headers; the default maximum of the service API.
--
headerPageLimit :: Limit
headerPageLimit = 360

-- | The maximum number of payloads in a batch request; the default limit of
-- the service API.
--
payloadBatchSize :: Int
payloadBatchSize = 1_000

-- | Calls the function with each page of the headers of a chain with a height
-- in the given range, in the order of the service. Within a chain, a header
-- comes after its parent.
--
forHeaders
    :: ServiceApi
    -> ChainId
    -> BlockHeight
    -> BlockHeight
    -> ([BlockHeader] -> IO ())
    -> IO ()
forHeaders api cid lo hi f = go Nothing
  where
    go next = do
        page <- runServiceApi api $ headersClient (_saVersion api) cid
            (Just headerPageLimit) next (Just $ int lo) (Just $ int hi)
        unless (null $ _pageItems page) $
            f $ sortOn (view blockHeight) $ _pageItems page
        forM_ (_pageNext page) $ go . Just

-- | Whether the chain has a header with at least the given height.
--
hasHeaderAtOrAbove :: ServiceApi -> ChainId -> BlockHeight -> IO Bool
hasHeaderAtOrAbove api cid h = do
    page <- runServiceApi api $ headersClient (_saVersion api) cid
        (Just 1) Nothing (Just $ int h) Nothing
    return $ not $ null $ _pageItems page

-- | Fetches payloads with outputs for the given heights and payload hashes, in
-- batches of 'payloadBatchSize'. The result is in the order of the request.
-- Fails if the service does not return a requested payload.
--
fetchPayloadsWithOutputs
    :: ServiceApi
    -> ChainId
    -> [(BlockHeight, BlockPayloadHash)]
    -> IO [(BlockHeight, PayloadWithOutputs)]
fetchPayloadsWithOutputs api cid = fmap concat . mapM batch . chunks payloadBatchSize
  where
    batch req = do
        PayloadWithOutputsList ps <- runServiceApi api $
            outputsBatchClient (_saVersion api) cid (WithHeights req)
        let byHash = HM.fromList [(_payloadWithOutputsPayloadHash p, p) | p <- ps]
        forM req $ \(h, k) -> case HM.lookup k byHash of
            Nothing -> ioError $ userError $
                "Chainweb.StorageTools.Http.fetchPayloadsWithOutputs: the service did not return the payload "
                <> show k <> " at height " <> show h <> " of chain " <> show cid
            Just p -> do
                _ <- evaluate p
                return (h, p)

chunks :: Int -> [a] -> [[a]]
chunks _ [] = []
chunks n xs = let (a, b) = splitAt n xs in a : chunks n b
