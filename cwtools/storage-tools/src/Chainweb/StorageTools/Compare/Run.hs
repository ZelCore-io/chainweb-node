{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.StorageTools.Compare.Run
--
-- The runner of @cw-compare@: sends each request to both sides, judges the
-- answers with "Chainweb.StorageTools.Compare.Rules" and collects the
-- outcomes in a 'Report'.
--
-- Every call to a host goes through the 'Limiter' of that host: requests, tip
-- probes and orphan lookups alike. Two sides on one host share one limiter
-- (ruling 3.3).
--
module Chainweb.StorageTools.Compare.Run
( -- * Limiters per host
  HostLimiters
, newHostLimiters
, limiterFor
, hostOfUrl

  -- * Running a comparison
, depthFor
, runCompare
, compareWorkers
, tipRefreshInterval
, maxPinRetries
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (concurrently, replicateConcurrently_)
import Control.Concurrent.MVar
import Control.Exception (evaluate, try)
import Control.Monad

import Data.Aeson
import qualified Data.Aeson.Key as K
import Data.Aeson.Types (Parser, parseMaybe)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.IORef
import qualified Data.Map.Strict as M
import Data.Maybe (isJust)
import qualified Data.Text as T
import Data.Word

import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Client.TLS as HTTP
import Network.HTTP.Types (Header, Method, hAccept, hContentType, methodGet, methodPost, statusCode)

import Servant.Client (baseUrlHost, parseBaseUrl)

import System.LogLevel

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.Payload (BlockPayloadHash)
import Chainweb.StorageTools.Compare.Report
import Chainweb.StorageTools.Compare.Rules
import Chainweb.StorageTools.Compare.Types
import Chainweb.StorageTools.Http
import Chainweb.Utils (toText)

sshow :: Show a => a -> T.Text
sshow = T.pack . show

-- -------------------------------------------------------------------------- --
-- Limiters per host

-- | One 'Limiter' per host, created on first use, all with the same rate and
-- pause file.
--
data HostLimiters = HostLimiters
    { _hlRate :: !Double
    , _hlPauseFile :: !(Maybe FilePath)
    , _hlLimiters :: !(MVar (M.Map T.Text Limiter))
    }

-- | @newHostLimiters rate pauseFile@: each host gets @rate@ requests per
-- second, across all callers. Fails on an invalid rate.
--
newHostLimiters :: Double -> Maybe FilePath -> IO HostLimiters
newHostLimiters rate pauseFile = do
    _ <- newLimiter rate pauseFile
    HostLimiters rate pauseFile <$> newMVar mempty

-- | The host of a base URL, in lower case. The scheme and the port are not
-- part of it: two services on one machine share its limit. Two names of one
-- machine (a name and its address) are two hosts.
--
hostOfUrl :: T.Text -> IO T.Text
hostOfUrl url = T.toLower . T.pack . baseUrlHost <$> parseBaseUrl (T.unpack url)

-- | The limiter of the host of a base URL. Fails on an invalid URL.
--
limiterFor :: HostLimiters -> T.Text -> IO Limiter
limiterFor hl url = do
    host <- hostOfUrl url
    modifyMVar (_hlLimiters hl) $ \m -> case M.lookup host m of
        Just l -> return (m, l)
        Nothing -> do
            l <- newLimiter (_hlRate hl) (_hlPauseFile hl)
            return (M.insert host l m, l)

-- -------------------------------------------------------------------------- --
-- Parameters

-- | The number of requests in flight.
--
compareWorkers :: Int
compareWorkers = 8

-- | A side's tip on a chain is probed again after this many requests.
--
tipRefreshInterval :: Int
tipRefreshInterval = 100

-- | A request whose pins disagree is sent again at most this many times.
--
maxPinRetries :: Int
maxPinRetries = 5

-- | @depthFor tip target@: the rewind depth of the target below the tip, or
-- 'Nothing' if the target is above the tip.
--
depthFor :: BlockHeight -> BlockHeight -> Maybe Word64
depthFor tip target
    | target > tip = Nothing
    | otherwise = Just (getBlockHeight tip - getBlockHeight target)

-- -------------------------------------------------------------------------- --
-- Environment

data Role = OracleRole | TestRole
    deriving (Show, Eq, Ord)

data Env = Env
    { _envManager :: !HTTP.Manager
    , _envCoverage :: !Coverage
    , _envOracle :: !Side
    , _envTest :: !Side
    , _envTips :: !(IORef (M.Map (Role, ChainId) (BlockHeight, Int)))
        -- ^ tip and the request count when it was probed
    , _envStarted :: !(IORef Int)
        -- ^ requests started
    }

sideOf :: Env -> Role -> Side
sideOf env OracleRole = _envOracle env
sideOf env TestRole = _envTest env

-- -------------------------------------------------------------------------- --
-- HTTP

-- | Sends a request through the limiter of the side's host. Any HTTP status
-- is an answer. Connection failures and timeouts are tried 5 times, with
-- delays of 1, 2, 4 and 8 seconds; each attempt takes a permit.
--
sendRaw
    :: Env
    -> Side
    -> Method
    -> T.Text
    -> [Header]
    -> Maybe B.ByteString
    -> IO (Either T.Text (Int, B.ByteString))
sendRaw env side method path hdrs body = go (1 :: Int)
  where
    url = T.unpack (T.dropWhileEnd (== '/') (_sideBaseUrl side) <> path)
    go attempt = do
        r <- try $ throttled (_sideLimiter side) $ do
            req0 <- HTTP.parseRequest url
            let req = req0
                    { HTTP.method = method
                    , HTTP.requestHeaders = hdrs
                    , HTTP.requestBody = maybe (HTTP.requestBody req0) HTTP.RequestBodyBS body
                    }
            HTTP.httpLbs req (_envManager env)
        case r of
            Right resp -> return $ Right
                (statusCode (HTTP.responseStatus resp), BL.toStrict (HTTP.responseBody resp))
            Left (e :: HTTP.HttpException)
                | attempt < 5 -> do
                    threadDelay (1_000_000 * 2 ^ (attempt - 1))
                    go (attempt + 1)
                | otherwise -> return $ Left $
                    "no answer from " <> _sideName side <> " for " <> T.pack url <> ": " <> sshow e

requestHeaders :: Request -> [Header]
requestHeaders r = (hAccept, "application/json")
    : [ (hContentType, "application/json") | isJust (_reqBody r) ]

toAnswer :: Request -> (Int, B.ByteString) -> Answer
toAnswer r (s, b) = Answer s b $ if isPinnedClass (_reqClass r) then answerPin b else Nothing

sendTo :: Env -> Role -> Request -> T.Text -> IO (Either T.Text Answer)
sendTo env role r path = fmap (toAnswer r) <$>
    sendRaw env (sideOf env role) (_reqMethod r) path (requestHeaders r) (_reqBody r)

bothAnswers
    :: (Either T.Text Answer, Either T.Text Answer)
    -> Either Verdict (Answer, Answer)
bothAnswers = \case
    (Right a, Right b) -> Right (a, b)
    (Left e, _) -> Left (Differ e)
    (_, Left e) -> Left (Differ e)

-- -------------------------------------------------------------------------- --
-- Tips

data ProbeFailure
    = ProbeTransport !T.Text
    | ProbeNoPin !Int !B.ByteString
    deriving (Eq)

probeText :: ProbeFailure -> T.Text
probeText = \case
    ProbeTransport e -> e
    ProbeNoPin s b -> "status " <> sshow s <> " without block metadata, " <> sshow (B.length b) <> " bytes"

data TipDecision = UseCached !BlockHeight | ProbeTip !(Maybe BlockHeight)

-- | The tip of a side on the chain of a pinned request: the metadata height
-- of the request itself at depth 0. Cached per side and chain; probed again
-- after 'tipRefreshInterval' requests, or when forced. While one worker
-- probes, others use the cached tip. If a probe fails, the cached tip is
-- used if there is one.
--
tipOf :: Env -> Role -> Request -> Bool -> IO (Either ProbeFailure BlockHeight)
tipOf env role r force = do
    now <- readIORef (_envStarted env)
    decision <- atomicModifyIORef' (_envTips env) $ \m -> case M.lookup key m of
        Just (tip, stamp)
            | not force && now - stamp < tipRefreshInterval -> (m, UseCached tip)
            | otherwise -> (M.insert key (tip, now) m, ProbeTip (Just tip))
        Nothing -> (m, ProbeTip Nothing)
    case decision of
        UseCached tip -> return (Right tip)
        ProbeTip old -> probe >>= \case
            Right tip -> do
                n <- readIORef (_envStarted env)
                atomicModifyIORef' (_envTips env) (\m -> (M.insert key (tip, n) m, ()))
                return (Right tip)
            Left f -> return $ maybe (Left f) Right old
  where
    key = (role, _reqChain r)
    probe = sendRaw env (sideOf env role) (_reqMethod r) (fillDepth 0 r) (requestHeaders r) (_reqBody r) >>= \case
        Left e -> return (Left (ProbeTransport e))
        Right (s, b) -> return $ maybe (Left (ProbeNoPin s b)) (Right . _pinHeight) (answerPin b)

-- | Sends a pinned request to both sides, each at the depth of the target
-- below its own tip.
--
sendPinned :: Env -> Request -> Bool -> IO (Either Verdict (Answer, Answer))
sendPinned env r force = case _reqTarget r of
    Nothing -> return (Left (Skipped Unpinned))
    Just target -> do
        tips <- concurrently (tipOf env OracleRole r force) (tipOf env TestRole r force)
        case tips of
            (Right to, Right tt) -> case (depthFor to target, depthFor tt target) of
                (Just dO, Just dT) -> bothAnswers <$> concurrently
                    (sendTo env OracleRole r (fillDepth dO r))
                    (sendTo env TestRole r (fillDepth dT r))
                (Nothing, _) -> return $ Left $ Differ $
                    "the target height is above the tip " <> sshow to <> " of the oracle"
                (_, Nothing) -> return $ Left $ Differ $
                    "the target height is above the tip " <> sshow tt <> " of the test side"
            (Left fo, Left ft)
                | fo == ft, ProbeNoPin{} <- fo -> return (Left (Skipped Unpinned))
                | otherwise -> return $ Left $ Differ $
                    "the tip probes failed differently: oracle " <> probeText fo <> "; test side " <> probeText ft
            (Left fo, Right _) -> return $ Left $ Differ $
                "the tip probe failed on the oracle only: " <> probeText fo
            (Right _, Left ft) -> return $ Left $ Differ $
                "the tip probe failed on the test side only: " <> probeText ft

sendPlain :: Env -> Request -> IO (Either Verdict (Answer, Answer))
sendPlain env r = bothAnswers <$> concurrently
    (sendTo env OracleRole r (_reqPath r))
    (sendTo env TestRole r (_reqPath r))

-- -------------------------------------------------------------------------- --
-- Orphan payloads

-- | The canonical header of a side at a height: the branch below the side's
-- current cut, at that height.
--
canonicalAt :: Env -> Side -> T.Text -> ChainId -> BlockHeight -> IO (Either T.Text CanonicalHeader)
canonicalAt env side prefix cid h =
    sendRaw env side methodGet (prefix <> "/cut") [(hAccept, "application/json")] Nothing >>= \case
        Left e -> return (Left e)
        Right (200, cb) -> case decodeStrict' cb >>= parseMaybe cutHash of
            Nothing -> return (Left "the cut does not decode")
            Just top -> branch top
        Right (s, _) -> return (Left ("the cut: status " <> sshow s))
  where
    hs = sshow (getBlockHeight h)
    branchPath = prefix <> "/chain/" <> toText cid <> "/header/branch?minheight=" <> hs <> "&maxheight=" <> hs

    branch top = sendRaw env side methodPost branchPath
        [ (hAccept, "application/json;blockheader-encoding=object")
        , (hContentType, "application/json")
        ]
        (Just (BL.toStrict $ encode $ object [ "lower" .= ([] :: [T.Text]), "upper" .= [top] ]))
        >>= \case
            Left e -> return (Left e)
            Right (200, bb) -> return $ case decodeStrict' bb >>= parseMaybe items of
                Nothing -> Left "the branch does not decode"
                Just xs -> case [ c | (ht, c) <- xs, ht == h ] of
                    [c] -> Right c
                    [] -> Left ("no canonical header at height " <> hs <> " on " <> _sideName side)
                    _ -> Left ("more than one header at height " <> hs <> " on the branch of " <> _sideName side)
            Right (s, _) -> return (Left ("the branch: status " <> sshow s))

    cutHash :: Value -> Parser T.Text
    cutHash = withObject "cut" $ \o -> do
        m <- o .: "hashes"
        e <- withObject "hashes" (\x -> x .: K.fromText (toText cid)) m
        withObject "block hash with height" (.: "hash") e

    items :: Value -> Parser [(BlockHeight, CanonicalHeader)]
    items = withObject "page" $ \o -> do
        xs <- o .: "items"
        forM xs $ withObject "header" $ \x -> (,)
            <$> x .: "height"
            <*> (CanonicalHeader <$> x .: "hash" <*> x .: "payloadHash")

checkOrphan :: Env -> Request -> BlockPayloadHash -> IO Verdict
checkOrphan env r ph = case (_reqTarget r, apiPrefixOfPath (_reqPath r)) of
    (Just h, Just prefix) -> do
        (co, ct) <- concurrently
            (canonicalAt env (_envOracle env) prefix cid h)
            (canonicalAt env (_envTest env) prefix cid h)
        return (orphanVerdict cid h ph co ct)
    _ -> return $ Differ "found on the oracle only; no target height or API prefix for the orphan check"
  where
    cid = _reqChain r

-- -------------------------------------------------------------------------- --
-- Comparing

compareOne :: Env -> (Int, Request) -> IO Outcome
compareOne env (i, r) = do
    atomicModifyIORef' (_envStarted env) (\n -> (n + 1, ()))
    case preJudge (_envCoverage env) r of
        Just v -> return (Outcome i r v Nothing Nothing 0)
        Nothing -> go 1
  where
    go :: Int -> IO Outcome
    go attempt = do
        sent <- if isPinnedClass (_reqClass r)
            then sendPinned env r (attempt > 1)
            else sendPlain env r
        case sent of
            Left v -> return (Outcome i r v Nothing Nothing attempt)
            Right (a, b) -> case judge (_envCoverage env) r a b of
                Final v -> done v a b
                RetryPinMismatch
                    | attempt <= maxPinRetries -> go (attempt + 1)
                    | otherwise -> done (Skipped PinUnstable) a b
                CheckOrphan ph -> do
                    v <- checkOrphan env r ph
                    done v a b
                CompareSpvSubjects -> done (compareSpvProofs a b) a b
      where
        done v a b = return (Outcome i r v (Just a) (Just b) attempt)

-- | Compares the answers of the oracle and the test side to the requests, with
-- 'compareWorkers' requests in flight.
--
-- A request outside the coverage is 'Skipped' 'NotCovered' before anything is
-- sent to either side (ruling 1.14). A pinned request is sent to each side at
-- the depth of its target below that side's tip; if the pins of the answers
-- disagree, the tips are probed again and the request is sent again, at most
-- 'maxPinRetries' times, after which it is 'Skipped' 'PinUnstable'.
--
runCompare :: Logger l => l -> Coverage -> Side -> Side -> [Request] -> IO Report
runCompare logger cov oracle test reqs = do
    mgr <- HTTP.newTlsManagerWith HTTP.tlsManagerSettings
        { HTTP.managerResponseTimeout = HTTP.responseTimeoutMicro 120_000_000 }
    env <- Env mgr cov oracle test <$> newIORef mempty <*> newIORef 0
    queue <- newMVar (zip [0 ..] reqs)
    rep <- newMVar emptyReport
    finished <- newIORef (0 :: Int)
    let worker = do
            next <- modifyMVar queue $ \case
                [] -> return ([], Nothing)
                x : xs -> return (xs, Just x)
            forM_ next $ \x -> do
                o <- compareOne env x
                case effectiveVerdict (_outVerdict o) of
                    Differ d -> logg Warn $ "difference in request #" <> sshow (_outIndex o)
                        <> " (" <> _reqPath (_outRequest o) <> "): " <> d
                    _ -> return ()
                modifyMVar_ rep (evaluate . flip addOutcome o)
                n <- atomicModifyIORef' finished (\k -> (k + 1, k + 1))
                when (n `mod` 1_000 == 0) $ logg Info $ "compared " <> sshow n <> " requests"
                worker
    replicateConcurrently_ compareWorkers worker
    result <- readMVar rep
    logg Info $ "verdict: " <> sshow (reportVerdict result)
        <> ", compared " <> sshow (reportCompared result)
        <> ", differences " <> sshow (reportDiffCount result)
    return result
  where
    logg = logFunctionText logger
