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
, RunOptions(..)
, defaultRunOptions
, runCompareWith
, runCompareClasses
, tipProbe
, isPreSendFailure
, newCompareManager
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
import Data.Int (Int64)
import qualified Data.Text.Encoding as T
import Data.Word

import qualified Network.HTTP.Client as HTTP
import qualified Network.HTTP.Client.TLS as HTTP
import Network.HTTP.Types (Header, Method, hAccept, hContentType, methodGet, methodPost, statusCode)

import Pact.Core.Hash (hash, hashToText)

import Servant.Client (baseUrlHost, parseBaseUrl)

import System.Clock (Clock(Realtime), TimeSpec(..), getTime)

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

-- | Parameters of the runner; tests shorten them.
--
data RunOptions = RunOptions
    { _roTransportAttempts :: !Int
        -- ^ attempts of a call that failed before it reached the server
        -- ('isPreSendFailure'). A call that may have reached it is never
        -- sent again.
    , _roRetryDelay :: !Int
        -- ^ microseconds before the second attempt, doubled before each
        -- further one
    , _roResponseTimeout :: !Int
        -- ^ microseconds
    , _roOracleLocalTimeout :: !Int
        -- ^ microseconds; the response timeout of a @/local@ on the oracle
        -- (tip probes included), in place of '_roResponseTimeout'
    , _roProbeAttempts :: !Int
        -- ^ tip probes sent while they answer without block metadata
    }

-- | 5 attempts with delays of 1, 2, 4 and 8 seconds, for calls that never
-- reached the server; a response timeout of 120 seconds, 30 seconds for a
-- @/local@ on the oracle; 3 tip probes.
--
defaultRunOptions :: RunOptions
defaultRunOptions = RunOptions
    { _roTransportAttempts = 5
    , _roRetryDelay = 1_000_000
    , _roResponseTimeout = 120_000_000
    , _roOracleLocalTimeout = 30_000_000
    , _roProbeAttempts = 3
    }

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
    { _envOptions :: !RunOptions
    , _envManager :: !HTTP.Manager
    , _envCoverage :: !(RequestClass -> Coverage)
        -- ^ the coverage of each class, see 'classCoverage'
    , _envOracle :: !Side
    , _envTest :: !Side
    , _envTips :: !(IORef (M.Map (Role, ChainId) (BlockHeight, Int)))
        -- ^ tip and the request count when it was probed
    , _envStarted :: !(IORef Int)
        -- ^ requests started
    , _envLog :: !(LogLevel -> T.Text -> IO ())
    }

sideOf :: Env -> Role -> Side
sideOf env OracleRole = _envOracle env
sideOf env TestRole = _envTest env

-- -------------------------------------------------------------------------- --
-- HTTP

-- | Whether a failed call never reached the server: the connection could not
-- be opened (refused, unreachable, a name that does not resolve) or was not
-- opened in time. Only such a call is sent again.
--
-- Every other failure is final: a response timeout, a connection closed or
-- reset after the request was written, an empty or broken answer. The server
-- may have the request and be working on it; a @/local@ that runs on a busy
-- production chain must not be queued there a second time (I1 of the P0
-- review).
--
isPreSendFailure :: HTTP.HttpException -> Bool
isPreSendFailure = \case
    HTTP.HttpExceptionRequest _ (HTTP.ConnectionFailure _) -> True
    HTTP.HttpExceptionRequest _ HTTP.ConnectionTimeout -> True
    _ -> False

-- | The HTTP manager of @cw-compare@: the given response timeout in
-- microseconds, and every request on a connection of its own. A pooled
-- connection that the server closed while idle looks like a request without
-- an answer, and http-client would send such a request again on its own; a
-- connection that is never reused rules both out.
--
newCompareManager :: Int -> IO HTTP.Manager
newCompareManager timeout = HTTP.newTlsManagerWith HTTP.tlsManagerSettings
    { HTTP.managerResponseTimeout = HTTP.responseTimeoutMicro timeout
    , HTTP.managerIdleConnectionCount = 0
    , HTTP.managerRetryableException = const False
    }

-- | Sends a request through the limiter of the side's host. Any HTTP status
-- is an answer. A call that failed before it reached the server
-- ('isPreSendFailure') is tried '_roTransportAttempts' times (by default 5,
-- with delays of 1, 2, 4 and 8 seconds); each attempt takes a permit. Any
-- other failure is final and sent once only. A @/local@ to the oracle waits
-- '_roOracleLocalTimeout' for its answer.
--
sendRaw
    :: Env
    -> Role
    -> Method
    -> T.Text
    -> [Header]
    -> Maybe B.ByteString
    -> IO (Either T.Text (Int, B.ByteString))
sendRaw env role method path hdrs body = go (1 :: Int)
  where
    opts = _envOptions env
    side = sideOf env role
    url = T.unpack (T.dropWhileEnd (== '/') (_sideBaseUrl side) <> path)
    timeout
        | role == OracleRole, "/pact/api/v1/local" `T.isInfixOf` path =
            HTTP.responseTimeoutMicro (_roOracleLocalTimeout opts)
        | otherwise = HTTP.responseTimeoutMicro (_roResponseTimeout opts)
    go attempt = do
        r <- try $ throttled (_sideLimiter side) $ do
            req0 <- HTTP.parseRequest url
            let req = req0
                    { HTTP.method = method
                    , HTTP.requestHeaders = hdrs
                    , HTTP.requestBody = maybe (HTTP.requestBody req0) HTTP.RequestBodyBS body
                    , HTTP.responseTimeout = timeout
                    }
            HTTP.httpLbs req (_envManager env)
        case r of
            Right resp -> return $ Right
                (statusCode (HTTP.responseStatus resp), BL.toStrict (HTTP.responseBody resp))
            Left (e :: HTTP.HttpException)
                | isPreSendFailure e, attempt < _roTransportAttempts opts -> do
                    threadDelay (_roRetryDelay opts * 2 ^ (attempt - 1))
                    go (attempt + 1)
                | otherwise -> return $ Left $
                    "no answer from " <> _sideName side <> " for " <> T.pack url
                    <> " after " <> sshow attempt <> " attempt(s): " <> sshow e

requestHeaders :: Request -> [Header]
requestHeaders r = (hAccept, "application/json")
    : [ (hContentType, "application/json") | isJust (_reqBody r) ]

toAnswer :: Request -> (Int, B.ByteString) -> Answer
toAnswer r (s, b) = Answer s b $ if isPinnedClass (_reqClass r) then answerPin b else Nothing

sendTo :: Env -> Role -> Request -> T.Text -> IO (Either T.Text Answer)
sendTo env role r path = fmap (toAnswer r) <$>
    sendRaw env role (_reqMethod r) path (requestHeaders r) (_reqBody r)

-- | Sending a request once to both sides.
--
data Attempt
    = Decided !Verdict !(Maybe NoAnswer)
        -- ^ decided before the request was sent: a tip probe failed, or the
        -- target is above a tip
    | Answers !(Maybe (Word64, Word64)) !(Either T.Text Answer) !(Either T.Text Answer)
        -- ^ the depths used on the oracle and the test side (pinned classes
        -- only), and what each side answered

-- -------------------------------------------------------------------------- --
-- Tips

data ProbeFailure
    = ProbeTransport !T.Text
    | ProbeNoPin !Int !B.ByteString

probeText :: ProbeFailure -> T.Text
probeText = \case
    ProbeTransport e -> e
    ProbeNoPin s b -> "status " <> sshow s <> " without block metadata, " <> sshow (B.length b) <> " bytes"

data TipDecision = UseCached !BlockHeight | ProbeTip !(Maybe BlockHeight)

-- | The tip probe of a chain, the same for every request class: @(+ 1 1)@ as
-- an unsigned @/local@ command at rewind depth 0. Its answer carries the
-- block metadata on Pact 4 and Pact 5 alike, whatever the code of the request
-- would do at the tip.
--
-- Takes the API prefix @\/chainweb\/0.0\/{version}@ (the version is the network
-- id), the chain and the current time in seconds since the epoch; returns the
-- path and the body.
--
tipProbe :: T.Text -> ChainId -> Int64 -> (T.Text, B.ByteString)
tipProbe prefix cid now = (path, BL.toStrict (encode command))
  where
    path = prefix <> "/chain/" <> toText cid <> "/pact/api/v1/local?rewindDepth=0"
    network = case reverse (filter (not . T.null) (T.splitOn "/" prefix)) of
        v : _ -> v
        [] -> ""
    payload = BL.toStrict $ encode $ object
        [ "networkId" .= network
        , "payload" .= object
            [ "exec" .= object [ "code" .= ("(+ 1 1)" :: T.Text), "data" .= object [] ] ]
        , "signers" .= ([] :: [Value])
        , "meta" .= object
            [ "chainId" .= toText cid
            , "sender" .= ("cw-compare" :: T.Text)
            , "gasLimit" .= (1_000 :: Int)
            , "gasPrice" .= (0.00000001 :: Double)
            , "ttl" .= (3_600 :: Int)
            , "creationTime" .= (now - 60)
            ]
        , "nonce" .= ("cw-compare tip probe " <> sshow now)
        ]
    command = object
        [ "hash" .= hashToText (hash payload)
        , "sigs" .= ([] :: [Value])
        , "cmd" .= T.decodeUtf8 payload
        ]

-- | The tip of a side on a chain: the metadata height of its 'tipProbe'.
-- Cached per side and chain; probed again after 'tipRefreshInterval'
-- requests, or when forced. While one worker probes, others use the cached
-- tip.
--
-- A probe answered without block metadata is sent again, up to
-- '_roProbeAttempts' probes in all; a probe without an HTTP answer is not
-- (see 'sendRaw'). If the probe fails, the cached tip is used if there is
-- one.
--
tipOf :: Env -> Role -> T.Text -> ChainId -> Bool -> IO (Either ProbeFailure BlockHeight)
tipOf env role prefix cid force = do
    now <- readIORef (_envStarted env)
    decision <- atomicModifyIORef' (_envTips env) $ \m -> case M.lookup key m of
        Just (tip, stamp)
            | not force && now - stamp < tipRefreshInterval -> (m, UseCached tip)
            | otherwise -> (M.insert key (tip, now) m, ProbeTip (Just tip))
        Nothing -> (m, ProbeTip Nothing)
    case decision of
        UseCached tip -> return (Right tip)
        ProbeTip old -> probe (1 :: Int) >>= \case
            Right tip -> do
                n <- readIORef (_envStarted env)
                atomicModifyIORef' (_envTips env) (\m -> (M.insert key (tip, n) m, ()))
                return (Right tip)
            Left f -> return $ maybe (Left f) Right old
  where
    key = (role, cid)
    opts = _envOptions env
    probe n = do
        TimeSpec secs _ <- getTime Realtime
        let (path, body) = tipProbe prefix cid secs
        sendRaw env role methodPost path
            [(hAccept, "application/json"), (hContentType, "application/json")] (Just body) >>= \case
            Left e -> return (Left (ProbeTransport e))
            Right (s, b) -> case answerPin b of
                Just p -> return (Right (_pinHeight p))
                Nothing
                    | n < _roProbeAttempts opts -> do
                        threadDelay (_roRetryDelay opts * 2 ^ (n - 1))
                        probe (n + 1)
                    | otherwise -> return (Left (ProbeNoPin s b))

-- | Sends a pinned request to both sides, each at the depth of the target
-- below its own tip.
--
-- * The oracle's probe got no HTTP answer: 'Skipped' 'NoOracleAnswer'.
-- * The oracle's probe answered without block metadata, or the target is
--   above the oracle's tip: 'Skipped' 'Unpinned'. Both count toward 'Void'.
-- * The oracle's probe worked and the test side's did not, or the target is
--   above the test side's tip: 'Differ', since the test side claims to hold
--   the target.
--
sendPinned :: Env -> Request -> Bool -> IO Attempt
sendPinned env r force = case (_reqTarget r, apiPrefixOfPath (_reqPath r)) of
    (Just target, Just prefix) -> do
        tips <- concurrently (tipOf env OracleRole prefix cid force) (tipOf env TestRole prefix cid force)
        case tips of
            (Right to, Right tt) -> case (depthFor to target, depthFor tt target) of
                (Just dO, Just dT) -> uncurry (Answers (Just (dO, dT))) <$> concurrently
                    (sendTo env OracleRole r (fillDepth dO r))
                    (sendTo env TestRole r (fillDepth dT r))
                (Nothing, _) -> do
                    logg Warn $ "the target of " <> _reqPath r <> " is above the tip " <> sshow to <> " of the oracle"
                    return (Decided (Skipped Unpinned) Nothing)
                (_, Nothing) -> return $ Decided
                    (Differ ("the target height is above the tip " <> sshow tt <> " of the test side"))
                    Nothing
            (Left fo@(ProbeTransport _), ft) -> do
                logg Warn $ "the tip probe of the oracle failed: " <> probeText fo
                return $ Decided (Skipped NoOracleAnswer) $ Just $ case ft of
                    Left (ProbeTransport _) -> NoAnswerBoth
                    _ -> NoAnswerOracle
            (Left fo@ProbeNoPin{}, ft) -> do
                logg Warn $ "the tip probe of the oracle failed: " <> probeText fo
                return $ Decided (Skipped Unpinned) $ case ft of
                    Left (ProbeTransport _) -> Just NoAnswerTest
                    _ -> Nothing
            (Right _, Left ft) -> return $ Decided
                (Differ ("the tip probe failed on the test side only: " <> probeText ft))
                (case ft of ProbeTransport _ -> Just NoAnswerTest; ProbeNoPin{} -> Nothing)
    _ -> return (Decided (Skipped Unpinned) Nothing)
  where
    cid = _reqChain r
    logg = _envLog env

sendPlain :: Env -> Request -> IO Attempt
sendPlain env r = uncurry (Answers Nothing) <$> concurrently
    (sendTo env OracleRole r (_reqPath r))
    (sendTo env TestRole r (_reqPath r))

-- -------------------------------------------------------------------------- --
-- Orphan payloads

headerItems :: Value -> Parser [(BlockHeight, HeaderRef)]
headerItems = withObject "page" $ \o -> do
    xs <- o .: "items"
    forM xs $ withObject "header" $ \x -> (,)
        <$> x .: "height"
        <*> (HeaderRef <$> x .: "hash" <*> x .: "payloadHash")

-- | The canonical header of a side at a height: the branch below the side's
-- current cut, at that height.
--
canonicalAt :: Env -> Role -> T.Text -> ChainId -> BlockHeight -> IO (Either T.Text HeaderRef)
canonicalAt env role prefix cid h =
    sendRaw env role methodGet (prefix <> "/cut") [(hAccept, "application/json")] Nothing >>= \case
        Left e -> return (Left e)
        Right (200, cb) -> case decodeStrict' cb >>= parseMaybe cutHash of
            Nothing -> return (Left "the cut does not decode")
            Just top -> branch top
        Right (s, _) -> return (Left ("the cut: status " <> sshow s))
  where
    side = sideOf env role
    hs = sshow (getBlockHeight h)
    branchPath = prefix <> "/chain/" <> toText cid <> "/header/branch?minheight=" <> hs <> "&maxheight=" <> hs

    branch top = sendRaw env role methodPost branchPath
        [ (hAccept, "application/json;blockheader-encoding=object")
        , (hContentType, "application/json")
        ]
        (Just (BL.toStrict $ encode $ object [ "lower" .= ([] :: [T.Text]), "upper" .= [top] ]))
        >>= \case
            Left e -> return (Left e)
            Right (200, bb) -> return $ case decodeStrict' bb >>= parseMaybe headerItems of
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

-- | Every header of a side at a height, canonical or not: the first page of
-- @GET .../header?minheight=h&maxheight=h@.
--
headersAt :: Env -> Role -> T.Text -> ChainId -> BlockHeight -> IO (Either T.Text [HeaderRef])
headersAt env role prefix cid h =
    sendRaw env role methodGet path [(hAccept, "application/json;blockheader-encoding=object")] Nothing >>= \case
        Left e -> return (Left e)
        Right (200, b) -> return $ case decodeStrict' b >>= parseMaybe headerItems of
            Nothing -> Left "the headers do not decode"
            Just xs -> Right [ x | (ht, x) <- xs, ht == h ]
        Right (s, _) -> return (Left ("the headers: status " <> sshow s))
  where
    hs = sshow (getBlockHeight h)
    path = prefix <> "/chain/" <> toText cid <> "/header?minheight=" <> hs <> "&maxheight=" <> hs

-- | Looks up the headers at the target height for 'orphanVerdict': every
-- header of the oracle there, and the canonical header of each side.
--
checkOrphan :: Env -> Request -> BlockPayloadHash -> IO Verdict
checkOrphan env r ph = case (_reqTarget r, apiPrefixOfPath (_reqPath r)) of
    (Just h, Just prefix) -> do
        ((hs, co), ct) <- concurrently
            (concurrently
                (headersAt env OracleRole prefix cid h)
                (canonicalAt env OracleRole prefix cid h))
            (canonicalAt env TestRole prefix cid h)
        return (orphanVerdict cid h ph hs co ct)
    _ -> return $ Differ "found on the oracle only; no target height or API prefix for the orphan check"
  where
    cid = _reqChain r

-- -------------------------------------------------------------------------- --
-- Comparing

compareOne :: Env -> (Int, Request) -> IO Outcome
compareOne env (i, r) = do
    atomicModifyIORef' (_envStarted env) (\n -> (n + 1, ()))
    case preJudge (_envCoverage env (_reqClass r)) r of
        Just v -> return (Outcome i r v Nothing Nothing 0 Nothing Nothing)
        Nothing -> go 1
  where
    go :: Int -> IO Outcome
    go attempt = do
        sent <- if isPinnedClass (_reqClass r)
            then sendPinned env r (attempt > 1)
            else sendPlain env r
        case sent of
            Decided v na -> return (Outcome i r v Nothing Nothing attempt Nothing na)
            Answers ds (Right a) (Right b) ->
                let done v = return (Outcome i r v (Just a) (Just b) attempt ds Nothing)
                in case judge (_envCoverage env (_reqClass r)) r a b of
                    Final v -> done v
                    RetryPinMismatch
                        | attempt <= maxPinRetries -> go (attempt + 1)
                        | otherwise -> done (Skipped PinUnstable)
                    CheckOrphan ph -> checkOrphan env r ph >>= done
                    CompareSpvSubjects -> done (compareSpvProofs a b)
            Answers ds (Right a) (Left e) ->
                return (Outcome i r (Differ e) (Just a) Nothing attempt ds (Just NoAnswerTest))
            Answers ds (Left e) eb -> do
                _envLog env Warn $ "request #" <> sshow i <> ": " <> e
                return $ Outcome i r (Skipped NoOracleAnswer) Nothing (either (const Nothing) Just eb) attempt ds
                    (Just (either (const NoAnswerBoth) (const NoAnswerOracle) eb))

-- | Compares the answers of the oracle and the test side to the requests, with
-- 'compareWorkers' requests in flight.
--
-- A request outside the coverage is 'Skipped' 'NotCovered' before anything is
-- sent to either side (ruling 1.14). A pinned request is sent to each side at
-- the depth of its target below that side's tip; if the pins of the answers
-- disagree, the tips are probed again and the request is sent again, at most
-- 'maxPinRetries' times, after which it is 'Skipped' 'PinUnstable'. A side's
-- tip is the metadata height of 'tipProbe', the same for every class.
--
-- A request the test side alone gives no HTTP answer to is a difference that
-- keeps the oracle's answer. One the oracle gives no HTTP answer to is
-- 'Skipped' 'NoOracleAnswer', whatever the test side did. A request that may
-- have reached a side is never sent to it again ('isPreSendFailure').
--
runCompare :: Logger l => l -> Coverage -> Side -> Side -> [Request] -> IO Report
runCompare logger = runCompareWith logger defaultRunOptions

-- | 'runCompare' with other 'RunOptions'.
--
runCompareWith :: Logger l => l -> RunOptions -> Coverage -> Side -> Side -> [Request] -> IO Report
runCompareWith logger opts cov = runCompareClasses logger opts (const cov)

-- | 'runCompareWith' with a coverage per request class, for instance
-- @'classCoverage' (Just pactChains) cov@ for an in-process test side: a
-- request outside the coverage of its class is 'Skipped' 'NotCovered' and
-- never sent.
--
runCompareClasses
    :: Logger l
    => l
    -> RunOptions
    -> (RequestClass -> Coverage)
    -> Side
    -> Side
    -> [Request]
    -> IO Report
runCompareClasses logger opts cov oracle test reqs = do
    mgr <- newCompareManager (_roResponseTimeout opts)
    env <- Env opts mgr cov oracle test <$> newIORef mempty <*> newIORef 0 <*> pure logg
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
                    Skipped reason | _outAttempts o > 0 -> logg Warn $ "request #" <> sshow (_outIndex o)
                        <> " (" <> _reqPath (_outRequest o) <> ") skipped: " <> skipReasonText reason
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
