{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.StorageTools.Compare.Requests
--
-- The request generator of @cw-compare@: turns a sampled target (class,
-- chain, height) into a 'Request' that is sent unchanged to both sides.
--
-- A request is deterministic per seed and target. Its random choices come
-- from a generator seeded with (seed, class, chain, height), and the facts it
-- needs (hashes, request keys, accounts, txids, pact ids) come from the
-- oracle's canonical block at the target, or a few blocks above it. Every
-- lookup on the oracle takes a permit of the oracle host's 'Limiter', the one
-- in '_saLimiter' (ruling 3.3); 'oracleServiceApi' builds a 'ServiceApi' with
-- the limiter of 'limiterFor', which the oracle 'Side' shares.
--
-- Lookups go upward from the sampled height, and a request's target is the
-- highest height it touches, so a request that leaves the coverage is skipped
-- as 'NotCovered' by the runner and never sent.
--
module Chainweb.StorageTools.Compare.Requests
( -- * @/local@ commands
  LocalPayload(..)
, LocalMeta(..)
, defaultLocalMeta
, localCommand
, localCommandWith
, localCreationTime
, localGasLimit
, historyGasLimit
, localTtl
, preflightTimeSlack
  -- * Generating requests
, GenOptions(..)
, defaultGenOptions
, requestsFor
, requestsForWith
, generateRequests
, GenSkip(..)
, skipNoTx
, skipLookupFailed
, generateRequestsSkipping
, generateWorkers
, RequestGenError(..)
, oracleServiceApi
, apiPrefix
, txScanWindow
, txScanMax
, txScanStep
, maxRangeExtent
, maxBatchExtra
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently)
import Control.Exception (Exception, throwIO, try)
import Control.Monad
import Data.Aeson
import qualified Data.Aeson.Key as K
import Data.Aeson.Types (Parser, parseMaybe)
import Data.Bits (shiftR, xor)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.Char (isAscii, isAlphaNum)
import qualified Data.HashSet as HS
import Data.Int (Int64)
import Data.List (nubBy, sort, sortOn)
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe, mapMaybe, maybeToList)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word
import qualified Network.HTTP.Client as HTTP
import Network.HTTP.Types (Header, Method, hAccept, hContentType, methodGet, methodPost, statusCode)
import Pact.Core.Hash (hash, hashToText)
import System.Random (StdGen, mkStdGen, uniformR)

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.StorageTools.Compare.PactSet
import Chainweb.StorageTools.Compare.Run (HostLimiters, isPreSendFailure, isRefusal, limiterFor)
import Chainweb.StorageTools.Compare.Sample (historyMaxHeight)
import Chainweb.StorageTools.Compare.Types
import Chainweb.StorageTools.Http
import Chainweb.Utils (decodeB64UrlNoPaddingText, encodeB64UrlNoPaddingText, toText)
import Chainweb.Version

tshow :: Show a => a -> T.Text
tshow = T.pack . show

encodeStrict :: ToJSON a => a -> B.ByteString
encodeStrict = BL.toStrict . encode

-- -------------------------------------------------------------------------- --
-- /local commands

-- | What a @/local@ command runs.
--
data LocalPayload
    = LocalExec !T.Text
        -- ^ code
    | LocalCont !T.Text !Int
        -- ^ a continuation: pact id and step, rollback false, no proof
    deriving (Show, Eq)

-- | The fields of a command that are not fixed.
--
data LocalMeta = LocalMeta
    { _lmSender :: !T.Text
    , _lmSigners :: ![T.Text]
        -- ^ public keys of unrestricted signers; only with
        -- @signatureVerification=false@, since the command has no signatures
    , _lmCreationTime :: !Int64
        -- ^ seconds since the epoch
    , _lmGasLimit :: !Int
    }
    deriving (Show, Eq)

-- | Sender @cw-compare@, no signers, 'localCreationTime', 'localGasLimit'.
--
defaultLocalMeta :: LocalMeta
defaultLocalMeta = LocalMeta "cw-compare" [] localCreationTime localGasLimit

-- | The fixed creation time of every @/local@ command but preflight:
-- 2020-09-13T12:26:40Z. Plain @/local@ does not check it.
--
localCreationTime :: Int64
localCreationTime = 1_600_000_000

-- | One day, below Pact's maximum of two days.
--
localTtl :: Int
localTtl = 86_400

-- | A preflight command is created this many seconds before the target
-- block, so that it is valid on top of the target block and its parent.
--
preflightTimeSlack :: Int64
preflightTimeSlack = 600

-- | The @/local@ command for (chain, code, seed): fixed creation time
-- ('localCreationTime'), nonce @"cw-compare <seed>"@, gas limit, gas price
-- and TTL. It is built once and the same bytes go to both sides.
--
localCommand :: ChainwebVersion -> ChainId -> T.Text -> Word64 -> B.ByteString
localCommand v cid code = localCommandWith v cid defaultLocalMeta (LocalExec code)

-- | 'localCommand' with any payload and meta data. The command is unsigned;
-- its hash is the Blake2b hash of the payload bytes.
--
localCommandWith :: ChainwebVersion -> ChainId -> LocalMeta -> LocalPayload -> Word64 -> B.ByteString
localCommandWith v cid lm pl seed = encodeStrict $ object
    [ "hash" .= hashToText (hash payload)
    , "sigs" .= ([] :: [Value])
    , "cmd" .= T.decodeUtf8 payload
    ]
  where
    payload = encodeStrict $ object
        [ "networkId" .= getChainwebVersionName (_versionName v)
        , "payload" .= case pl of
            LocalExec code -> object
                [ "exec" .= object [ "code" .= code, "data" .= object [] ] ]
            LocalCont pid step -> object
                [ "cont" .= object
                    [ "pactId" .= pid
                    , "step" .= step
                    , "rollback" .= False
                    , "data" .= object []
                    , "proof" .= Null
                    ]
                ]
        , "signers" .= [ object [ "pubKey" .= k ] | k <- _lmSigners lm ]
        , "meta" .= object
            [ "chainId" .= toText cid
            , "sender" .= _lmSender lm
            , "gasLimit" .= _lmGasLimit lm
            , "gasPrice" .= (0.00000001 :: Double)
            , "ttl" .= localTtl
            , "creationTime" .= _lmCreationTime lm
            ]
        , "nonce" .= ("cw-compare " <> tshow seed)
        ]

-- -------------------------------------------------------------------------- --
-- Options and constants

newtype GenOptions = GenOptions
    { _goSameSnapshot :: Bool
        -- ^ both sides were copied from one RocksDB snapshot, so their orphan
        -- headers are the same: plain @/header?minheight&maxheight@ ranges are
        -- generated too
    }
    deriving (Show, Eq)

defaultGenOptions :: GenOptions
defaultGenOptions = GenOptions False

-- | Poll and SPV requests look for a block with transactions from the
-- sampled height up, first in this many blocks, then in windows of
-- 'txScanStep' blocks, up to 'txScanMax' blocks in all.
--
txScanWindow :: Word64
txScanWindow = 8

txScanStep :: Word64
txScanStep = 64

-- | A poll or SPV target without a block with a transaction in this many
-- blocks from the sampled height up (or up to the oracle's tip) is skipped
-- at generation with the reason 'skipNoTx' (ruling C3 on Task 6): a request
-- of unknown keys only is answered the same by any two nodes and would be a
-- hollow pass.
--
txScanMax :: Word64
txScanMax = 1_000

-- | A header range spans up to this many blocks above the sampled height.
--
maxRangeExtent :: Word64
maxRangeExtent = 9

-- | A payload batch holds up to this many blocks above the sampled height.
--
maxBatchExtra :: Word64
maxBatchExtra = 4

-- | The oracle could not give what a request needs, or the target cannot
-- have a request of its class.
--
data RequestGenError
    = RequestGenError !T.Text
    | NoTxWithin !Word64 !Word64
        -- ^ no block with a transaction in the heights [lo, hi] (poll and
        -- SPV), see 'txScanMax'
    deriving Show

instance Exception RequestGenError

genError :: T.Text -> IO a
genError = throwIO . RequestGenError

-- | The oracle's service API, throttled by the limiter of its host from
-- 'limiterFor': the limiter the oracle 'Side' gets from the same
-- 'HostLimiters'.
--
-- Its manager keeps connections open between lookups, which are many and
-- cheap. On a reused connection that closes without an answer (the oracle
-- closed it while idle), http-client sends the lookup once more on a new
-- connection. A lookup is a read of headers or payloads, never a @/local@;
-- the runner's manager ('newCompareManager') reuses no connection.
--
oracleServiceApi :: HostLimiters -> ChainwebVersion -> T.Text -> IO ServiceApi
oracleServiceApi hl v url = limiterFor hl url >>= newServiceApi url v

-- | @/chainweb/0.0/{version}@
--
apiPrefix :: ChainwebVersion -> T.Text
apiPrefix v = "/chainweb/0.0/" <> getChainwebVersionName (_versionName v)

-- -------------------------------------------------------------------------- --
-- Seeded choices

mix64 :: Word64 -> Word64
mix64 z0 = z2 `xor` (z2 `shiftR` 31)
  where
    z1 = (z0 `xor` (z0 `shiftR` 30)) * 0xbf58476d1ce4e5b9
    z2 = (z1 `xor` (z1 `shiftR` 27)) * 0x94d049bb133111eb

targetGen :: Word64 -> RequestClass -> ChainId -> Word64 -> StdGen
targetGen seed cls cid h = mkStdGen $ fromIntegral $ foldl (\a x -> mix64 (a `xor` x)) (mix64 seed)
    [ fromIntegral (fromEnum cls), chainIdInt cid, h ]

pickOr :: a -> [a] -> StdGen -> (a, StdGen)
pickOr d [] g = (d, g)
pickOr _ xs g = let (i, g') = uniformR (0, length xs - 1) g in (xs !! i, g')

-- | Up to n distinct elements, in a seeded order.
--
sampleN :: Int -> [a] -> StdGen -> ([a], StdGen)
sampleN n xs g
    | n <= 0 || null xs = ([], g)
    | otherwise =
        let (i, g1) = uniformR (0, length xs - 1) g
        in case splitAt i xs of
            (as, b : bs) -> let (rest, g2) = sampleN (n - 1) (as <> bs) g1 in (b : rest, g2)
            (_, []) -> ([], g1)

-- | A request key that no block has: 32 seeded random bytes.
--
unknownKey :: StdGen -> (T.Text, StdGen)
unknownKey g0 = (encodeB64UrlNoPaddingText (B.pack (concatMap bytes ws)), g4)
  where
    (w1, g1) = uniformR (0, maxBound :: Word64) g0
    (w2, g2) = uniformR (0, maxBound) g1
    (w3, g3) = uniformR (0, maxBound) g2
    (w4, g4) = uniformR (0, maxBound) g3
    ws = [w1, w2, w3, w4]
    bytes w = [ fromIntegral (w `shiftR` (8 * i)) | i <- [7, 6 .. 0] ]

-- -------------------------------------------------------------------------- --
-- Oracle lookups

oracleAttempts :: Int
oracleAttempts = 5

-- | One call to the oracle. Each attempt takes a permit of '_saLimiter'.
-- A call that failed before it reached the oracle ('isPreSendFailure'), and
-- a refusal ('isRefusal': 429, 503), are tried 5 times in all, after 1, 2, 4
-- and 8 s. Any other failure (a response timeout, a connection closed without
-- an answer) is thrown at once, and any other status is returned: the oracle
-- may have done the work, so the call is never sent again.
--
oracleCall :: ServiceApi -> Method -> T.Text -> [Header] -> Maybe B.ByteString -> IO (Int, B.ByteString)
oracleCall api m path hdrs body = do
    req0 <- HTTP.parseRequest (T.unpack (_saBaseUrl api <> path))
    let req = req0
            { HTTP.method = m
            , HTTP.requestHeaders = hdrs
            , HTTP.requestBody = HTTP.RequestBodyBS (fromMaybe "" body)
            }
        go :: Int -> IO (Int, B.ByteString)
        go attempt = try (throttled (_saLimiter api) (HTTP.httpLbs req (_saManager api))) >>= \case
            Right resp
                | s <- statusCode (HTTP.responseStatus resp)
                , isRefusal s && attempt < oracleAttempts -> backoff attempt >> go (attempt + 1)
                | otherwise -> return (statusCode (HTTP.responseStatus resp), BL.toStrict (HTTP.responseBody resp))
            Left (e :: HTTP.HttpException)
                | isPreSendFailure e, attempt < oracleAttempts -> backoff attempt >> go (attempt + 1)
                | otherwise -> throwIO e
    go 1
  where
    backoff attempt = threadDelay (1_000_000 * 2 ^ (attempt - 1))

oracleValue :: ServiceApi -> Method -> T.Text -> [Header] -> Maybe B.ByteString -> IO Value
oracleValue api m path hdrs body = oracleCall api m path hdrs body >>= \case
    (200, b) -> maybe (genError ("the oracle's answer does not decode: " <> path)) return (decodeStrict b)
    (s, _) -> genError ("the oracle answers status " <> tshow s <> " for " <> path)

data HeaderInfo = HeaderInfo
    { _hiHeight :: !Word64
    , _hiHash :: !T.Text
    , _hiPayload :: !T.Text
    , _hiCreation :: !Int64
        -- ^ microseconds since the epoch
    }

chainPrefix :: ServiceApi -> ChainId -> T.Text
chainPrefix api cid = apiPrefix (_saVersion api) <> "/chain/" <> toText cid

-- | The canonical headers with heights in [lo, hi], below the oracle's
-- current cut, in ascending order of height. Two lookups: the cut, and a
-- branch query from the cut's hash.
--
-- @hi@ is clamped to the chain's height in the cut: a 3.2.1 branch query
-- whose maxheight is above its upper bound is empty, not cut at the tip
-- (Minor 1 of the P0 review). Above the cut there are no headers, and no
-- branch query is made.
--
canonicalHeaders :: ServiceApi -> ChainId -> Word64 -> Word64 -> IO [HeaderInfo]
canonicalHeaders api cid lo hi0 = do
    cut <- oracleValue api methodGet (apiPrefix (_saVersion api) <> "/cut") [(hAccept, "application/json")] Nothing
    (top, tipHeight) <- maybe (genError "the oracle's cut does not decode") return (parseMaybe cutEntry cut)
    let hi = min hi0 tipHeight
    if lo > hi then return [] else do
        page <- oracleValue api methodPost
            (chainPrefix api cid <> "/header/branch?minheight=" <> tshow lo <> "&maxheight=" <> tshow hi)
            [ (hAccept, "application/json;blockheader-encoding=object"), (hContentType, "application/json") ]
            (Just (encodeStrict (object [ "lower" .= ([] :: [T.Text]), "upper" .= [top] ])))
        hs <- maybe (genError "the oracle's branch does not decode") return (parseMaybe items page)
        return $ sortOn _hiHeight [ h | h <- hs, _hiHeight h >= lo, _hiHeight h <= hi ]
  where
    cutEntry :: Value -> Parser (T.Text, Word64)
    cutEntry = withObject "cut" $ \o -> do
        m <- o .: "hashes"
        e <- withObject "hashes" (\x -> x .: K.fromText (toText cid)) m
        withObject "block hash with height" (\x -> (,) <$> x .: "hash" <*> x .: "height") e
    items :: Value -> Parser [HeaderInfo]
    items = withObject "page" $ \o -> o .: "items" >>= mapM (withObject "header" $ \x -> HeaderInfo
        <$> x .: "height"
        <*> x .: "hash"
        <*> x .: "payloadHash"
        <*> x .: "creationTime")

headerAt :: Word64 -> [HeaderInfo] -> IO HeaderInfo
headerAt h hs = case [ x | x <- hs, _hiHeight x == h ] of
    x : _ -> return x
    [] -> genError ("the oracle has no canonical header at height " <> tshow h)

-- | What a block offers to fill a request: accounts with the keys that sign
-- for them (the miner first), txids (the coinbase first), request keys and
-- pact ids.
--
data Facts = Facts
    { _fAccounts :: ![(T.Text, [T.Text])]
    , _fTxids :: ![Word64]
    , _fKeys :: ![T.Text]
    , _fPactIds :: ![T.Text]
    , _fCoinbaseKey :: !(Maybe T.Text)
    }

-- | Account names that are safe inside a Pact string literal.
--
safeAccount :: T.Text -> Bool
safeAccount a = not (T.null a) && T.length a <= 256
    && T.all (\c -> isAscii c && (isAlphaNum c || c `elem` ("-_:." :: String))) a

factsOf :: Value -> Facts
factsOf p = Facts
    { _fAccounts = nubBy (\a b -> fst a == fst b) $ filter (safeAccount . fst) $ maybeToList miner <> senders
    , _fTxids = mapMaybe (>>= intField "txId") (coinbase : map Just outs)
    , _fKeys = mapMaybe (>>= textField "hash") (map fst txs)
    , _fPactIds = mapMaybe (\o -> o >>= field "continuation" >>= textField "pactId") (map Just outs)
    , _fCoinbaseKey = coinbase >>= textField "reqKey"
    }
  where
    b64Json :: T.Text -> Maybe Value
    b64Json t = either (const Nothing) Just (decodeB64UrlNoPaddingText t) >>= decodeStrict
    get :: FromJSON a => T.Text -> Value -> Maybe a
    get k = parseMaybe (withObject "object" (.: K.fromText k))
    field = get :: T.Text -> Value -> Maybe Value
    textField = get :: T.Text -> Value -> Maybe T.Text
    intField = get :: T.Text -> Value -> Maybe Word64

    txs :: [(Maybe Value, Maybe Value)]
    txs = [ (b64Json t, b64Json o) | [t, o] <- fromMaybe [] (get "transactions" p :: Maybe [[T.Text]]) ]
    outs = mapMaybe snd txs
    coinbase = textField "coinbase" p >>= b64Json
    miner = do
        m <- textField "minerData" p >>= b64Json
        (,) <$> textField "account" m <*> pure (fromMaybe [] (get "public-keys" m))
    senders =
        [ (s, keys)
        | Just tx <- map fst txs
        , Just cmd <- [textField "cmd" tx >>= decodeStrict . T.encodeUtf8]
        , Just s <- [field "meta" cmd >>= textField "sender"]
        , let keys = mapMaybe (textField "pubKey") (fromMaybe [] (get "signers" cmd))
        ]

-- | The facts of the given blocks: one batch of payloads with outputs.
--
blockFacts :: ServiceApi -> ChainId -> [HeaderInfo] -> IO [(HeaderInfo, Facts)]
blockFacts api cid hs = do
    v <- oracleValue api methodPost (chainPrefix api cid <> "/payload/outputs/batch")
        [ (hAccept, "application/json"), (hContentType, "application/json") ]
        (Just (encodeStrict (object [ "heights" .= map _hiHeight hs, "hashes" .= map _hiPayload hs ])))
    ps <- maybe (genError "the oracle's payload batch does not decode") return (parseMaybe parseJSON v)
    let byHash = M.fromList [ (ph, p) | p <- ps, Just ph <- [parseMaybe (withObject "payload" (.: "payloadHash")) p] ]
    forM hs $ \h -> case M.lookup (_hiPayload h :: T.Text) byHash of
        Nothing -> genError ("the oracle did not return the payload " <> _hiPayload h)
        Just p -> return (h, factsOf p)

-- | The first block from the sampled height up that has transactions: in
-- 'txScanWindow' blocks, then in windows of 'txScanStep' blocks, up to
-- 'txScanMax' blocks in all or the oracle's tip ('canonicalHeaders' stops at
-- the height of the oracle's cut). Throws 'NoTxWithin' if
-- there is none (ruling C3 on Task 6), so a poll or SPV request always has a
-- request key of a real transaction.
--
firstWithTxs :: ServiceApi -> ChainId -> Word64 -> IO (Word64, Facts)
firstWithTxs api cid t = go t txScanWindow
  where
    end = t + txScanMax - 1
    go lo w = do
        let hi = min end (lo + w - 1)
        hs <- canonicalHeaders api cid lo hi
        when (lo == t) $ void (headerAt t hs)
        -- the heights from lo up without a gap, so that no block is missed
        unless (map _hiHeight hs == take (length hs) [lo ..]) $
            genError ("the oracle's branch from " <> tshow lo <> " to " <> tshow hi <> " has a gap")
        fs <- if null hs then return [] else blockFacts api cid hs
        case [ x | x@(_, f) <- fs, not (null (_fKeys f)) ] of
            (h, f) : _ -> return (_hiHeight h, f)
            []
                | hi < end && length hs == fromIntegral (hi - lo + 1) -> go (hi + 1) txScanStep
                | otherwise -> throwIO (NoTxWithin t (if null hs then lo - 1 else _hiHeight (last hs)))

-- -------------------------------------------------------------------------- --
-- Requests

-- | The request for a sampled target, with the default options.
--
requestsFor :: ServiceApi -> PactSet -> Word64 -> (RequestClass, ChainId, BlockHeight) -> IO Request
requestsFor = requestsForWith defaultGenOptions

-- | The request for a sampled target (class, chain, height):
--
-- * payload: by hash, with and without the height, the payload or its
--   outputs, single or a batch of up to 'maxBatchExtra' more blocks;
-- * header: by hash, or a branch query from the target's hash down to the
--   sampled height; with '_goSameSnapshot', also a plain range;
-- * local, history, preflight: a pinned @/local@ ('localCommandWith') of an
--   entry of the Pact set, filled from the block; history only at Pact 4
--   heights ('historyMaxHeight'), refused otherwise; the sender of a
--   preflight is a @k:@ account of the block (the miner or a transaction's
--   sender), @cw-compare@ if the block has none;
-- * poll: request keys of the first block with transactions plus one or two
--   unknown keys;
-- * spv: a request key of the first block with transactions, to a chain
--   that exists at that height;
-- * poll and spv: the first block with transactions is searched up to
--   'txScanMax' blocks from the sampled height up; without one, the target
--   is refused with 'NoTxWithin'.
--
-- The Pact set entries marked 'sameSnapshotOnly' are used only with
-- '_goSameSnapshot' ('generatedEntries').
--
-- Throws 'RequestGenError' if the oracle cannot give what the request needs,
-- and 'NoTxWithin' (poll and spv) if no block has a transaction.
--
requestsForWith
    :: GenOptions
    -> ServiceApi
    -> PactSet
    -> Word64
    -> (RequestClass, ChainId, BlockHeight)
    -> IO Request
requestsForWith opts api pset seed (cls, cid, BlockHeight t) = case cls of

    ClsPayload -> do
        let (variant, g1) = uniformR (0, 7 :: Int) g0
            (extra, _) = uniformR (1, maxBatchExtra) g1
            batch = variant >= 4
            withHeight = odd variant
            outputs = variant `elem` [2, 3, 6, 7]
            outputsPath = if outputs then "/outputs" else ""
        hs <- canonicalHeaders api cid t (if batch then t + extra else t)
        h0 <- headerAt t hs
        if not batch
        then return $ request (BlockHeight t) methodGet
            ("/payload/" <> _hiPayload h0 <> outputsPath <> (if withHeight then "?height=" <> tshow t else ""))
            Nothing
        else do
            let body
                    | withHeight = object [ "heights" .= map _hiHeight hs, "hashes" .= map _hiPayload hs ]
                    | otherwise = toJSON (map _hiPayload hs)
            return $ request (BlockHeight (maximum (map _hiHeight hs))) methodPost
                ("/payload" <> outputsPath <> "/batch") (Just (encodeStrict body))

    ClsHeader -> do
        let (variant, g1) = uniformR (0, if _goSameSnapshot opts then 2 else 1 :: Int) g0
            (k, _) = uniformR (0, maxRangeExtent) g1
        case variant of
            0 -> do
                h0 <- canonicalHeaders api cid t t >>= headerAt t
                return $ request (BlockHeight t) methodGet ("/header/" <> _hiHash h0) Nothing
            1 -> do
                hs <- canonicalHeaders api cid t (t + k)
                top <- case reverse hs of
                    x : _ -> return x
                    [] -> genError ("the oracle has no canonical header at height " <> tshow t)
                return $ request (BlockHeight (_hiHeight top)) methodPost
                    ("/header/branch?minheight=" <> tshow t <> "&maxheight=" <> tshow (_hiHeight top))
                    (Just (encodeStrict (object [ "lower" .= ([] :: [T.Text]), "upper" .= [_hiHash top] ])))
            _ -> return $ request (BlockHeight (t + k)) methodGet
                ("/header?minheight=" <> tshow t <> "&maxheight=" <> tshow (t + k)) Nothing

    ClsPoll -> do
        (hit, facts) <- firstWithTxs api cid t
        let (nKnown, g1) = uniformR (1, 3) g0
            (known, g2) = sampleN nKnown (_fKeys facts) g1
            (nUnknown, g3) = uniformR (1, 2 :: Int) g2
            unknown = take nUnknown (unfold g3)
        return $ request (BlockHeight hit) methodPost "/pact/api/v1/poll"
            (Just (encodeStrict (object [ "requestKeys" .= (known <> unknown) ])))

    ClsSpv -> do
        (hit, facts) <- firstWithTxs api cid t
        -- '_fKeys' is not empty: 'firstWithTxs' found a transaction
        let (key, g2) = pickOr "" (_fKeys facts) g0
            targets = [ c | c <- sort (HS.toList (chainIds v)), genesisBlockHeight v c <= BlockHeight hit ]
            (trg, _) = pickOr cid targets g2
        return $ request (BlockHeight hit) methodPost "/pact/spv"
            (Just (encodeStrict (object [ "requestKey" .= key, "targetChainId" .= toText trg ])))

    _ -> do
        when (cls == ClsHistory && maybe True (\m -> BlockHeight t > m) (historyMaxHeight v)) $
            genError ("no history request above the last Pact 4 height, at " <> tshow t)
        let entries = generatedEntries (_goSameSnapshot opts) pset cls
        firstEntry <- case entries of
            [] -> genError ("the Pact set has no entry for the class " <> requestClassText cls)
            e : _ -> return e
        h0 <- canonicalHeaders api cid t t >>= headerAt t
        facts <- blockFacts api cid [h0] >>= \case
            [(_, f)] -> return f
            _ -> genError "the oracle's payload batch has the wrong length"
        let (entry, g1) = pickOr firstEntry entries g0
            -- a preflight sender is a k: account: coin's enforce-reserved
            -- gives it a keyset guard, so buy-gas runs no user guard (at Pact
            -- 4 heights below 3,250,348 that guard runs on free gas)
            senders
                | cls == ClsPreflight = filter (("k:" `T.isPrefixOf`) . fst) (_fAccounts facts)
                | otherwise = _fAccounts facts
            ((account, keys), g2) = pickOr ("cw-compare", []) senders g1
            (txid, g3) = pickOr 0 (_fTxids facts) g2
            (pid, _) = pickOr "cw-compare-no-pact" (_fPactIds facts <> _fKeys facts <> maybeToList (_fCoinbaseKey facts)) g3
            code = T.replace "{account}" account
                . T.replace "{txid}" (tshow txid)
                . T.replace "{pactId}" pid
                $ _peCode entry
            payload = case _peKind entry of
                PactExec -> LocalExec code
                PactCont -> LocalCont code (_peStep entry)
            (meta, query)
                | cls == ClsPreflight =
                    ( LocalMeta account keys (_hiCreation h0 `div` 1_000_000 - preflightTimeSlack) localGasLimit
                    , "?preflight=true&signatureVerification=false&rewindDepth="
                    )
                | cls == ClsHistory = (defaultLocalMeta { _lmGasLimit = historyGasLimit }, "?rewindDepth=")
                | otherwise = (defaultLocalMeta, "?rewindDepth=")
        return $ (request (BlockHeight t) methodPost ("/pact/api/v1/local" <> query <> depthPlaceholder)
            (Just (localCommandWith v cid meta payload seed))) { _reqEntry = Just (_peName entry) }
  where
    v = _saVersion api
    g0 = targetGen seed cls cid t
    request h m path body = Request cls cid (Just h) m (chainPrefix api cid <> path) body Nothing
    unfold g = let (k, g') = unknownKey g in k : unfold g'

-- | The number of targets whose lookups run at the same time in
-- 'generateRequests'. The oracle's limiter bounds the rate either way.
--
generateWorkers :: Int
generateWorkers = 8

-- | A target dropped at generation, and why.
--
data GenSkip = GenSkip
    { _gsTarget :: !(RequestClass, ChainId, BlockHeight)
    , _gsKind :: !T.Text
        -- ^ 'skipNoTx' or 'skipLookupFailed'; the report counts skips per
        -- class and kind
    , _gsReason :: !T.Text
        -- ^ the details
    }
    deriving (Show, Eq)

-- | The kind of a 'GenSkip' of a poll or SPV target without a transaction
-- within 'txScanMax' blocks.
--
skipNoTx :: T.Text
skipNoTx = "no tx within 1000 blocks"

-- | The kind of a 'GenSkip' whose oracle lookups failed.
--
skipLookupFailed :: T.Text
skipLookupFailed = "an oracle lookup failed"

-- | 'requestsForWith' for every target, in the order of the targets. A
-- target whose lookups fail ('RequestGenError', or an HTTP failure after all
-- attempts) is dropped and returned as a 'GenSkip' (ruling on Task 6a; the
-- report counts them per kind, see 'reportGeneration'), and so is a poll or
-- SPV target without a transaction within 'txScanMax' blocks ('NoTxWithin').
--
generateRequestsSkipping
    :: GenOptions
    -> ServiceApi
    -> PactSet
    -> Word64
    -> [(RequestClass, ChainId, BlockHeight)]
    -> IO ([Request], [GenSkip])
generateRequestsSkipping opts api pset seed targets = do
    rs <- fmap concat $ mapM (mapConcurrently one) $ chunks targets
    return ([ r | Right r <- rs ], [ s | Left s <- rs ])
  where
    one target = try (try (requestsForWith opts api pset seed target)) >>= \case
        Right (Right r) -> return (Right r)
        Right (Left (RequestGenError e)) -> return (Left (GenSkip target skipLookupFailed e))
        Right (Left (NoTxWithin lo hi)) -> return (Left (GenSkip target skipNoTx
            ("no block with a transaction at heights " <> tshow lo <> " to " <> tshow hi)))
        Left (e :: HTTP.HttpException) -> return (Left (GenSkip target skipLookupFailed (tshow e)))
    chunks [] = []
    chunks xs = let (a, b) = splitAt generateWorkers xs in a : chunks b

-- | 'requestsForWith' for every target, in the order of the targets. The
-- first 'RequestGenError' (or HTTP failure after all attempts) is rethrown.
--
generateRequests
    :: GenOptions
    -> ServiceApi
    -> PactSet
    -> Word64
    -> [(RequestClass, ChainId, BlockHeight)]
    -> IO [Request]
generateRequests opts api pset seed =
    fmap concat . mapM (mapConcurrently (requestsForWith opts api pset seed)) . chunks
  where
    chunks [] = []
    chunks xs = let (a, b) = splitAt generateWorkers xs in a : chunks b
