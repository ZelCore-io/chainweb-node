{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.StorageTools.Compare.Types
--
-- The model of the differential harness @cw-compare@: requests, answers,
-- verdicts and coverage.
--
-- The harness sends the same request to the oracle (the production node) and
-- to the test side and compares the answers. Only the HTTP status and the body
-- of an answer are compared. Response headers (date, server timestamp, peer
-- address, connection handling) are never read, which is why 'Answer' has no
-- field for them (spec 10.1, "Transport"; ruling 4.4).
--
module Chainweb.StorageTools.Compare.Types
( -- * Requests
  RequestClass(..)
, allRequestClasses
, requestClassText
, requestClassFromText
, isPinnedClass
, isPactClass
, Request(..)
, depthPlaceholder
, hasDepthPlaceholder
, fillDepth

  -- * Answers
, Pin(..)
, Answer(..)

  -- * Verdicts
, AllowRule(..)
, allowRuleText
, SkipReason(..)
, skipReasonText
, Verdict(..)
, Judgement(..)

  -- * Coverage
, Coverage(..)
, isCovered
, classCoverage

  -- * Sides
, Side(..)
) where


import Data.Aeson
import qualified Data.ByteString as B
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word

import Network.HTTP.Types.Method (Method)

-- internal modules

import Chainweb.BlockHash (BlockHash)
import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Payload (BlockPayloadHash)
import Chainweb.StorageTools.Http (Limiter)
import Chainweb.Utils (decodeB64UrlNoPaddingText, encodeB64UrlNoPaddingText)

-- -------------------------------------------------------------------------- --
-- Requests

-- | The request classes of spec 10.1.
--
data RequestClass
    = ClsPayload
        -- ^ payloads and payloads with outputs by hash
    | ClsHeader
        -- ^ headers by hash, by height range, branch queries
    | ClsLocal
        -- ^ @/local@ at a height
    | ClsHistory
        -- ^ @txids@, @txlog@, @keylog@ through @/local@, Pact 4 heights only
    | ClsPoll
        -- ^ @/poll@ and lookups of request keys
    | ClsSpv
        -- ^ @/spv@ proofs
    | ClsPreflight
        -- ^ @/local@ with preflight
    deriving (Show, Eq, Ord, Enum, Bounded)

allRequestClasses :: [RequestClass]
allRequestClasses = [minBound .. maxBound]

requestClassText :: RequestClass -> T.Text
requestClassText = \case
    ClsPayload -> "payload"
    ClsHeader -> "header"
    ClsLocal -> "local"
    ClsHistory -> "history"
    ClsPoll -> "poll"
    ClsSpv -> "spv"
    ClsPreflight -> "preflight"

requestClassFromText :: T.Text -> Maybe RequestClass
requestClassFromText t = lookup t [ (requestClassText c, c) | c <- allRequestClasses ]

-- | The classes whose answers depend on the tip of the side that answers:
-- @/local@ requests, which address a height by a depth relative to the tip.
-- Their answers are compared only when both sides report the same block
-- height and parent hash in the metadata of the answer.
--
isPinnedClass :: RequestClass -> Bool
isPinnedClass = \case
    ClsLocal -> True
    ClsHistory -> True
    ClsPreflight -> True
    _ -> False

-- | The classes that a Pact service answers: @/local@ (plain, history and
-- preflight), @/poll@ and @/spv@. An in-process test side answers them on its
-- Pact chains only.
--
isPactClass :: RequestClass -> Bool
isPactClass = \case
    ClsPayload -> False
    ClsHeader -> False
    ClsLocal -> True
    ClsHistory -> True
    ClsPoll -> True
    ClsSpv -> True
    ClsPreflight -> True

-- | A request, sent unchanged to both sides, except for the depth placeholder.
--
-- The path of a request of a pinned class (see 'isPinnedClass') carries
-- 'depthPlaceholder', for instance
-- @\/chainweb\/0.0\/mainnet01\/chain\/0\/pact\/api\/v1\/local?rewindDepth={rewindDepth}@.
-- The harness replaces it per side with the depth of '_reqTarget' below the
-- tip of that side.
--
data Request = Request
    { _reqClass :: !RequestClass
    , _reqChain :: !ChainId
    , _reqTarget :: !(Maybe BlockHeight)
        -- ^ The height the request is about; it decides whether the request
        -- is covered by the test side. 'Nothing' only for requests that do not
        -- depend on a height, such as a lookup of an unknown request key.
    , _reqMethod :: !Method
    , _reqPath :: !T.Text
        -- ^ path and query, appended to the base URL of a side
    , _reqBody :: !(Maybe B.ByteString)
    , _reqEntry :: !(Maybe T.Text)
        -- ^ the name of the Pact set entry a @/local@ request (local,
        -- history, preflight) was generated from; the report counts the
        -- successes of each entry. Never sent.
    }
    deriving (Show, Eq)

-- | The placeholder for the rewind depth in the path of a pinned request.
--
depthPlaceholder :: T.Text
depthPlaceholder = "{rewindDepth}"

hasDepthPlaceholder :: Request -> Bool
hasDepthPlaceholder r = depthPlaceholder `T.isInfixOf` _reqPath r

-- | The path of the request with the depth placeholder replaced.
--
fillDepth :: Word64 -> Request -> T.Text
fillDepth d r = T.replace depthPlaceholder (T.pack (show d)) (_reqPath r)

-- | The JSON encoding of a request, one per line in a request file. The body
-- is in the field @body@ if it is valid UTF-8, and base64url encoded in the
-- field @bodyB64@ otherwise.
--
instance ToJSON Request where
    toJSON r = object $
        [ "class" .= requestClassText (_reqClass r)
        , "chain" .= _reqChain r
        , "target" .= _reqTarget r
        , "method" .= T.decodeLatin1 (_reqMethod r)
        , "path" .= _reqPath r
        ] <> (case _reqBody r of
            Nothing -> []
            Just b -> case T.decodeUtf8' b of
                Right t -> [ "body" .= t ]
                Left _ -> [ "bodyB64" .= encodeB64UrlNoPaddingText b ])
        <> [ "entry" .= e | Just e <- [_reqEntry r] ]

instance FromJSON Request where
    parseJSON = withObject "Request" $ \o -> Request
        <$> (o .: "class" >>= \t -> maybe (fail $ "unknown request class: " <> T.unpack t) return (requestClassFromText t))
        <*> o .: "chain"
        <*> o .:? "target"
        <*> (T.encodeUtf8 <$> o .: "method")
        <*> o .: "path"
        <*> body o
        <*> o .:? "entry"
      where
        body o = (fmap T.encodeUtf8 <$> o .:? "body") >>= \case
            Just b -> return (Just b)
            Nothing -> o .:? "bodyB64" >>= \case
                Nothing -> return Nothing
                Just t -> either (fail . show) (return . Just) (decodeB64UrlNoPaddingText t)

-- -------------------------------------------------------------------------- --
-- Answers

-- | The block an answer of a pinned request was computed at: the block height
-- and the parent hash from the metadata of the answer.
--
data Pin = Pin
    { _pinHeight :: !BlockHeight
    , _pinParent :: !BlockHash
    }
    deriving (Show, Eq)

-- | The answer of one side. Only status and body are compared; response
-- headers are not kept (ruling 4.4).
--
data Answer = Answer
    { _ansStatus :: !Int
    , _ansBody :: !B.ByteString
    , _ansPin :: !(Maybe Pin)
        -- ^ for pinned classes, parsed from the metadata in the body
    }
    deriving (Show, Eq)

-- -------------------------------------------------------------------------- --
-- Verdicts

-- | The allowlist of spec 10.1: the only differences that are allowed, each
-- with evidence. Tip skew and requests that are not covered are never
-- allowed; such requests are 'Skipped' (ruling 3.6).
--
data AllowRule
    = AllowOrphanPayload
        -- ^ the oracle has a payload that the test side lacks, and no
        -- canonical header of either side at the target height references it
    | AllowSpvTarget
        -- ^ the proofs differ in bytes, but prove the same subject on the same
        -- target chain up to the same root
    deriving (Show, Eq, Ord, Enum, Bounded)

allowRuleText :: AllowRule -> T.Text
allowRuleText = \case
    AllowOrphanPayload -> "orphan-payload"
    AllowSpvTarget -> "spv-target"

-- | Why a request was not compared. A skipped request never counts as passed.
--
data SkipReason
    = NotCovered
        -- ^ the test side does not hold the chain or height of the request;
        -- the request is sent to neither side
    | PinUnstable
        -- ^ the pins of the two answers did not agree after all retries
    | Unpinned
        -- ^ a request of a pinned class without a target height, depth
        -- placeholder or API prefix (never sent); or one that could not be
        -- pinned on the oracle: its tip probe gave no block metadata, or the
        -- target is above its tip
    | NoOracleAnswer
        -- ^ the oracle gave no HTTP answer to the request or to its tip
        -- probe after all attempts, whatever the test side did
    deriving (Show, Eq, Ord, Enum, Bounded)

skipReasonText :: SkipReason -> T.Text
skipReasonText = \case
    NotCovered -> "not-covered"
    PinUnstable -> "pin-unstable"
    Unpinned -> "unpinned"
    NoOracleAnswer -> "no-oracle-answer"

data Verdict
    = Same
    | Allowed !AllowRule !T.Text
        -- ^ a difference the allowlist allows, with evidence. An 'Allowed'
        -- verdict without evidence counts as 'Differ'.
    | Differ !T.Text
    | Skipped !SkipReason
    deriving (Show, Eq)

-- | The result of judging a pair of answers. Everything but 'Final' asks the
-- runner for more work (ruling 4.5).
--
data Judgement
    = Final !Verdict
    | RetryPinMismatch
        -- ^ the answers were computed at different blocks, or not at the
        -- target height: probe the tips again and resend
    | CheckOrphan !BlockPayloadHash
        -- ^ the oracle has the payload and the test side does not: look up the
        -- canonical headers at the target height on both sides
    | CompareSpvSubjects
        -- ^ the proofs differ in bytes: compare target chain, subject and root
    deriving (Show, Eq)

-- -------------------------------------------------------------------------- --
-- Coverage

-- | The chains and inclusive height ranges that the test side holds.
--
newtype Coverage = Coverage (M.Map ChainId [(BlockHeight, BlockHeight)])
    deriving (Show, Eq)

-- | Whether the test side holds the chain and height. A request without a
-- target height is covered if its chain is.
--
isCovered :: Coverage -> ChainId -> Maybe BlockHeight -> Bool
isCovered (Coverage m) cid target = case M.lookup cid m of
    Nothing -> False
    Just ranges -> case target of
        Nothing -> not (null ranges)
        Just h -> any (\(lo, hi) -> lo <= h && h <= hi) ranges

-- | The coverage of a request class. With @Just@ the Pact chains of the test
-- side, a class of 'isPactClass' is covered on those chains only; every other
-- class, and every class with 'Nothing', has the whole coverage.
--
classCoverage :: Maybe [ChainId] -> Coverage -> RequestClass -> Coverage
classCoverage Nothing cov _ = cov
classCoverage (Just pactChains) cov@(Coverage m) cls
    | isPactClass cls = Coverage (M.filterWithKey (\c _ -> c `elem` pactChains) m)
    | otherwise = cov

-- -------------------------------------------------------------------------- --
-- Sides

-- | One side of a comparison: a service API reachable over HTTP.
--
data Side = Side
    { _sideName :: !T.Text
    , _sideBaseUrl :: !T.Text
        -- ^ for instance @http://127.0.0.1:1848@
    , _sideLimiter :: !Limiter
        -- ^ the limiter of the host of the base URL, from
        -- 'Chainweb.StorageTools.Compare.Run.limiterFor', shared by every
        -- caller of that host
    }
