{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.StorageTools.Compare.Rules
--
-- The comparison rules of @cw-compare@ (spec 10.1).
--
-- Never allowed to differ: payload bytes; headers; any pinned @/local@ result,
-- including gas, logs hash, events, error text and metadata; @keys@ contents
-- and order; @txlog@ contents and order; @txids@; @/poll@ results for covered
-- heights; "not found" versus found. Answers are compared as bytes: status
-- and body, nothing is parsed or normalised before the comparison.
--
-- What may differ, each narrow and with evidence:
--
-- * Tip skew: a pinned answer is compared only when both sides report the
--   same block height and parent hash, at the target height. Otherwise the
--   runner retries; a request that never pins is 'Skipped', never passed.
-- * Not covered: requests for a chain or height the test side does not hold
--   are 'Skipped' and counted, never passed.
-- * Orphan payloads ('AllowOrphanPayload'): found on the oracle, not found on
--   the test side, no canonical header of either side at the target height
--   references the payload hash, and the oracle holds a non-canonical header
--   at that height that does.
-- * @/spv@ target ('AllowSpvTarget'): the proofs differ in bytes but have the
--   same target chain, subject and root.
--
module Chainweb.StorageTools.Compare.Rules
( -- * Judging a pair of answers
  judge
, preJudge
, compareBytes

  -- * Pins
, answerPin

  -- * Orphan payloads
, payloadHashOfPath
, apiPrefixOfPath
, HeaderRef(..)
, orphanVerdict

  -- * SPV proofs
, compareSpvProofs
) where

import Control.Applicative ((<|>))
import Control.Monad ((>=>))

import Data.Aeson
import Data.Aeson.Types (Parser, parseEither, parseMaybe)
import qualified Data.ByteString as B
import Data.Maybe (isJust, isNothing)
import qualified Data.Text as T
import Data.MerkleLog (MerkleProof(..))

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.MerkleUniverse (ChainwebMerkleHashAlgorithm)
import Chainweb.Payload (BlockPayloadHash)
import Chainweb.SPV (TransactionOutputProof(..))
import Chainweb.SPV.VerifyProof (runTransactionOutputProof)
import Chainweb.StorageTools.Compare.Types
import Chainweb.Utils (decodeB64UrlNoPaddingText, fromText, toText)

sshow :: Show a => a -> T.Text
sshow = T.pack . show

-- -------------------------------------------------------------------------- --
-- Judging

-- | The verdict on a request that is decided before anything is sent: not
-- covered, or a pinned class without a target height or depth placeholder.
--
preJudge :: Coverage -> Request -> Maybe Verdict
preJudge cov r
    | not (isCovered cov (_reqChain r) (_reqTarget r)) = Just (Skipped NotCovered)
    | _reqClass r == ClsSpv
    , Just trg <- spvTargetChain r
    , not (isCovered cov trg ((+ spvTargetReach) <$> _reqTarget r)) = Just (Skipped NotCovered)
    | isPinnedClass (_reqClass r)
    , not (hasDepthPlaceholder r) || isNothing (_reqTarget r) || isNothing (apiPrefixOfPath (_reqPath r))
        = Just (Skipped Unpinned)
    | otherwise = Nothing

-- | An SPV proof to another chain ends in a header of the target chain at most
-- this many blocks above the source block: the diameter of the chain graphs of
-- mainnet and testnet (3). The test side must hold the target chain up to
-- there, or its proof cannot be the oracle's (Minor 5 of the Task 6 review).
--
spvTargetReach :: BlockHeight
spvTargetReach = 3

-- | The @targetChainId@ of an @/spv@ request body.
--
spvTargetChain :: Request -> Maybe ChainId
spvTargetChain r = _reqBody r >>= decodeStrict
    >>= parseMaybe (withObject "spv" (.: "targetChainId"))
    >>= \t -> fromText (t :: T.Text)

-- | Judges the answers of the oracle (first) and of the test side (second).
--
-- A pinned pair in which neither answer has block metadata (an error both
-- sides returned without it) is compared byte for byte: there is no
-- evidence of tip skew, so nothing is allowed to differ.
--
judge :: Coverage -> Request -> Answer -> Answer -> Judgement
judge cov r a b = case preJudge cov r of
    Just v -> Final v
    Nothing
        | isPinnedClass (_reqClass r) -> judgePinned r a b
        | otherwise -> case _reqClass r of
            ClsPayload -> judgePayload r a b
            ClsSpv -> judgeSpv a b
            _ -> Final (compareBytes a b)

-- | Pinned answers are compared only at the same block, at the target height.
-- If only one answer has block metadata, the answers differ: one side
-- executed the code and the other did not.
--
judgePinned :: Request -> Answer -> Answer -> Judgement
judgePinned r a b = case (_ansPin a, _ansPin b) of
    (Just pa, Just pb)
        | pa /= pb -> RetryPinMismatch
        | Just (_pinHeight pa) /= _reqTarget r -> RetryPinMismatch
        | otherwise -> Final (compareBytes a b)
    (Nothing, Nothing) -> Final (compareBytes a b)
    (Just pa, Nothing) -> Final $ Differ $
        "only the oracle answered with block metadata (height " <> sshow (_pinHeight pa) <> "); " <> bytesDetail a b
    (Nothing, Just pb) -> Final $ Differ $
        "only the test side answered with block metadata (height " <> sshow (_pinHeight pb) <> "); " <> bytesDetail a b

-- | A payload that the oracle has and the test side does not may be an
-- orphan. Only for a single payload by hash with a target height.
--
judgePayload :: Request -> Answer -> Answer -> Judgement
judgePayload r a b
    | _ansStatus a == 200
    , _ansStatus b == 404
    , isJust (_reqTarget r)
    , Just h <- payloadHashOfPath (_reqPath r) = CheckOrphan h
    | otherwise = Final (compareBytes a b)

judgeSpv :: Answer -> Answer -> Judgement
judgeSpv a b
    | _ansStatus a == 200
    , _ansStatus b == 200
    , _ansBody a /= _ansBody b = CompareSpvSubjects
    | otherwise = Final (compareBytes a b)

-- | Status and body, byte for byte.
--
compareBytes :: Answer -> Answer -> Verdict
compareBytes a b
    | _ansStatus a /= _ansStatus b || _ansBody a /= _ansBody b = Differ (bytesDetail a b)
    | otherwise = Same

bytesDetail :: Answer -> Answer -> T.Text
bytesDetail a b
    | _ansStatus a /= _ansStatus b =
        "status " <> sshow (_ansStatus a) <> " on the oracle, " <> sshow (_ansStatus b) <> " on the test side"
    | otherwise =
        "bodies differ from byte " <> sshow firstDiff
        <> " (oracle " <> sshow (B.length ba) <> " bytes, test side " <> sshow (B.length bb) <> " bytes)"
  where
    ba = _ansBody a
    bb = _ansBody b
    firstDiff = length $ takeWhile id $ B.zipWith (==) ba bb

-- -------------------------------------------------------------------------- --
-- Pins

-- | The block of a @/local@ answer: @blockHeight@ and @prevBlockHash@ of its
-- @metaData@, at the top level or under @preflightResult@.
--
answerPin :: B.ByteString -> Maybe Pin
answerPin = decodeStrict' >=> parseMaybe pinP
  where
    pinP :: Value -> Parser Pin
    pinP = withObject "local result" $ \o -> do
        cr <- (o .: "preflightResult") <|> pure (Object o)
        flip (withObject "command result") cr $ \c -> do
            md <- c .: "metaData"
            flip (withObject "metaData") md $ \m -> Pin
                <$> m .: "blockHeight"
                <*> m .: "prevBlockHash"

-- -------------------------------------------------------------------------- --
-- Orphan payloads

-- | The segments of the path, without the query.
--
pathSegments :: T.Text -> [T.Text]
pathSegments = filter (not . T.null) . T.splitOn "/" . T.takeWhile (/= '?')

-- | The payload hash of a request for a single payload by hash:
-- @...\/payload\/{hash}@ or @...\/payload\/{hash}\/outputs@. Batch requests
-- have none.
--
payloadHashOfPath :: T.Text -> Maybe BlockPayloadHash
payloadHashOfPath p = case dropWhile (/= "payload") (pathSegments p) of
    "payload" : h : rest | rest `elem` [[], ["outputs"]] -> fromText h
    _ -> Nothing

-- | The API prefix @\/chainweb\/0.0\/{version}@ of a request path.
--
apiPrefixOfPath :: T.Text -> Maybe T.Text
apiPrefixOfPath p = case pathSegments p of
    "chainweb" : api : v : _ -> Just ("/chainweb/" <> api <> "/" <> v)
    _ -> Nothing

-- | A block header by its hash and payload hash, as the header endpoints
-- return them.
--
data HeaderRef = HeaderRef
    { _hrHash :: !T.Text
    , _hrPayloadHash :: !T.Text
    }
    deriving (Show, Eq)

-- | The verdict on a payload that the oracle has and the test side lacks.
-- Allowed only if every lookup succeeded, neither canonical header at the
-- target height references the payload, and the oracle holds a
-- non-canonical header at that height that does (positive evidence of an
-- orphan). Anything else is a difference.
--
orphanVerdict
    :: ChainId
    -> BlockHeight
    -> BlockPayloadHash
    -> Either T.Text [HeaderRef]
        -- ^ every header of the oracle at the height, canonical or not
    -> Either T.Text HeaderRef
        -- ^ the canonical header of the oracle at the height
    -> Either T.Text HeaderRef
        -- ^ the canonical header of the test side at the height
    -> Verdict
orphanVerdict cid h ph eh eo et = case (eh, eo, et) of
    (_, Left e, _) -> Differ $ "found on the oracle only; the canonical header lookup failed on the oracle: " <> e
    (_, _, Left e) -> Differ $ "found on the oracle only; the canonical header lookup failed on the test side: " <> e
    (Left e, _, _) -> Differ $ "found on the oracle only; the lookup of the headers at height "
        <> sshow h <> " failed on the oracle: " <> e
    (Right hs, Right o, Right t)
        | _hrPayloadHash o == pht -> Differ $ referenced "oracle" o
        | _hrPayloadHash t == pht -> Differ $ referenced "test side" t
        | otherwise -> case [ x | x <- hs, _hrPayloadHash x == pht, _hrHash x /= _hrHash o, _hrHash x /= _hrHash t ] of
            [] -> Differ $ "found on the oracle only, but no non-canonical header of the oracle at height "
                <> sshow h <> " of chain " <> toText cid <> " carries payload " <> pht
            x : _ -> Allowed AllowOrphanPayload $
                "payload " <> pht <> " found on the oracle only; it is the payload of the non-canonical header "
                <> _hrHash x <> " of the oracle at height " <> sshow h <> " of chain " <> toText cid
                <> "; the canonical header there is " <> _hrHash o
                <> " (payload " <> _hrPayloadHash o <> ") on the oracle and " <> _hrHash t
                <> " (payload " <> _hrPayloadHash t <> ") on the test side"
  where
    pht = toText ph
    referenced side c =
        "found on the oracle only, but the canonical header " <> _hrHash c <> " at height "
        <> sshow h <> " of chain " <> toText cid <> " on the " <> side <> " references it"

-- -------------------------------------------------------------------------- --
-- SPV proofs

type OutputProof = TransactionOutputProof ChainwebMerkleHashAlgorithm

-- | A proof as the service API returns it: a JSON string with the base64url
-- encoding of the JSON proof, or the JSON proof itself.
--
decodeProof :: B.ByteString -> Either String OutputProof
decodeProof bs = eitherDecodeStrict' bs >>= \case
    String t -> either (Left . show) Right (decodeB64UrlNoPaddingText t) >>= eitherDecodeStrict'
    v -> parseEither parseJSON v

-- | Two proofs that differ in bytes. Allowed only if both decode and have the
-- same target chain, the same subject and the same root.
--
compareSpvProofs :: Answer -> Answer -> Verdict
compareSpvProofs a b = case (decodeProof (_ansBody a), decodeProof (_ansBody b)) of
    (Left e, _) -> Differ $ "the proof of the oracle does not decode: " <> T.pack e
    (_, Left e) -> Differ $ "the proof of the test side does not decode: " <> T.pack e
    (Right pa@(TransactionOutputProof ca ma), Right pb@(TransactionOutputProof cb mb))
        | ca /= cb -> Differ $
            "target chain " <> toText ca <> " on the oracle, " <> toText cb <> " on the test side"
        | _merkleProofSubject ma /= _merkleProofSubject mb -> Differ
            "the proofs have different subjects"
        | ra /= rb -> Differ $
            "the proofs have different roots: " <> toText ra <> " on the oracle, " <> toText rb <> " on the test side"
        | otherwise -> Allowed AllowSpvTarget $
            "the proofs differ in bytes but have the same target chain " <> toText ca
            <> ", the same subject and the same root " <> toText ra
      where
        ra = runTransactionOutputProof pa
        rb = runTransactionOutputProof pb
