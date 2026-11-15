{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.StorageTools.Compare.Rules
--
-- The comparison rules of @cw-compare@ (spec 10.1).
--
-- Never allowed to differ: payload bytes; headers; any pinned @/local@ result,
-- including gas, logs hash, events, error text and metadata; @keys@ contents
-- and order; @txlog@ contents (but not the order of its rows, see below);
-- @txids@; @/poll@ results for covered
-- heights; "not found" versus found. Answers are compared as bytes: status
-- and body, nothing is parsed or normalised before the comparison.
--
-- What may differ, each narrow and with evidence:
--
-- * Tip skew: a pinned answer is compared only when both sides report the
--   same block height and parent hash, at the target height. Otherwise the
--   runner retries; a request that never pins is 'Skipped', never passed.
-- * @txlog@ row order ('OrderOnly'): the rows are the same, byte for byte, as a
--   multiset, in another order. @txlog@ is a @SELECT@ without an @ORDER BY@
--   over rows that stock nodes commit in @HashMap@ order, so the order is not
--   a property of the data (ruling of M0c). Never 'Same', never 'Differ'; any
--   row content or count difference is 'Differ'.
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
, orderOnlyRows

  -- * Request files
, allowedRequest
, parseRequestFile

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
import Control.Monad (guard, (>=>))

import Data.Aeson
import Data.Aeson.Types (Parser, parseEither, parseMaybe)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import Data.Char (isAsciiLower, isAsciiUpper, isDigit)
import qualified Data.List as L
import Data.Maybe (isJust, isNothing)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.MerkleLog (MerkleProof(..))

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.MerkleUniverse (ChainwebMerkleHashAlgorithm)
import Chainweb.Payload (BlockPayloadHash)
import Chainweb.SPV (TransactionOutputProof(..))
import Chainweb.SPV.VerifyProof (runTransactionOutputProof)
import Chainweb.StorageTools.Compare.PactSet (codeProblems)
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

-- -------------------------------------------------------------------------- --
-- Request files

-- | Whether a request of a request file (@--requests FILE@) may be sent: the
-- harness is read-only towards production, so a request file holds only the
-- read requests of the 7 classes, as the generator makes them. Takes the API
-- prefix @\/chainweb\/0.0\/{version}@ of the run.
--
-- * header: @GET \/cut@, @GET \/chain\/{c}\/header@ (a height range),
--   @GET \/chain\/{c}\/header\/{hash}@, and the branch query
--   @POST \/chain\/{c}\/header\/branch@ (a read; the chainweb API takes
--   its bounds in a body);
-- * payload: @GET \/chain\/{c}\/payload\/{hash}@ and its @\/outputs@, the
--   read-only batches @POST \/chain\/{c}\/payload\/batch@ and
--   @POST \/chain\/{c}\/payload\/outputs\/batch@;
-- * local, history, preflight: @POST \/chain\/{c}\/pact\/api\/v1\/local@,
--   which never commits; @preflight=true@ for preflight only, and the rewind
--   depth always the placeholder @{rewindDepth}@ with a target height, so
--   that the request is pinned (Minor 5 of the P0 review);
-- * poll: @POST \/chain\/{c}\/pact\/api\/v1\/poll@;
-- * spv: @POST \/chain\/{c}\/pact\/spv@.
--
-- Nothing else is allowed: no @\/send@, no @\/listen@ (it blocks), no
-- @PUT \/cut@, no other method. The chain in the path is the chain of the
-- request, every query parameter is known, a GET has no body and a POST has
-- one.
--
allowedRequest :: T.Text -> Request -> Either T.Text ()
allowedRequest prefix r = do
    rest <- maybe (Left ("the path does not start with " <> prefix <> "/")) Right
        (T.stripPrefix (prefix <> "/") path)
    let segs = T.splitOn "/" rest
    unless' (all goodSegment segs) "a path segment is empty, a dot segment, or has a character other than A-Z a-z 0-9 - _"
    params <- mapM param (if T.null query then [] else T.splitOn "&" query)
    (method, classes, keys) <- endpoint segs
    unless' (_reqMethod r == method) ("the method of this path is " <> T.pack (B8.unpack method))
    unless' (cls `elem` classes) ("this path is not of the class " <> requestClassText cls)
    unless' (all ((`elem` keys) . fst) params) ("query parameters other than " <> T.intercalate ", " keys)
    unless' (length params == length (dedup (map fst params))) "a query parameter twice"
    case method of
        "GET" -> unless' (isNothing (_reqBody r)) "a GET with a body"
        _ -> case _reqBody r of
            Nothing -> Left "a POST without a body"
            Just body
                | isLocalPath segs -> localCommandProblem (_reqClass r) body
                | otherwise -> Right ()
    mapM_ (checkValue params) keys
  where
    cls = _reqClass r
    (path, query) = T.drop 1 <$> T.breakOn "?" (_reqPath r)
    chain = toText (_reqChain r)

    unless' ok e = if ok then Right () else Left e
    dedup = foldr (\x xs -> if x `elem` xs then xs else x : xs) []

    goodSegment t = not (T.null t) && t /= "." && t /= ".."
        && T.all (\c -> isAsciiLower c || isAsciiUpper c || isDigit c || c == '-' || c == '_') t

    param t = case T.splitOn "=" t of
        [k, v] -> Right (k, v)
        _ -> Left ("a query parameter that is not key=value: " <> t)

    isLocalPath = \case
        ("chain" : _ : xs) -> xs == ["pact", "api", "v1", "local"]
        _ -> False

    pinned = [ClsLocal, ClsHistory, ClsPreflight]
    heights = ["minheight", "maxheight", "limit"]

    endpoint = \case
        ["cut"] -> Right ("GET", [ClsHeader], ["maxheight"])
        ("chain" : c : xs)
            | c /= chain -> Left ("the chain in the path is not the chain " <> chain <> " of the request")
            | otherwise -> chainEndpoint xs
        _ -> Left "not a read endpoint of the 7 classes"

    chainEndpoint = \case
        ["header"] -> Right ("GET", [ClsHeader], heights)
        ["header", "branch"] -> Right ("POST", [ClsHeader], heights)
        ["header", _] -> Right ("GET", [ClsHeader], [])
        ["payload", "batch"] -> Right ("POST", [ClsPayload], [])
        ["payload", "outputs", "batch"] -> Right ("POST", [ClsPayload], [])
        ["payload", h] | h /= "outputs" -> Right ("GET", [ClsPayload], ["height"])
        ["payload", h, "outputs"] | h /= "batch" -> Right ("GET", [ClsPayload], ["height"])
        ["pact", "api", "v1", "local"] -> Right ("POST", pinned, ["rewindDepth", "preflight", "signatureVerification"])
        ["pact", "api", "v1", "poll"] -> Right ("POST", [ClsPoll], [])
        ["pact", "spv"] -> Right ("POST", [ClsSpv], [])
        _ -> Left "not a read endpoint of the 7 classes"

    checkValue params k = case (k, lookup k params) of
        ("rewindDepth", v)
            | cls `elem` pinned && v /= Just depthPlaceholder ->
                Left ("a pinned request needs rewindDepth=" <> depthPlaceholder)
        ("preflight", v)
            | cls == ClsPreflight && v /= Just "true" -> Left "a preflight request needs preflight=true"
            | cls /= ClsPreflight && isJust v -> Left "preflight=... in a request that is not of the class preflight"
        ("signatureVerification", Just v)
            | v `notElem` ["true", "false"] -> Left "signatureVerification is true or false"
        (_, Just v)
            | k `elem` ("height" : heights), T.null v || not (T.all isDigit v) -> Left (k <> " is not a number")
        _
            | k == "rewindDepth", cls `elem` pinned, isNothing (_reqTarget r) ->
                Left "a pinned request needs a target height"
            | otherwise -> Right ()

-- | A @/local@ body is a command, which runs Pact code on the oracle. Like a
-- generated one it must run at most what the generator sends (N1 of the P0-B
-- re-review): a @cont@ at step 0 only (the step check fails before any code
-- of the defpact runs), and not a rollback (@rollback@ absent or @false@, as
-- the generator writes it; a rollback would run the step's rollback code);
-- exec code that 'codeProblems' accepts, as for a live
-- entry of a Pact set; a @gasLimit@ at most the generator's, 'historyGasLimit'
-- for History and 'localGasLimit' otherwise. A body that is not a command
-- with one of the two payloads is refused too.
--
localCommandProblem :: RequestClass -> B.ByteString -> Either T.Text ()
localCommandProblem cls body = do
    (code, step, gas) <- either (const (Left "the body is not a command with an exec or cont payload and a gasLimit")) Right $
        parseEither parseCommand =<< maybe (Left "cmd") Right (decodeStrict body)
    unless' (isJust code || isJust step) "the body is not a command with an exec or cont payload and a gasLimit"
    unless' (maybe True ((== 0) . fst) step) "a cont payload at a step other than 0"
    unless' (maybe True ((/= Just True) . snd) step) "a cont payload with rollback: true"
    case concatMap codeProblems code of
        [] -> Right ()
        ps -> Left (T.intercalate "; " ps)
    unless' (gas <= fromIntegral cap) ("a gasLimit of " <> T.pack (show gas) <> ", above " <> T.pack (show cap))
  where
    cap = if cls == ClsHistory then historyGasLimit else localGasLimit
    unless' ok e = if ok then Right () else Left e

    parseCommand :: Value -> Parser (Maybe T.Text, Maybe (Integer, Maybe Bool), Integer)
    parseCommand = withObject "command" $ \c -> do
        cmd <- c .: "cmd"
        o <- either fail return (eitherDecodeStrict' (T.encodeUtf8 cmd))
        flip (withObject "payload") o $ \po -> do
            pl <- po .: "payload"
            (code, step) <- flip (withObject "payload") pl $ \x -> do
                ex <- x .:? "exec"
                ct <- x .:? "cont"
                (,) <$> traverse (withObject "exec" (.: "code")) ex
                    <*> traverse (withObject "cont" $ \co -> (,) <$> co .: "step" <*> co .:? "rollback") ct
            gas <- po .: "meta" >>= withObject "meta" (.: "gasLimit")
            return (code, step, gas)

-- | The requests of a request file: one JSON request per line, blank lines
-- skipped. Fails at the first line that does not decode, or whose request
-- 'allowedRequest' refuses, with its line number. Takes the API prefix of the
-- run.
--
parseRequestFile :: T.Text -> B.ByteString -> Either String [Request]
parseRequestFile prefix bs = fmap concat $ mapM line $ zip [1 :: Int ..] (B8.lines bs)
  where
    line (i, l)
        | B8.all (`elem` (" \t\r" :: String)) l = Right []
        | otherwise = case eitherDecodeStrict' l of
            Left e -> Left (show i <> ": " <> e)
            Right r -> case allowedRequest prefix r of
                Left e -> Left (show i <> ": not allowed: " <> T.unpack e <> ": " <> B8.unpack (_reqMethod r) <> " " <> T.unpack (_reqPath r))
                Right () -> Right [r]

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
        | otherwise -> Final (compareFor r a b)
    (Nothing, Nothing) -> Final (compareFor r a b)
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

-- | The verdict on two answers of one request: 'compareBytes', except that the
-- rows of a @txlog@ may come in another order (see 'orderOnlyRows').
--
compareFor :: Request -> Answer -> Answer -> Verdict
compareFor r a b = case compareBytes a b of
    Differ d
        | Just (table, txid) <- txlogOfRequest r
        , _ansStatus a == 200
        , _ansStatus b == 200
        , Just n <- orderOnlyRows (_ansBody a) (_ansBody b) ->
            OrderOnly $ "txlog " <> table <> " " <> txid <> ": " <> sshow n <> " rows in another order"
        | otherwise -> Differ d
    v -> v

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
-- txlog rows

-- | The bytes of two History answers of one @txlog@, compared by 'compareFor'
-- when they differ.
--
-- A @txlog@ is a @SELECT@ without an @ORDER BY@, and Pact 4's commit writes
-- a block's rows in the order of a @HashMap@, so two stock nodes can return
-- the rows of one block in different orders (ruling of M0c). The rows are
-- compared as a multiset of byte-equal rows: the answers must be equal
-- byte for byte outside the array @result.data@, and the rows in it must be
-- the same bytes the same number of times, in another order. The rows are
-- cut out of the bytes of the answer, not parsed and written again, so a row
-- that differs in a key order or a number format is a different row.
--
-- Returns the number of rows, or 'Nothing' if the answers are not that: not
-- both a JSON object with an array at @result.data@, the array not written
-- as the rows joined by commas, the rest of the answers different, or the
-- rows in the same order (then the bodies differ elsewhere).
--
orderOnlyRows :: B.ByteString -> B.ByteString -> Maybe Int
orderOnlyRows a b = do
    (pa, ra, sa) <- splitRows a
    (pb, rb, sb) <- splitRows b
    guard (pa == pb && sa == sb)
    guard (ra /= rb && L.sort ra == L.sort rb)
    return (length ra)

-- | An answer cut into the bytes before @result.data@'s array, its rows and
-- the bytes after it. 'Nothing' if there is no such array or it is not
-- written as @[row,row,...]@ without any other byte.
--
splitRows :: B.ByteString -> Maybe (B.ByteString, [B.ByteString], B.ByteString)
splitRows body = do
    (s, e) <- memberSpan ["result", "data"] body
    guard (B.index body s == 0x5b && B.index body (e - 1) == 0x5d)
    let inner = B.take (e - s - 2) (B.drop (s + 1) body)
    rows <- if B.null inner then Just [] else elements inner
    guard (B.intercalate "," rows == inner)
    return (B.take s body, rows, B.drop e body)

-- | The elements of the inside of a JSON array, each as the bytes between
-- the commas at depth 0.
--
elements :: B.ByteString -> Maybe [B.ByteString]
elements inner = go 0
  where
    n = B.length inner
    go i
        | i >= n = Just []
        | otherwise = do
            e <- skipValue inner i
            let row = B.take (e - i) (B.drop i inner)
            if e == n
                then Just [row]
                else do
                    guard (B.index inner e == 0x2c)
                    (row :) <$> go (e + 1)

-- | The byte range @[start, end)@ of the value at a path of object keys,
-- the first member of that name at each level. A key written with an escape
-- never matches.
--
memberSpan :: [B.ByteString] -> B.ByteString -> Maybe (Int, Int)
memberSpan path s = go path 0
  where
    go [] i = (,) i <$> skipValue s i
    go (k : ks) i = do
        guard (at i == Just 0x7b)
        members k ks (skipWs (i + 1))
    members k ks i = do
        guard (at i == Just 0x22)
        ke <- skipString s i
        let key = B.take (ke - i - 2) (B.drop (i + 1) s)
            c = skipWs ke
        guard (at c == Just 0x3a)
        let v = skipWs (c + 1)
        ve <- skipValue s v
        if key == k
            then go ks v
            else do
                let d = skipWs ve
                guard (at d == Just 0x2c)
                members k ks (skipWs (d + 1))
    at i = fst <$> B.uncons (B.drop i s)
    skipWs i = i + B.length (B.takeWhile (`B.elem` " \t\r\n") (B.drop i s))

-- | The end of the string that starts at a quote.
--
skipString :: B.ByteString -> Int -> Maybe Int
skipString s i0 = go (i0 + 1)
  where
    go i = case B.indexMaybe s i of
        Nothing -> Nothing
        Just 0x22 -> Just (i + 1)
        Just 0x5c -> go (i + 2)
        Just _ -> go (i + 1)

-- | The end of the JSON value that starts at an index: a string, an object or
-- an array (to the bracket that closes it), or a literal or number (to the
-- next comma, bracket or white space).
--
skipValue :: B.ByteString -> Int -> Maybe Int
skipValue s i = case B.indexMaybe s i of
    Nothing -> Nothing
    Just 0x22 -> skipString s i
    Just c
        | c == 0x7b || c == 0x5b -> nested (1 :: Int) (i + 1)
        | c == 0x7d || c == 0x5d || c == 0x2c -> Nothing
        | otherwise -> Just (i + B.length (B.takeWhile (not . (`B.elem` ",]} \t\r\n")) (B.drop i s)))
  where
    nested 0 j = Just j
    nested d j = case B.indexMaybe s j of
        Nothing -> Nothing
        Just 0x22 -> skipString s j >>= nested d
        Just c
            | c == 0x7b || c == 0x5b -> nested (d + 1) (j + 1)
            | c == 0x7d || c == 0x5d -> nested (d - 1) (j + 1)
            | otherwise -> nested d (j + 1)

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
