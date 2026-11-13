{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.StorageTools.Test.Requests
--
-- Tests of the @cw-compare@ request generator against a stub oracle on
-- 127.0.0.1, of the fixed @/local@ command, of coverage from each chain's
-- genesis, of the SPV rule with a real proof, and of the report additions of
-- Task 6 (Pact failures per class, the classes of a generated run).
--
module Chainweb.StorageTools.Test.Requests
( tests
) where

import Control.Exception (SomeException, try)
import Control.Monad
import Data.Aeson
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import Data.Aeson.Types (parseMaybe)
import Data.Bits (xor)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import Data.IORef
import Data.List (isInfixOf, nub)
import qualified Data.List.NonEmpty as NE
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe, mapMaybe)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Data.Word (Word64)
import Network.HTTP.Types
import Network.Wai (Application, pathInfo, queryString, responseLBS, strictRequestBody)
import Network.Wai.Handler.Warp (testWithApplication)
import Test.Tasty
import Test.Tasty.HUnit
import qualified Options.Applicative as O
import System.Exit (ExitCode(..))
import Text.Read (readMaybe)

import Data.MerkleLog (MerkleProof(..), decodeMerkleProofObject, encodeMerkleProofObject, merkleProof_)

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Crypto.MerkleLog (bodyTree, headerTree_)
import Chainweb.MerkleUniverse (ChainwebHashTag, ChainwebMerkleHashAlgorithm)
import Chainweb.Payload
import Chainweb.SPV (TransactionOutputProof(..))
import Chainweb.StorageTools.Compare.InProcess (LocalSettings(..), localSettingsParser)
import Chainweb.StorageTools.Compare.PactSet
import Chainweb.StorageTools.Compare.Report
import Chainweb.StorageTools.Compare.Requests
import Chainweb.StorageTools.Compare.Rules
import Chainweb.StorageTools.Compare.Run
import Chainweb.StorageTools.Compare.Sample
import Chainweb.StorageTools.Compare.Types
import Chainweb.StorageTools.Http (ServiceApi, limiterPermits)
import Chainweb.Utils (encodeB64UrlNoPaddingText, decodeB64UrlNoPaddingText)
import Chainweb.Version.Mainnet (mainnet)

tests :: TestTree
tests = testGroup "Requests"
    [ testCase "localCommand for one fixed (chain, code, seed): bytes equal a literal golden; creation time and nonce are the fixed values" localGolden
    , testCase "the default Pact set decodes and serves local, history and preflight" pactSetDecodes
    , testCase "requestsFor is deterministic per seed and target; seed 1 and seed 2 differ; none is decided before sending" deterministic
    , testCase "every oracle lookup takes a permit of the oracle host's limiter, shared with the oracle side" oracleLimiter
    , testCase "header: branch queries for ranges; a plain range only with --same-snapshot" headerRequests
    , testCase "payload: by hash with and without height, single and batch" payloadRequests
    , testCase "local, preflight and history: pinned /local with a localCommand body filled from the block" pinnedRequests
    , testCase "history above the last Pact 4 height is refused before any lookup" historyRefused
    , testCase "poll: keys from a covered payload plus unknown keys; spv: a sampled transaction" pollAndSpv
    , testCase "coverage starts at each chain's genesis: chains 10-19 from 852,054" genesisCoverageTest
    , testCase "spv with a real proof: identical proofs Same, a tampered path element Differ" spvRealProof
    , testCase "report: per class, the 200 answers whose Pact result status is failure" pactFailures
    , testCase "a class in --classes with nothing compared voids the run" expectedClasses
    , testCase "History against a live oracle (defaults) is txlog only over 1000 seeds; txids and keylog only with --same-snapshot" liveHistory
    , testCase "keys against a live oracle only on coin.allocation-table; keys on a large table only with --same-snapshot" liveKeys
    , testCase "the default Pact set: every live entry records its bound; unbounded entries are same-snapshot only; defpact state at step 0" pactSetBounds
    , testCase "poll and spv scan up to 1000 blocks for a transaction: its block is the target, every request has a real key" sparseTxs
    , testCase "poll and spv without a transaction within 1000 blocks are skipped at generation as no tx within 1000 blocks" noTxSkip
    , testCase "report: poll and spv count compared requests with a real key; a run of them with none is Void" realKeys
    , testCase "a run whose classes are not all 7 is at best PartialPass (exit 4)" partialPass
    , testCase "generation skips are reported per kind" skipKinds
    , testCase "spv: a target chain outside the coverage at the target height + 3 is NotCovered" spvTargetCoverage
    , testCase "--allow-reads-in-local takes a required true|false" allowReadsOption
    ]

cid0, cid3, cid10 :: ChainId
cid0 = unsafeChainId 0
cid3 = unsafeChainId 3
cid10 = unsafeChainId 10

tshow :: Show a => a -> T.Text
tshow = T.pack . show

-- -------------------------------------------------------------------------- --
-- localCommand

goldenCommand :: B.ByteString
goldenCommand =
    "{\"cmd\":\"{\\\"meta\\\":{\\\"chainId\\\":\\\"3\\\",\\\"creationTime\\\":1600000000,\\\"gasLimit\\\":100000,\\\"gasPrice\\\":1.0e-8,\\\"sender\\\":\\\"cw-compare\\\",\\\"ttl\\\":86400},\\\"networkId\\\":\\\"mainnet01\\\",\\\"nonce\\\":\\\"cw-compare 7\\\",\\\"payload\\\":{\\\"exec\\\":{\\\"code\\\":\\\"(coin.details \\\\\\\"k:example\\\\\\\")\\\",\\\"data\\\":{}}},\\\"signers\\\":[]}\"\
    \,\"hash\":\"3wpB-n7-oT60e9bcn-0bdVFU91E8AmOg_8k9rwjOA0s\",\"sigs\":[]}"

localGolden :: Assertion
localGolden = do
    let bytes = localCommand mainnet cid3 "(coin.details \"k:example\")" 7
    bytes @?= goldenCommand
    -- built once: the same arguments give the same bytes
    localCommand mainnet cid3 "(coin.details \"k:example\")" 7 @?= bytes
    assertBool "another seed gives another nonce" (localCommand mainnet cid3 "(coin.details \"k:example\")" 8 /= bytes)
    p <- either assertFailure return $ commandPayload bytes
    field ["meta", "creationTime"] p @?= Just (Number 1_600_000_000)
    field ["meta", "creationTime"] p @?= Just (Number (fromIntegral localCreationTime))
    field ["nonce"] p @?= Just (String "cw-compare 7")
    field ["networkId"] p @?= Just (String "mainnet01")
    field ["meta", "chainId"] p @?= Just (String "3")
    field ["payload", "exec", "code"] p @?= Just (String "(coin.details \"k:example\")")

-- | The payload of a command: the JSON in its @cmd@ string.
--
commandPayload :: B.ByteString -> Either String Value
commandPayload b = do
    c <- eitherDecodeStrict' b
    t <- maybe (Left "no cmd") Right $ parseMaybe (withObject "command" (.: "cmd")) c
    eitherDecodeStrict' (T.encodeUtf8 t)

field :: [T.Text] -> Value -> Maybe Value
field [] v = Just v
field (k : ks) (Object o) = KM.lookup (K.fromText k) o >>= field ks
field _ _ = Nothing

pactSetDecodes :: Assertion
pactSetDecodes = do
    forM_ [ClsLocal, ClsHistory, ClsPreflight] $ \c ->
        assertBool (show c <> " has entries") (not (null (entriesFor defaultPactSet c)))
    forM_ (entriesFor defaultPactSet ClsHistory) $ \e ->
        assertBool (T.unpack (_peName e) <> " is a history function")
            (any (`T.isPrefixOf` _peCode e) ["(txids ", "(txlog ", "(keylog "])
    assertBool "one entry reads defpact state" (any ((== PactCont) . _peKind) (_pactEntries defaultPactSet))
    assertBool "a bad class is refused" $ either (const True) (const False) $
        decodePactSet "entries:\n  - name: x\n    classes: [payload]\n    code: '(+ 1 1)'\n"

-- -------------------------------------------------------------------------- --
-- Stub oracle
--
-- Tip 1000 on every chain. The canonical header at height h has the hash
-- @H<h>@, the payload @P<h>@ and the creation time 1,600,000,000 + 30 h
-- seconds. Blocks at heights divisible by 3 hold two transactions; the second
-- one leaves a pact.

stubTip :: Word64
stubTip = 1_000

hashAt, payloadAt :: Word64 -> T.Text
hashAt h = "H" <> tshow h
payloadAt h = "P" <> tshow h

creationSecondsAt :: Word64 -> Integer
creationSecondsAt h = 1_600_000_000 + 30 * fromIntegral h

hasTxs :: Word64 -> Bool
hasTxs h = h `mod` 3 == 0

rk :: Word64 -> Int -> T.Text
rk h i = "rk" <> tshow h <> "-" <> tshow i

b64 :: Value -> T.Text
b64 = encodeB64UrlNoPaddingText . BL.toStrict . encode

payloadObjWith :: (Word64 -> Bool) -> Word64 -> Value
payloadObjWith txsAt h = object
    [ "transactions" .= [ [b64 (txJson i), b64 (outJson i)] | txsAt h, i <- [0, 1] ]
    , "minerData" .= b64 (object
        [ "account" .= ("k:miner" :: T.Text), "predicate" .= ("keys-all" :: T.Text), "public-keys" .= ["miner-key" :: T.Text] ])
    , "coinbase" .= b64 (object [ "reqKey" .= ("cb" <> tshow h), "txId" .= (10 * h), "continuation" .= Null ])
    , "payloadHash" .= payloadAt h
    , "transactionsHash" .= ("T" <> tshow h)
    , "outputsHash" .= ("O" <> tshow h)
    ]
  where
    txJson i = object
        [ "hash" .= rk h i
        , "sigs" .= ([] :: [Value])
        , "cmd" .= T.decodeUtf8 (BL.toStrict (encode (object
            [ "meta" .= object [ "sender" .= ("k:sender" <> tshow h) ]
            , "signers" .= [ object [ "pubKey" .= ("sender-key" :: T.Text) ] ]
            ])))
        ]
    outJson i = object
        [ "reqKey" .= rk h i
        , "txId" .= (10 * h + fromIntegral i + 1)
        , "continuation" .= if i == 1 then object [ "pactId" .= ("pact" <> tshow h) ] else Null
        ]

headerObj :: Word64 -> Value
headerObj h = object
    [ "height" .= h
    , "hash" .= hashAt h
    , "payloadHash" .= payloadAt h
    , "creationTime" .= (1_000_000 * creationSecondsAt h)
    , "chainId" .= (0 :: Int)
    ]

oracleAppWith :: (Word64 -> Bool) -> IORef Int -> Application
oracleAppWith txsAt arrivals req respond = do
    atomicModifyIORef' arrivals (\n -> (n + 1, ()))
    body <- BL.toStrict <$> strictRequestBody req
    let q k = lookup k (queryString req) >>= id >>= readMaybe . B8.unpack
        json v = respond $ responseLBS status200 [(hContentType, "application/json")] (encode v)
    case drop 3 (pathInfo req) of
        ["cut"] -> json $ object
            [ "hashes" .= object
                [ K.fromText (tshow c) .= object [ "hash" .= hashAt stubTip, "height" .= stubTip ] | c <- [0 .. 19 :: Int] ]
            , "height" .= (20 * stubTip)
            ]
        ["chain", _, "header", "branch"]
            | Just lo <- q "minheight", Just hi <- q "maxheight" ->
                let items = [ headerObj h | h <- reverse [lo .. min hi stubTip] ]
                in json $ object [ "items" .= items, "limit" .= length items, "next" .= Null ]
        ["chain", _, "payload", "outputs", "batch"]
            | Just hs <- decodeStrict body >>= parseMaybe (withObject "batch" (.: "heights")) ->
                json $ toJSON [ payloadObjWith txsAt h | h <- hs ]
        _ -> respond $ responseLBS status404 [] "not found"

withOracle :: (IORef Int -> HostLimiters -> ServiceApi -> T.Text -> IO a) -> IO a
withOracle = withOracleTxs hasTxs

-- | The stub oracle with transactions at the heights of the predicate.
--
withOracleTxs :: (Word64 -> Bool) -> (IORef Int -> HostLimiters -> ServiceApi -> T.Text -> IO a) -> IO a
withOracleTxs txsAt act = do
    arrivals <- newIORef 0
    testWithApplication (return (oracleAppWith txsAt arrivals)) $ \port -> do
        let url = "http://127.0.0.1:" <> tshow port
        hl <- newHostLimiters 1_000 Nothing
        api <- oracleServiceApi hl mainnet url
        act arrivals hl api url

gen :: ServiceApi -> Word64 -> RequestClass -> Word64 -> IO Request
gen api seed c h = requestsFor api defaultPactSet seed (c, cid0, BlockHeight h)

genSnap :: ServiceApi -> Word64 -> RequestClass -> Word64 -> IO Request
genSnap api seed c h = requestsForWith (GenOptions True) api defaultPactSet seed (c, cid0, BlockHeight h)

prefix :: T.Text
prefix = "/chainweb/0.0/mainnet01/chain/0"

bodyValue :: Request -> Maybe Value
bodyValue r = _reqBody r >>= decodeStrict

-- -------------------------------------------------------------------------- --
-- requestsFor

deterministic :: Assertion
deterministic = withOracle $ \_ _ api _ -> do
    let targets = [ (c, h) | c <- allRequestClasses, h <- [300, 301, 457, 999] ]
    a <- mapM (\(c, h) -> gen api 1 c h) targets
    b <- mapM (\(c, h) -> gen api 1 c h) targets
    a @?= b
    c2 <- mapM (\(c, h) -> gen api 2 c h) targets
    assertBool "seed 2 gives other requests" (a /= c2)
    map _reqClass a @?= map fst targets
    -- every generated request is sent: none is decided before sending (an
    -- spv request needs its target chain covered too)
    let cov = Coverage (M.fromList [ (unsafeChainId i, [(0, 2_000)]) | i <- [0 .. 19] ])
    forM_ a $ \r -> preJudge cov r @?= Nothing

oracleLimiter :: Assertion
oracleLimiter = withOracle $ \arrivals hl api url -> do
    lim <- limiterFor hl url
    side <- Side "oracle" url <$> limiterFor hl url
    p0 <- limiterPermits lim
    forM_ allRequestClasses $ \c -> gen api 1 c 300
    n <- readIORef arrivals
    p1 <- limiterPermits lim
    assertBool "lookups were made" (n >= length allRequestClasses)
    p1 - p0 @?= fromIntegral n
    limiterPermits (_sideLimiter side) >>= (@?= p1)

headerRequests :: Assertion
headerRequests = withOracle $ \_ _ api _ -> do
    rs <- forM [1 .. 40] $ \s -> gen api s ClsHeader 300
    forM_ rs $ \r -> do
        assertBool ("no plain range: " <> T.unpack (_reqPath r)) (not ("/header?" `T.isInfixOf` _reqPath r))
        case T.stripPrefix (prefix <> "/header/") (_reqPath r) of
            Just rest | "branch?" `T.isPrefixOf` rest -> do
                _reqMethod r @?= methodPost
                let top = fromMaybe 0 (_reqTarget r)
                assertBool "the range ends at the target" (("maxheight=" <> tshow top) `T.isInfixOf` rest)
                assertBool "the range starts at the sampled height" ("minheight=300&" `T.isInfixOf` rest)
                (bodyValue r >>= field ["upper"]) @?= Just (toJSON [hashAt (getBlockHeight top)])
            Just h -> do
                _reqMethod r @?= methodGet
                h @?= hashAt 300
                _reqTarget r @?= Just 300
            Nothing -> assertFailure ("not a header request: " <> T.unpack (_reqPath r))
    assertBool "by hash" (any ((== Just 300) . _reqTarget) rs && any ((== methodGet) . _reqMethod) rs)
    assertBool "branch" (any ((== methodPost) . _reqMethod) rs)
    snap <- forM [1 .. 40] $ \s -> genSnap api s ClsHeader 300
    assertBool "a plain range with --same-snapshot"
        (any (\r -> (prefix <> "/header?minheight=300&maxheight=") `T.isPrefixOf` _reqPath r) snap)

payloadRequests :: Assertion
payloadRequests = withOracle $ \_ _ api _ -> do
    rs <- forM [1 .. 60] $ \s -> gen api s ClsPayload 300
    let paths = map _reqPath rs
        has p = assertBool "a request of each form" (any p rs)
    has $ \r -> _reqPath r == prefix <> "/payload/P300" && _reqTarget r == Just 300
    has $ \r -> _reqPath r == prefix <> "/payload/P300?height=300"
    has $ \r -> _reqPath r == prefix <> "/payload/P300/outputs"
    has $ \r -> _reqPath r == prefix <> "/payload/P300/outputs?height=300"
    has $ \r -> _reqPath r == prefix <> "/payload/batch" && fmap (B.take 1) (_reqBody r) == Just "["
    has $ \r -> _reqPath r == prefix <> "/payload/batch" && maybe False ("heights" `B.isInfixOf`) (_reqBody r)
    has $ \r -> _reqPath r == prefix <> "/payload/outputs/batch"
    forM_ rs $ \r -> when ("batch" `T.isSuffixOf` _reqPath r) $ do
        _reqMethod r @?= methodPost
        let hashes = case bodyValue r of
                Just (Array a) -> V.toList a
                Just o -> maybe [] (\case Array a -> V.toList a; _ -> []) (field ["hashes"] o)
                Nothing -> []
        assertBool "a batch holds the sampled payload" (toJSON (payloadAt 300) `elem` hashes)
        assertBool "the target is the highest height in the batch"
            (toJSON (payloadAt (maybe 0 getBlockHeight (_reqTarget r))) `elem` hashes)
    assertBool "four single paths and two batch paths" (length (nub paths) == 6)

pinnedRequests :: Assertion
pinnedRequests = withOracle $ \_ _ api _ -> do
    let accounts = ["k:miner", "k:sender300"]
    locals <- forM [1 .. 30] $ \s -> gen api s ClsLocal 300
    forM_ locals $ \r -> do
        _reqMethod r @?= methodPost
        _reqPath r @?= prefix <> "/pact/api/v1/local?rewindDepth=" <> depthPlaceholder
        _reqTarget r @?= Just 300
        p <- either assertFailure return (commandPayload (fromMaybe "" (_reqBody r)))
        field ["meta", "creationTime"] p @?= Just (Number 1_600_000_000)
        let code = case field ["payload", "exec", "code"] p of
                Just (String t) -> t
                _ -> maybe "" (\case String t -> t; _ -> "") (field ["payload", "cont", "pactId"] p)
        assertBool ("placeholders filled: " <> T.unpack code) (not ("{" `T.isInfixOf` code))
    let codes = mapMaybe (\r -> commandPayload' r >>= field ["payload", "exec", "code"]) locals
    assertBool "an account of the block is used"
        (any (\case String t -> any (`T.isInfixOf` t) accounts; _ -> False) codes)
    assertBool "a continuation of a pact of the block"
        (any (\r -> (commandPayload' r >>= field ["payload", "cont", "pactId"]) == Just (String "pact300")) locals)

    pre <- forM [1 .. 10] $ \s -> gen api s ClsPreflight 300
    forM_ pre $ \r -> do
        _reqPath r @?= prefix <> "/pact/api/v1/local?preflight=true&signatureVerification=false&rewindDepth=" <> depthPlaceholder
        p <- either assertFailure return (commandPayload (fromMaybe "" (_reqBody r)))
        field ["meta", "creationTime"] p @?= Just (Number (fromIntegral (creationSecondsAt 300 - 600)))
        case field ["meta", "sender"] p of
            Just (String s) -> assertBool "the sender is an account of the block" (s `elem` accounts)
            v -> assertFailure ("sender: " <> show v)
        assertBool "the sender's keys sign" (field ["signers"] p /= Just (toJSON ([] :: [Value])))

    hist <- forM [1 .. 20] $ \s -> gen api s ClsHistory 300
    forM_ hist $ \r -> do
        _reqClass r @?= ClsHistory
        _reqTarget r @?= Just 300
        assertBool "pinned" (hasDepthPlaceholder r)
    let hcodes = [ t | Just (String t) <- map (\r -> commandPayload' r >>= field ["payload", "exec", "code"]) hist ]
    assertBool "a txid of the block" (any (\t -> any (`T.isInfixOf` t) ["3000", "3001", "3002"]) hcodes)
  where
    commandPayload' r = either (const Nothing) Just (commandPayload (fromMaybe "" (_reqBody r)))

historyRefused :: Assertion
historyRefused = withOracle $ \arrivals _ api _ -> do
    r <- try @SomeException (gen api 1 ClsHistory 5_555_698 >>= \x -> x `seq` return x)
    assertBool "refused" (either (const True) (const False) r)
    readIORef arrivals >>= (@?= 0)

pollAndSpv :: Assertion
pollAndSpv = withOracle $ \_ _ api _ -> do
    let known = [rk 303 0, rk 303 1]
    polls <- forM [1 .. 10] $ \s -> gen api s ClsPoll 301
    forM_ polls $ \r -> do
        _reqPath r @?= prefix <> "/pact/api/v1/poll"
        _reqTarget r @?= Just 303
        keys <- case bodyValue r >>= field ["requestKeys"] of
            Just (Array a) -> return [ t | String t <- V.toList a ]
            v -> assertFailure ("requestKeys: " <> show v)
        assertBool "a key of the covered payload" (any (`elem` known) keys)
        assertBool "an unknown key" (any (\k -> not ("rk" `T.isPrefixOf` k)) keys)
    spvs <- forM [1 .. 10] $ \s -> gen api s ClsSpv 301
    forM_ spvs $ \r -> do
        _reqPath r @?= prefix <> "/pact/spv"
        _reqTarget r @?= Just 303
        case bodyValue r >>= field ["requestKey"] of
            Just (String k) -> assertBool "a sampled transaction" (k `elem` known)
            v -> assertFailure ("requestKey: " <> show v)
        assertBool "a target chain" (isJust' (bodyValue r >>= field ["targetChainId"]))
  where
    isJust' = maybe False (const True)

-- -------------------------------------------------------------------------- --
-- Coverage

genesisCoverageTest :: Assertion
genesisCoverageTest = do
    genesisCoverage mainnet [cid0, cid10] (800_000, 900_000) @?= Coverage (M.fromList
        [ (cid0, [(800_000, 900_000)])
        , (cid10, [(852_054, 900_000)])
        ])
    genesisCoverage mainnet [cid10] (100, 200) @?= Coverage (M.fromList [(cid10, [])])
    let spec cov = SampleSpec 1 500 allRequestClasses cov mainnetBoundaries boundaryShare (historyMaxHeight mainnet) Nothing
        ts = sampleTargets (spec (genesisCoverage mainnet [cid0, cid10] (852_000, 852_100)))
    assertBool "targets drawn" (length ts == 500)
    forM_ ts $ \(_, c, h) -> when (c == cid10) $
        assertBool ("chain 10 at " <> show h) (h >= 852_054)
    assertBool "chain 10 is sampled" (any (\(_, c, _) -> c == cid10) ts)
    sampleTargets (spec (genesisCoverage mainnet [cid10] (100, 200))) @?= []

-- -------------------------------------------------------------------------- --
-- SPV with a real proof

spvRealProof :: Assertion
spvRealProof = do
    let txs = V.fromList
            [ (Transaction (B8.pack ("tx" <> show i)), TransactionOutput (B8.pack ("out" <> show i))) | i <- [0 .. 4 :: Int] ]
        co = CoinbaseOutput "coinbase"
        payload = newBlockPayload (MinerData "miner") co txs :: BlockPayload
        outs = snd (newBlockOutputs co (snd <$> txs)) :: BlockOutputs
        (subj, pos, tree) = bodyTree @ChainwebMerkleHashAlgorithm @ChainwebHashTag outs 2
    proof <- merkleProof_ subj ((pos, tree) NE.:| [headerTree_ @BlockOutputsHash @ChainwebMerkleHashAlgorithm payload])
    let objBytes = encodeMerkleProofObject (_merkleProofObject proof) :: B.ByteString
        flipped = B.init objBytes <> B.singleton (B.last objBytes `xor` 1)
    obj' <- decodeMerkleProofObject flipped
    let good = TransactionOutputProof cid0 proof :: TransactionOutputProof ChainwebMerkleHashAlgorithm
        bad = TransactionOutputProof cid0 proof { _merkleProofObject = obj' }
        spvBody p = BL.toStrict (encode (encodeB64UrlNoPaddingText (BL.toStrict (encode p))))
        cov = Coverage (M.fromList [(cid0, [(0, 1_000)])])
        req = Request ClsSpv cid0 (Just 300) methodPost (prefix <> "/pact/spv") (Just "{}")
        ans = Answer 200
    -- the encoded proof decodes to the same proof
    (decodeStrict (spvBody good) >>= \case
        String t -> either (const Nothing) Just (decodeB64UrlNoPaddingText t) >>= decodeStrict
        _ -> Nothing) @?= Just good
    judge cov req (ans (spvBody good) Nothing) (ans (spvBody good) Nothing) @?= Final Same
    judge cov req (ans (spvBody good) Nothing) (ans (spvBody bad) Nothing) @?= CompareSpvSubjects
    case compareSpvProofs (ans (spvBody good) Nothing) (ans (spvBody bad) Nothing) of
        Differ d -> assertBool ("roots differ: " <> T.unpack d) ("different roots" `T.isInfixOf` d)
        v -> assertFailure ("tampered path element: " <> show v)
    -- the same proof in another encoding is allowed: the proof decodes, so the
    -- difference above is the root
    case compareSpvProofs (ans (spvBody good) Nothing) (ans (BL.toStrict (encode good)) Nothing) of
        Allowed AllowSpvTarget _ -> return ()
        v -> assertFailure ("re-encoded proof: " <> show v)

-- -------------------------------------------------------------------------- --
-- Report

outcomeWith :: Int -> RequestClass -> Int -> B.ByteString -> Outcome
outcomeWith i c s b = Outcome
    { _outIndex = i
    , _outRequest = Request c cid0 (Just 300) methodPost (prefix <> "/pact/api/v1/local?rewindDepth=" <> depthPlaceholder) (Just "{}")
    , _outVerdict = Same
    , _outOracle = Just (Answer s b Nothing)
    , _outTest = Just (Answer s b Nothing)
    , _outAttempts = 1
    , _outDepths = Just (700, 690)
    , _outNoAnswer = Nothing
    }

pactFailures :: Assertion
pactFailures = do
    let failure = "{\"result\":{\"status\":\"failure\",\"error\":{\"message\":\"row not found\"}},\"metaData\":null}"
        success = "{\"result\":{\"status\":\"success\",\"data\":1},\"metaData\":null}"
        preFailure = "{\"preflightResult\":{\"result\":{\"status\":\"failure\",\"error\":{}}},\"preflightWarnings\":[]}"
        rep = foldl addOutcome emptyReport
            [ outcomeWith 0 ClsLocal 200 failure
            , outcomeWith 1 ClsLocal 200 success
            , outcomeWith 2 ClsLocal 400 failure
            , outcomeWith 3 ClsHistory 200 success
            , outcomeWith 4 ClsPreflight 200 preFailure
            , outcomeWith 5 ClsPayload 200 failure
            , outcomeWith 6 ClsLocal 200 failure
            ]
    reportPactFailures rep ClsLocal @?= 2
    reportPactFailures rep ClsHistory @?= 0
    reportPactFailures rep ClsPreflight @?= 1
    reportPactFailures rep ClsPayload @?= 0
    reportVerdict rep @?= PartialPass
    field ["perClass", "local", "pactFailure200"] (toJSON rep) @?= Just (Number 2)
    assertBool "the summary names the count" ("pact failure" `isInfixOf` T.unpack (reportSummary rep))

expectedClasses :: Assertion
expectedClasses = do
    let rep = addOutcome emptyReport (outcomeWith 0 ClsPayload 200 "x")
    reportVerdict rep @?= PartialPass
    reportVerdict (reportExpectClasses [ClsPayload] rep) @?= PartialPass
    let rep7 = reportExpectClasses allRequestClasses rep
    reportVerdict rep7 @?= Void
    let reasons = reportVoidReasons rep7
    forM_ (filter (/= ClsPayload) allRequestClasses) $ \c ->
        assertBool (show c <> " named") (any (requestClassText c `T.isPrefixOf`) reasons)
    assertBool "payload not named" (not (any ("payload" `T.isPrefixOf`) reasons))

-- -------------------------------------------------------------------------- --
-- Fix round 1 (Task 6)

-- | The code of an exec command.
--
execCode :: Request -> Maybe T.Text
execCode r = do
    b <- _reqBody r
    c <- decodeStrict b >>= parseMaybe (withObject "command" (.: "cmd"))
    v <- decodeStrict (T.encodeUtf8 c)
    field ["payload", "exec", "code"] v >>= \case
        String code -> Just code
        _ -> Nothing

requestKeysOf :: Request -> [T.Text]
requestKeysOf r = case bodyValue r >>= field ["requestKeys"] of
    Just (Array a) -> [ t | String t <- V.toList a ]
    _ -> []

liveHistory :: Assertion
liveHistory = withOracle $ \_ _ api _ -> do
    let targets = [ (s, 1 + (s * 37) `mod` 900) | s <- [1 .. 1000] ]
    live <- forM targets $ \(s, h) -> execCode <$> gen api s ClsHistory h
    forM_ live $ \c -> assertBool ("txlog only: " <> show c) (maybe False ("(txlog " `T.isPrefixOf`) c)
    snap <- forM (take 200 targets) $ \(s, h) -> execCode <$> genSnap api s ClsHistory h
    forM_ ["(txids ", "(keylog "] $ \f ->
        assertBool (T.unpack f <> "with --same-snapshot") (any (maybe False (f `T.isPrefixOf`)) snap)

liveKeys :: Assertion
liveKeys = withOracle $ \_ _ api _ -> do
    let targets = [ (s, 1 + (s * 37) `mod` 900) | s <- [1 .. 300] ]
    live <- forM targets $ \(s, h) -> execCode <$> gen api s ClsLocal h
    forM_ [ c | Just c <- live, "(keys " `T.isInfixOf` c ] $ \c -> c @?= "(keys coin.allocation-table)"
    assertBool "keys on the small table is sent" (Just "(keys coin.allocation-table)" `elem` live)
    snap <- forM targets $ \(s, h) -> execCode <$> genSnap api s ClsLocal h
    assertBool "keys on a large table with --same-snapshot"
        (any (maybe False (\c -> "(keys " `T.isInfixOf` c && not ("allocation-table" `T.isInfixOf` c))) snap)

pactSetBounds :: Assertion
pactSetBounds = do
    let es = _pactEntries defaultPactSet
        live = filter (not . _peSameSnapshotOnly) es
        byName n = filter ((== n) . _peName) es
    forM_ live $ \e -> assertBool (T.unpack (_peName e) <> ": the why records a bound") ("Bound:" `T.isInfixOf` _peWhy e)
    forM_ ["txids", "keylog", "txids-small-table", "keys-large-table"] $ \n -> case byName n of
        [e] -> assertBool (T.unpack n <> " is same-snapshot only") (_peSameSnapshotOnly e)
        x -> assertFailure (T.unpack n <> ": " <> show x)
    forM_ live $ \e -> do
        when ("(keys " `T.isInfixOf` _peCode e) $ _peCode e @?= "(keys coin.allocation-table)"
        assertBool (T.unpack (_peName e) <> ": no txids or keylog") $
            not (any (`T.isInfixOf` _peCode e) ["(txids ", "(keylog "])
    forM_ [ e | e <- es, _peKind e == PactCont ] $ \e -> _peStep e @?= 0
    assertBool "a row created late that is found above its creation height"
        (any (not . _peSameSnapshotOnly) (byName "namespace-before-creation"))

sparseTxs :: Assertion
sparseTxs = withOracleTxs (== 700) $ \_ _ api _ -> do
    let known = [rk 700 0, rk 700 1]
    forM_ [1 .. 5] $ \s -> do
        p <- gen api s ClsPoll 100
        _reqTarget p @?= Just 700
        assertBool "a real key" (any (`elem` known) (requestKeysOf p))
        sp <- gen api s ClsSpv 100
        _reqTarget sp @?= Just 700
        case bodyValue sp >>= field ["requestKey"] of
            Just (String k) -> assertBool "a real key" (k `elem` known)
            v -> assertFailure ("requestKey: " <> show v)

noTxSkip :: Assertion
noTxSkip = withOracleTxs (const False) $ \arrivals _ api _ -> do
    r <- try @RequestGenError (gen api 1 ClsPoll 0)
    case r of
        Left (NoTxWithin lo hi) -> (lo, hi) @?= (0, txScanMax - 1)
        Left e -> assertFailure ("expected NoTxWithin: " <> show e)
        Right q -> assertFailure ("expected NoTxWithin, got a request: " <> show q)
    n <- readIORef arrivals
    assertBool ("the scan is bounded: " <> show n <> " lookups") (n <= 60)
    (reqs, skips) <- generateRequestsSkipping defaultGenOptions api defaultPactSet 1
        [(ClsPoll, cid0, 0), (ClsSpv, cid0, 500), (ClsHeader, cid0, 10)]
    map _reqClass reqs @?= [ClsHeader]
    map _gsKind skips @?= [skipNoTx, skipNoTx]

realKeys :: Assertion
realKeys = do
    let pollOut i b = outcomeWith i ClsPoll 200 b
        spvOut i st = outcomeWith i ClsSpv st "{}"
        hollow = foldl addOutcome emptyReport [pollOut 0 "{}", spvOut 1 400]
        real = foldl addOutcome emptyReport [pollOut 0 "{\"rk\":{}}", pollOut 1 "{}", spvOut 2 200]
    reportRealKeyCompared hollow ClsPoll @?= 0
    reportRealKeyCompared hollow ClsSpv @?= 0
    reportRealKeyCompared real ClsPoll @?= 1
    reportRealKeyCompared real ClsSpv @?= 1
    let reasons = reportVoidReasons (reportExpectClasses [ClsPoll, ClsSpv] hollow)
    forM_ ["poll", "spv"] $ \c -> assertBool (T.unpack c <> " named: " <> show reasons) $
        any (\x -> (c <> ":") `T.isPrefixOf` x && "real key" `T.isInfixOf` x) reasons
    reportVoidReasons (reportExpectClasses [ClsPoll, ClsSpv] real) @?= []
    field ["perClass", "poll", "realKeyCompared"] (toJSON real) @?= Just (Number 1)

partialPass :: Assertion
partialPass = do
    let body c = if c == ClsPoll then "{\"rk\":{}}" else "{}"
        run cs = foldl addOutcome emptyReport [ outcomeWith i c 200 (body c) | (i, c) <- zip [0 ..] cs ]
        six = filter (/= ClsSpv) allRequestClasses
    reportVerdict (reportExpectClasses allRequestClasses (run allRequestClasses)) @?= Pass
    reportVerdict (run allRequestClasses) @?= Pass
    reportVerdict (reportExpectClasses six (run six)) @?= PartialPass
    reportVerdict (run six) @?= PartialPass
    gateExitCode PartialPass @?= ExitFailure 4
    -- hollow poll in a run of every class: Void, not Pass
    reportVerdict (reportExpectClasses allRequestClasses (run six `addOutcome` outcomeWith 9 ClsSpv 400 "{}")) @?= Void

skipKinds :: Assertion
skipKinds = do
    let rep = reportGeneration 10 (replicate 3 (ClsPoll, skipNoTx) <> [(ClsLocal, skipLookupFailed)])
            (addOutcome emptyReport (outcomeWith 0 ClsPayload 200 "x"))
        summary = reportSummary rep
    reportGenSkippedKind rep skipNoTx @?= 3
    reportGenSkipped rep ClsPoll @?= 3
    assertBool ("summary per kind: " <> T.unpack summary) $
        "no tx within 1000 blocks: 3" `T.isInfixOf` summary && "an oracle lookup failed: 1" `T.isInfixOf` summary
    assertBool "void reason per kind" (any ("no tx within 1000 blocks: 3" `T.isInfixOf`) (reportVoidReasons rep))

spvTargetCoverage :: Assertion
spvTargetCoverage = do
    let cid1 = unsafeChainId 1
        spvTo :: T.Text -> Request
        spvTo trg = Request ClsSpv cid0 (Just 300) methodPost (prefix <> "/pact/spv")
            (Just (BL.toStrict (encode (object ["requestKey" .= ("k" :: T.Text), "targetChainId" .= trg]))))
        cov = Coverage . M.fromList
        c0 = (cid0, [(0, 1_000)])
    preJudge (cov [c0]) (spvTo "1") @?= Just (Skipped NotCovered)
    preJudge (cov [c0, (cid1, [(0, 1_000)])]) (spvTo "1") @?= Nothing
    preJudge (cov [c0, (cid1, [(0, 302)])]) (spvTo "1") @?= Just (Skipped NotCovered)
    preJudge (cov [c0]) (spvTo "0") @?= Nothing

allowReadsOption :: Assertion
allowReadsOption = do
    let allow args = case O.execParserPure O.defaultPrefs (O.info localSettingsParser mempty) args of
            O.Success x -> Just (_lsAllowReadsInLocal x)
            _ -> Nothing
    allow ["--allow-reads-in-local", "true"] @?= Just True
    allow ["--allow-reads-in-local", "false"] @?= Just False
    allow [] @?= Nothing
    allow ["--allow-reads-in-local"] @?= Nothing
    allow ["--allow-reads-in-local", "yes"] @?= Nothing
