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

import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, try)
import Control.Monad
import Data.Aeson
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import Data.Aeson.Types (parseMaybe)
import Data.Either (isLeft)
import Data.Bits (xor)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import Data.IORef
import Data.List (isInfixOf, isPrefixOf, nub, sort)
import qualified Data.List.NonEmpty as NE
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe, mapMaybe)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import qualified Network.HTTP.Client as HTTP
import Data.Word (Word64)
import Network.HTTP.Types
import Network.Wai (Application, pathInfo, queryString, responseLBS, responseRaw, strictRequestBody)
import Network.Wai.Handler.Warp (testWithApplication)
import System.Directory (getTemporaryDirectory, removeFile)
import System.IO (hClose, openTempFile)
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
import Chainweb.StorageTools.Http (ServiceApi(..), limiterPermits)
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
    , testCase "--oracle-local-timeout takes 1 to 3600 seconds and reaches the run options" oracleLocalTimeoutOption
    , testCase "an oracle lookup is sent once after a status other than 429 or 503, a closed connection or a response timeout; 429, 503, 200 arrive 3 times" lookupNotResent
    , testCase "an oracle lookup through oracleServiceApi on a reused connection that the server closes without an answer arrives once" lookupNotResentOnReusedConnection
    , testCase "a custom Pact set fails closed: a cont entry needs a step; a live entry needs a bound, step 0, no scan" customPactSet
    , testCase "a request file holds only the read requests of the 7 classes, pinned ones with {rewindDepth} and a target" requestFileAllowlist
    , testCase "a class whose every compared answer is the same non-2xx is at best PartialPass" sameNon2xxOnly
    , testCase "a Pact set entry that expects success needs a Same success in each class, or the run is at best PartialPass" hollowEntries
    , testCase "lookups whose window crosses the oracle's tip stop at the height of its cut" nearTip
    , testCase "a preflight sender is a k: account of the block; plain /local takes any account" preflightSenders
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

-- | A payload with the miner account given, transactions at the heights of
-- the predicate.
--
payloadObjMiner :: T.Text -> (Word64 -> Bool) -> Word64 -> Value
payloadObjMiner minerAccount txsAt h = object
    [ "transactions" .= [ [b64 (txJson i), b64 (outJson i)] | txsAt h, i <- [0, 1] ]
    , "minerData" .= b64 (object
        [ "account" .= minerAccount, "predicate" .= ("keys-all" :: T.Text), "public-keys" .= ["miner-key" :: T.Text] ])
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
oracleAppWith = oracleAppMiner "k:miner"

-- | 'oracleAppWith' with another miner account.
--
oracleAppMiner :: T.Text -> (Word64 -> Bool) -> IORef Int -> Application
oracleAppMiner minerAccount txsAt arrivals req respond = do
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
        -- as in 3.2.1: a maxheight above the upper bound (the cut's header at
        -- 'stubTip') gives an empty branch, not one cut at the tip
        ["chain", _, "header", "branch"]
            | Just lo <- q "minheight", Just hi <- q "maxheight" ->
                let items = if hi > stubTip then [] else [ headerObj h | h <- reverse [lo .. hi] ]
                in json $ object [ "items" .= items, "limit" .= length items, "next" .= Null ]
        ["chain", _, "payload", "outputs", "batch"]
            | Just hs <- decodeStrict body >>= parseMaybe (withObject "batch" (.: "heights")) ->
                json $ toJSON [ payloadObjMiner minerAccount txsAt h | h <- hs ]
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
        req = Request ClsSpv cid0 (Just 300) methodPost (prefix <> "/pact/spv") (Just "{}") Nothing
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
    , _outRequest = Request c cid0 (Just 300) methodPost (prefix <> "/pact/api/v1/local?rewindDepth=" <> depthPlaceholder) (Just "{}") Nothing
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

-- | The checks of 'pactSetProblems', which 'loadPactSet' applies to a custom
-- set too: a bound in every live entry's why, no table scan in a live entry,
-- every continuation at step 0.
--
pactSetBounds :: Assertion
pactSetBounds = do
    let es = _pactEntries defaultPactSet
        byName n = filter ((== n) . _peName) es
    pactSetProblems defaultPactSet @?= []
    forM_ ["txids", "keylog", "txids-small-table", "keys-large-table"] $ \n -> case byName n of
        [e] -> assertBool (T.unpack n <> " is same-snapshot only") (_peSameSnapshotOnly e)
        x -> assertFailure (T.unpack n <> ": " <> show x)
    assertBool "a defpact state entry" (any ((== PactCont) . _peKind) es)
    assertBool "a row created late that is found above its creation height"
        (any (not . _peSameSnapshotOnly) (byName "namespace-before-creation"))
    -- the function finds what the checks look for
    let bad = PactSet
            [ PactEntry "unbounded" [ClsLocal] PactExec "(coin.details \"{account}\")" 0 "no bound" False ExpectSuccess
            , PactEntry "scan" [ClsLocal] PactExec "(keys  coin.coin-table)" 0 "Bound: none" False ExpectSuccess
            , PactEntry "history" [ClsHistory] PactExec "( txids coin.coin-table 0)" 0 "Bound: none" False ExpectSuccess
            , PactEntry "step1" [ClsLocal] PactCont "{pactId}" 1 "state" True ExpectFailure
            ]
    map (T.takeWhile (/= ':')) (pactSetProblems bad) @?= ["entry unbounded", "entry scan", "entry history", "entry step1"]

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
            (Just (BL.toStrict (encode (object ["requestKey" .= ("k" :: T.Text), "targetChainId" .= trg])))) Nothing
        cov = Coverage . M.fromList
        c0 = (cid0, [(0, 1_000)])
    preJudge (cov [c0]) (spvTo "1") @?= Just (Skipped NotCovered)
    preJudge (cov [c0, (cid1, [(0, 1_000)])]) (spvTo "1") @?= Nothing
    preJudge (cov [c0, (cid1, [(0, 302)])]) (spvTo "1") @?= Just (Skipped NotCovered)
    preJudge (cov [c0]) (spvTo "0") @?= Nothing

-- | @--oracle-local-timeout@ takes 1 to 3600 seconds, defaults to 30, and
-- reaches the 'RunOptions' that the run uses (N3 of the P0-B re-review).
--
oracleLocalTimeoutOption :: Assertion
oracleLocalTimeoutOption = do
    let parse args = case O.execParserPure O.defaultPrefs (O.info runOptionsParser mempty) args of
            O.Success x -> Just (_roOracleLocalTimeout x)
            _ -> Nothing
    parse ["--oracle-local-timeout", "0"] @?= Nothing
    parse ["--oracle-local-timeout", "3601"] @?= Nothing
    parse ["--oracle-local-timeout", "-1"] @?= Nothing
    parse ["--oracle-local-timeout", "ten"] @?= Nothing
    parse ["--oracle-local-timeout", "1"] @?= Just 1_000_000
    parse ["--oracle-local-timeout", "3600"] @?= Just 3_600_000_000
    parse [] @?= Just (_roOracleLocalTimeout defaultRunOptions)

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

-- -------------------------------------------------------------------------- --
-- Fix P0-A

-- | How often an oracle lookup (@oracleCall@) arrives at one loopback stub,
-- per kind of answer (N2 of the P0-B re-review). Only 429 and 503 are
-- refusals, which did no work, and retried; a connection closed without an
-- answer, a response timeout and any other status, the gateway answers 502
-- and 504 included, are final: a lookup is sent once and never again to a
-- production oracle.
--
lookupNotResent :: Assertion
lookupNotResent = do
    arrivals <- newIORef (0 :: Int)
    let status :: Status -> Application
        status st _ respond = respond (responseLBS st [] "")
    script <- newIORef (const (status status500))
    let app req respond = do
            _ <- strictRequestBody req
            n <- atomicModifyIORef' arrivals (\k -> (k + 1, k + 1))
            f <- readIORef script
            f n req respond
    testWithApplication (return app) $ \port -> do
        hl <- newHostLimiters 1_000 Nothing
        lim <- limiterFor hl ("http://127.0.0.1:" <> tshow port)
        forM_
            [ ("status 500", const (status status500), 1)
            , ("status 502", const (status status502), 1)
            , ("status 504", const (status status504), 1)
            , ("a connection closed without an answer"
              , const (\_ respond -> respond $ responseRaw (\_ _ -> return ()) (responseLBS status500 [] "no raw"))
              , 1
              )
            , ("429, then 503, then 200"
              , \n -> case n of
                    1 -> status status429
                    2 -> status status503
                    _ -> status status200
              , 3
              )
            , ("an answer after the response timeout"
              , const (\_ respond -> threadDelay 2_000_000 >> respond (responseLBS status200 [] ""))
              , 1
              )
            ] $ \(name, f, expected :: Int) -> do
                writeIORef arrivals 0
                writeIORef script f
                -- a new manager per row, so that no connection of an earlier
                -- row is reused (http-client resends once on a stale one);
                -- the response timeout is far below the stub's sleep
                mgr <- HTTP.newManager HTTP.defaultManagerSettings
                    { HTTP.managerResponseTimeout = HTTP.responseTimeoutMicro 300_000 }
                let api = ServiceApi ("http://127.0.0.1:" <> tshow port) mainnet mgr lim
                r <- try @SomeException (gen api 1 ClsHeader 10)
                assertBool (name <> ": the lookup fails") (either (const True) (const False) r)
                n <- readIORef arrivals
                assertEqual (name <> ": arrivals") expected n

-- | P0-C review M3: the manager of 'oracleServiceApi' does not resend. The
-- first lookup leaves its connection in the pool; the server closes it
-- without an answer to the second lookup, which http-client's default
-- settings would send again on a new connection.
--
lookupNotResentOnReusedConnection :: Assertion
lookupNotResentOnReusedConnection = do
    arrivals <- newIORef (0 :: Int)
    let app req respond = do
            _ <- strictRequestBody req
            n <- atomicModifyIORef' arrivals (\k -> (k + 1, k + 1))
            if n == 1
                then respond (responseLBS status200 [] "")
                else respond $ responseRaw (\_ _ -> return ()) (responseLBS status500 [] "no raw")
    testWithApplication (return app) $ \port -> do
        hl <- newHostLimiters 1_000 Nothing
        api <- oracleServiceApi hl mainnet ("http://127.0.0.1:" <> tshow port)
        _ <- try @SomeException (gen api 1 ClsHeader 10)
        readIORef arrivals >>= assertEqual "the first lookup arrives" 1
        r <- try @SomeException (gen api 1 ClsHeader 10)
        assertBool "the second lookup fails" (either (const True) (const False) r)
        n2 <- readIORef arrivals
        assertEqual ("the second lookup arrives once: " <> show r) 2 n2

-- | Loads a Pact set from YAML text through a file, as @--pact-set@ does.
--
loadText :: B.ByteString -> IO (Either SomeException PactSet)
loadText yaml = do
    tmp <- getTemporaryDirectory
    (f, h) <- openTempFile tmp "pact-set.yaml"
    B.hPut h yaml >> hClose h
    r <- try (loadPactSet f)
    removeFile f
    return r

customPactSet :: Assertion
customPactSet = do
    let entry extra = B8.unlines $
            [ "entries:"
            , "  - name: custom"
            , "    classes: [local]"
            ] <> map ("    " <>) extra
        rejected what yaml = loadText yaml >>= \case
            Left e -> assertBool (what <> ": the error names the entry: " <> show e) ("custom" `isInfixOf` show e)
            Right s -> assertFailure (what <> ": loaded " <> show s)
    case decodePactSet (entry ["kind: cont", "code: '{pactId}'", "why: 'Bound: one read'"]) of
        Left e -> assertBool ("names the step: " <> e) ("step" `isInfixOf` e)
        Right s -> assertFailure ("a cont entry without a step decodes: " <> show s)
    rejected "a live entry without a bound" $ entry ["code: '(coin.details \"{account}\")'", "why: 'coin details'"]
    rejected "a live cont entry at step 1" $ entry ["kind: cont", "code: '{pactId}'", "step: 1", "why: 'Bound: one read'"]
    rejected "a same-snapshot cont entry at step 1" $
        entry ["kind: cont", "code: '{pactId}'", "step: 1", "sameSnapshotOnly: true", "why: 'state'"]
    rejected "live keys on a large table" $ entry ["code: '(keys coin.coin-table)'", "why: 'Bound: none really'"]
    rejected "live txids" $ entry ["code: '(txids coin.coin-table 0)'", "why: 'Bound: none really'"]
    rejected "live select" $ entry ["code: '(select coin.coin-table (constantly true))'", "why: 'Bound: none really'"]
    ok1 <- loadText $ entry ["code: '(coin.details \"{account}\")'", "why: 'Bound: one indexed read'"]
    either (assertFailure . show) (\s -> map _peName (_pactEntries s) @?= ["custom"]) ok1
    ok2 <- loadText $ entry ["code: '(keys coin.coin-table)'", "sameSnapshotOnly: true", "why: 'no bound'"]
    either (assertFailure . show) (\s -> map _peSameSnapshotOnly (_pactEntries s) @?= [True]) ok2

-- | Every class Same with status 200, but the header class only with the same
-- 404: not a pass (Minor 3 of the P0 review).
--
sameNon2xxOnly :: Assertion
sameNon2xxOnly = do
    let body c = if c == ClsPoll then "{\"rk\":{}}" else "{}"
        status c = if c == ClsHeader then 404 else 200
        rep = reportExpectClasses allRequestClasses $ foldl addOutcome emptyReport
            [ outcomeWith i c (status c) (body c) | (i, c) <- zip [0 ..] allRequestClasses ]
    reportVerdict rep @?= PartialPass
    reportMissingClasses rep @?= [ClsHeader]
    -- an allowed difference covers its class
    let allowed = rep `addOutcome` (outcomeWith 9 ClsHeader 200 "{}") { _outVerdict = Allowed AllowOrphanPayload "evidence" }
    reportVerdict allowed @?= Pass

-- | Every generated request passes 'allowedRequest'; a request file line
-- that is not a read of the 7 classes, or a pinned one without the depth
-- placeholder or a target, stops the file with its line number.
--
requestFileAllowlist :: Assertion
requestFileAllowlist = do
    let v = apiPrefix mainnet
    withOracle $ \_ _ api _ -> forM_ [1 .. 40] $ \s -> forM_ allRequestClasses $ \c -> do
        let h = if c == ClsHistory then 300 else 1 + (s * 37) `mod` 900
        live <- gen api s c h
        snap <- genSnap api s c h
        allowedRequest v live @?= Right ()
        -- a request of a same-snapshot run holds a scan only in an entry that
        -- is sameSnapshotOnly; a request file is never a same-snapshot run
        let scans = maybe False (`elem` map _peName (filter _peSameSnapshotOnly (_pactEntries defaultPactSet)))
                (_reqEntry snap)
        if scans
            then assertBool "a same-snapshot scan is refused" (isLeft (allowedRequest v snap))
            else allowedRequest v snap @?= Right ()
    let local = Request ClsLocal cid0 (Just 300) methodPost
            (prefix <> "/pact/api/v1/local?rewindDepth=" <> depthPlaceholder) (Just (localCommand mainnet cid0 "(+ 1 2)" 1)) Nothing
        payload = Request ClsPayload cid0 (Just 300) methodGet (prefix <> "/payload/P300") Nothing Nothing
        file r = BL.toStrict (encode local) <> "\n\n" <> BL.toStrict (encode r) <> "\n"
        refused what r = case parseRequestFile v (file r) of
            Left e -> assertBool (what <> ": " <> e) ("3: not allowed: " `isInfixOf` e)
            Right _ -> assertFailure (what <> ": allowed")
    parseRequestFile v (file payload) @?= Right [local, payload]
    refused "PUT /cut" $ payload { _reqMethod = "PUT", _reqPath = "/chainweb/0.0/mainnet01/cut", _reqClass = ClsHeader }
    refused "/send" $ local { _reqPath = prefix <> "/pact/api/v1/send" }
    refused "/listen" $ local { _reqClass = ClsPoll, _reqPath = prefix <> "/pact/api/v1/listen" }
    refused "a dot segment" $ payload { _reqPath = prefix <> "/payload/../../cut" }
    refused "an encoded character" $ payload { _reqPath = prefix <> "/payload/P%2F300" }
    refused "another version" $ payload { _reqPath = "/chainweb/0.0/testnet04/chain/0/payload/P300" }
    refused "another chain in the path" $ payload { _reqPath = "/chainweb/0.0/mainnet01/chain/1/payload/P300" }
    refused "a payload path in the class local" $ payload { _reqClass = ClsLocal }
    refused "a GET with a body" $ payload { _reqBody = Just "{}" }
    refused "a POST /local without a body" $ local { _reqBody = Nothing }
    -- N1: the body of a /local is a command that runs what the generator runs
    let withBody b = local { _reqBody = Just b }
        history b = (withBody b) { _reqClass = ClsHistory }
        refusedFor what needle r = case parseRequestFile v (file r) of
            Left e -> assertBool (what <> ": " <> e) ("3: not allowed: " `isInfixOf` e && needle `isInfixOf` e)
            Right _ -> assertFailure (what <> ": allowed")
        meta = defaultLocalMeta
    refusedFor "a cont at step 1" "step other than 0" $
        withBody (localCommandWith mainnet cid0 meta (LocalCont "some-pact-id" 1) 1)
    -- P0-C review M1: a rollback at step 0 runs the step's rollback code
    let contAt0 = localCommandWith mainnet cid0 meta (LocalCont "some-pact-id" 0) 1
        rollbackFalse = "\\\"rollback\\\":false"
        (rbBefore, rbAfter) = B8.breakSubstring rollbackFalse contAt0
        rollbackTrue = rbBefore <> "\\\"rollback\\\":true" <> B8.drop (B8.length rollbackFalse) rbAfter
    assertBool ("the generator writes rollback false: " <> B8.unpack contAt0) (not (B8.null rbAfter))
    refusedFor "a cont at step 0 with rollback true" "rollback: true" $ withBody rollbackTrue
    refusedFor "a cont whose rollback is not a boolean" "not a command" $
        withBody (rbBefore <> "\\\"rollback\\\":\\\"true\\\"" <> B8.drop (B8.length rollbackFalse) rbAfter)
    refusedFor "txids in an exec" "table scan" $
        withBody (localCommand mainnet cid0 "(txids coin.coin-table 0)" 1)
    refusedFor "keys on a large table in an exec" "coin.allocation-table" $
        withBody (localCommand mainnet cid0 "(keys coin.coin-table)" 1)
    refusedFor "a gasLimit above the generator's" "gasLimit" $
        withBody (localCommandWith mainnet cid0 meta { _lmGasLimit = localGasLimit + 1 } (LocalExec "(+ 1 2)") 1)
    refusedFor "a History gasLimit above historyGasLimit" "gasLimit" $
        history (localCommandWith mainnet cid0 meta { _lmGasLimit = historyGasLimit + 1 } (LocalExec "(+ 1 2)") 1)
    refusedFor "a body that is not a command" "not a command" $ withBody "{}"
    -- what the generator writes loads unchanged: a cont at step 0, keys on
    -- coin.allocation-table, a History command at historyGasLimit
    let fine = [ withBody (localCommandWith mainnet cid0 meta (LocalCont "some-pact-id" 0) 1)
               , withBody (localCommand mainnet cid0 "(keys coin.allocation-table)" 1)
               , withBody (localCommand mainnet cid0 "(length (keys coin.allocation-table))" 2)
               , history (localCommandWith mainnet cid0 meta { _lmGasLimit = historyGasLimit } (LocalExec "(txlog coin.coin-table 1)") 1)
               ]
    parseRequestFile v (B8.unlines (map (BL.toStrict . encode) fine)) @?= Right fine
    refused "an unknown query parameter" $ payload { _reqPath = prefix <> "/payload/P300?foo=1" }
    refused "a height that is not a number" $ payload { _reqPath = prefix <> "/payload/P300?height=1;2" }
    refused "preflight=true in the class local" $
        local { _reqPath = prefix <> "/pact/api/v1/local?preflight=true&rewindDepth=" <> depthPlaceholder }
    refused "a preflight without preflight=true" $ local { _reqClass = ClsPreflight }
    -- Minor 5: a pinned request is pinned, or it is not in the file
    refused "a pinned request at a fixed depth" $ local { _reqPath = prefix <> "/pact/api/v1/local?rewindDepth=0" }
    refused "a pinned request without a depth" $ local { _reqPath = prefix <> "/pact/api/v1/local" }
    refused "a pinned request without a target" $ local { _reqTarget = Nothing }
    case parseRequestFile v "{\"class\":\"payload\"}\n" of
        Left e -> assertBool e ("1: " `isPrefixOf` e)
        Right x -> assertFailure (show x)

-- | The hollow Pact classes of the ledger: an entry that expects success and
-- only ever got the same error is not a pass; entries that expect a failure
-- are exempt.
--
hollowEntries :: Assertion
hollowEntries = do
    let success = "{\"result\":{\"status\":\"success\",\"data\":1},\"metaData\":null}"
        failure = "{\"result\":{\"status\":\"failure\",\"error\":{}},\"metaData\":null}"
        preSuccess = "{\"preflightResult\":{\"result\":{\"status\":\"success\"}},\"preflightWarnings\":[]}"
        withEntry e o = o { _outRequest = (_outRequest o) { _reqEntry = Just e } }
        body c = if c == ClsPoll then "{\"rk\":{}}" else "{}"
        others = [ outcomeWith i c 200 (body c) | (i, c) <- zip [0 ..] allRequestClasses, not (isPinnedClass c) ]
        pact =
            [ withEntry "coin-details" (outcomeWith 10 ClsLocal 200 failure)
            , withEntry "coin-details" (outcomeWith 11 ClsLocal 200 failure)
            , withEntry "defpact-state" (outcomeWith 12 ClsLocal 200 failure)
            , withEntry "txlog" (outcomeWith 13 ClsHistory 200 success)
            , withEntry "coin-details" (outcomeWith 14 ClsPreflight 200 preSuccess)
            ]
        expected = [(ClsLocal, "coin-details"), (ClsHistory, "txlog"), (ClsPreflight, "coin-details")]
        rep = reportExpectEntries expected $ reportExpectClasses allRequestClasses $
            foldl addOutcome emptyReport (others <> pact)
    reportVerdict rep @?= PartialPass
    reportHollowEntries rep @?= [(ClsLocal, "coin-details")]
    reportEntrySuccess rep ClsPreflight "coin-details" @?= 1
    assertBool "the summary names the entry" ("local:coin-details" `isInfixOf` T.unpack (reportSummary rep))
    field ["hollowEntries"] (toJSON rep) @?= Just (toJSON ["local:coin-details" :: T.Text])
    field ["entrySameSuccess", "history:txlog"] (toJSON rep) @?= Just (Number 1)
    reportVerdict (rep `addOutcome` withEntry "coin-details" (outcomeWith 15 ClsLocal 200 success)) @?= Pass
    -- a success that differs is no evidence (and fails the run)
    let differ = (withEntry "coin-details" (outcomeWith 15 ClsLocal 200 success)) { _outVerdict = Differ "d" }
    reportHollowEntries (rep `addOutcome` differ) @?= [(ClsLocal, "coin-details")]
    -- the built-in set: the entries that expect no success
    sort [ _peName e | e <- _pactEntries defaultPactSet, not (expectsSuccess e) ]
        @?= ["defpact-state", "keys-large-table", "namespace-before-creation", "table-before-creation"]
    -- generated /local requests carry their entry, which a request file keeps
    withOracle $ \_ _ api _ -> do
        r <- gen api 1 ClsLocal 300
        assertBool ("an entry of the set: " <> show (_reqEntry r)) $
            maybe False (`elem` map _peName (_pactEntries defaultPactSet)) (_reqEntry r)
        decode (encode r) @?= Just r
        p <- gen api 1 ClsPayload 300
        _reqEntry p @?= Nothing

-- | Minor 1 of the P0 review: a branch query whose maxheight is above the
-- cut's header is empty on 3.2.1, so a lookup window is clamped to the
-- chain's height in the cut.
--
nearTip :: Assertion
nearTip = withOracle $ \_ _ api _ -> do
    forM_ [1 .. 30] $ \s -> do
        p <- gen api s ClsPayload (stubTip - 1)
        assertBool ("payload target at most the tip: " <> show (_reqTarget p)) (_reqTarget p <= Just (BlockHeight stubTip))
        h <- gen api s ClsHeader (stubTip - 2)
        assertBool ("header target at most the tip: " <> show (_reqTarget h)) (_reqTarget h <= Just (BlockHeight stubTip))
    -- heights 998 and 1000 hold no transaction, 999 does
    poll <- gen api 1 ClsPoll (stubTip - 2)
    _reqTarget poll @?= Just (BlockHeight (stubTip - 1))
    -- at the tip itself, without a transaction above it: no tx up to the tip
    r <- try @RequestGenError (gen api 1 ClsSpv stubTip)
    case r of
        Left (NoTxWithin lo hi) -> (lo, hi) @?= (stubTip, stubTip)
        x -> assertFailure ("expected NoTxWithin at the tip: " <> show x)

-- | Task 6 re-review (1): coin's enforce-reserved gives a k: account a keyset
-- guard, so a preflight's buy-gas evaluates no user guard.
--
preflightSenders :: Assertion
preflightSenders = do
    arrivals <- newIORef 0
    testWithApplication (return (oracleAppMiner "NoMiner" hasTxs arrivals)) $ \port -> do
        hl <- newHostLimiters 1_000 Nothing
        api <- oracleServiceApi hl mainnet ("http://127.0.0.1:" <> tshow port)
        let sender r = either (const Nothing) (field ["meta", "sender"]) (commandPayload (fromMaybe "" (_reqBody r)))
        -- 300 has transactions of k:sender300, 301 has none
        withTxs <- forM [1 .. 20] $ \s -> sender <$> gen api s ClsPreflight 300
        nub withTxs @?= [Just (String "k:sender300")]
        without <- forM [1 .. 20] $ \s -> sender <$> gen api s ClsPreflight 301
        nub without @?= [Just (String "cw-compare")]
        -- plain /local fills {account} from any account, the miner included
        codes <- forM [1 .. 40] $ \s -> execCode <$> gen api s ClsLocal 301
        assertBool "a plain /local reads the miner NoMiner" (any (maybe False ("NoMiner" `T.isInfixOf`)) codes)
