{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.StorageTools.Test.Compare
--
-- Tests of the @cw-compare@ core: the comparison rules, the seeded sampling,
-- the runner against stub services on 127.0.0.1, and the report. Nothing here
-- touches the network beyond the loopback interface.
--
module Chainweb.StorageTools.Test.Compare
( tests
) where

import Control.Concurrent (threadDelay)
import Control.Exception (toException)
import Control.Monad (when)

import Data.Aeson (Value(..), decode, decodeStrict, encode, object, withObject, (.:), (.=))
import Data.Aeson.Types (Parser, parseMaybe)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import Data.IORef
import Data.Word (Word64)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import GHC.Clock (getMonotonicTime)

import qualified Network.HTTP.Client as HTTP
import Network.HTTP.Types
import Network.Wai
    (Application, pathInfo, queryString, rawPathInfo, rawQueryString, responseLBS, responseRaw, strictRequestBody)
import Network.Wai.Handler.Warp (testWithApplication)

import System.Exit (ExitCode(..))
import System.LogLevel (LogLevel(..))

import Test.Tasty
import Test.Tasty.HUnit

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.Payload (BlockPayloadHash)
import Chainweb.StorageTools.Compare.Report
import Chainweb.StorageTools.Compare.Rules
import Chainweb.StorageTools.Compare.Run
import Chainweb.StorageTools.Compare.Sample
import Chainweb.StorageTools.Compare.Types
import Chainweb.StorageTools.Http
import Chainweb.Utils (HasTextRepresentation, fromJuste, fromText, toText)
import Chainweb.Version.Mainnet (mainnet)

tests :: TestTree
tests = testGroup "Compare"
    [ rulesTests
    , sampleTests
    , reportTests
    , runTests
    ]

-- -------------------------------------------------------------------------- --
-- Fixtures

-- | A base64url hash text of 32 bytes; the last character tells them apart.
--
hashText :: Char -> T.Text
hashText c = T.pack (replicate 42 'A' <> [c])

mkHash :: HasTextRepresentation a => Char -> a
mkHash = fromJuste . fromText . hashText

cid0, cid3 :: ChainId
cid0 = unsafeChainId 0
cid3 = unsafeChainId 3

cov :: Coverage
cov = Coverage $ M.fromList [(cid0, [(0, 2_000)])]

apiPrefix :: T.Text
apiPrefix = "/chainweb/0.0/test"

payloadA, payloadB, payloadO, payloadC :: BlockPayloadHash
payloadA = mkHash 'A'
payloadB = mkHash 'E'
payloadO = mkHash 'I'  -- the orphan
payloadC = mkHash 'M'  -- the payload of every canonical header of the stubs

payloadReqFor :: BlockPayloadHash -> Request
payloadReqFor h = Request ClsPayload cid0 (Just 100) methodGet
    (apiPrefix <> "/chain/0/payload/" <> toText h) Nothing Nothing

payloadReq :: Request
payloadReq = payloadReqFor payloadA

headerReq :: Request
headerReq = Request ClsHeader cid0 (Just 100) methodGet
    (apiPrefix <> "/chain/0/header?minheight=100&maxheight=100") Nothing Nothing

localReqAt :: BlockHeight -> Request
localReqAt h = Request ClsLocal cid0 (Just h) methodPost
    (apiPrefix <> "/chain/0/pact/api/v1/local?rewindDepth=" <> depthPlaceholder)
    (Just "{\"code\":\"(coin.details \\\"k:00\\\")\"}") Nothing

keysReqAt :: BlockHeight -> Request
keysReqAt h = (localReqAt h) { _reqBody = Just "{\"code\":\"(keys coin.coin-table)\"}" }

historyReqAt :: BlockHeight -> Request
historyReqAt h = (localReqAt h)
    { _reqClass = ClsHistory
    , _reqBody = Just "{\"code\":\"(txids coin.coin-table 0)\"}"
    }

localReq, keysReq :: Request
localReq = localReqAt 100
keysReq = keysReqAt 100

uncoveredReq :: Request
uncoveredReq = (payloadReqFor payloadA)
    { _reqChain = cid3
    , _reqPath = apiPrefix <> "/chain/3/payload/" <> toText payloadA
    }

ok :: B.ByteString -> Answer
ok b = Answer 200 b Nothing

notFound :: Answer
notFound = Answer 404 "not found" Nothing

pinned :: Pin -> B.ByteString -> Answer
pinned p b = Answer 200 b (Just p)

found :: B.ByteString -> Answer
found = pinned p1

p1, p2, p101 :: Pin
p1 = Pin 100 (mkHash 'A')
p2 = Pin 100 (mkHash 'E')
p101 = Pin 101 (mkHash 'A')

-- | @?= cannot take a wildcard (ruling 4.11).
--
isDiffer :: Judgement -> Bool
isDiffer = \case
    Final (Differ _) -> True
    _ -> False

-- | The body of a @/local@ answer at a height.
--
localBody :: BlockHeight -> Char -> B.ByteString -> B.ByteString
localBody h parent dat = mconcat
    [ "{\"result\":{\"status\":\"success\",\"data\":", if B.null dat then "null" else dat, "}"
    , ",\"metaData\":{\"blockHeight\":", B8.pack (show h)
    , ",\"blockTime\":0,\"prevBlockHash\":\"", T.encodeUtf8 (hashText parent), "\"}}"
    ]

-- -------------------------------------------------------------------------- --
-- Rules

rulesTests :: TestTree
rulesTests = testGroup "rules"
    [ testCase "payload bytes" $
        assertBool "payload bytes" $ isDiffer (judge cov payloadReq (ok "A") (ok "B"))
    , testCase "found on the oracle and not on the test side: orphan check, payload class only" $ do
        judge cov payloadReq (ok "A") notFound @?= CheckOrphan payloadA
        assertBool "header class" $ isDiffer (judge cov headerReq (ok "A") notFound)
        assertBool "found on the test side only" $ isDiffer (judge cov payloadReq notFound (ok "A"))
    , testCase "pins that disagree: retry" $
        judge cov localReq (pinned p1 "x") (pinned p2 "x") @?= RetryPinMismatch
    , testCase "pins that agree off the target height: retry" $
        judge cov localReq (pinned p101 "x") (pinned p101 "x") @?= RetryPinMismatch
    , testCase "pins that agree at the target height: bytes compared" $ do
        judge cov localReq (pinned p1 "x") (pinned p1 "x") @?= Final Same
        assertBool "pinned bytes" $ isDiffer (judge cov localReq (pinned p1 "x") (pinned p1 "y"))
    , testCase "uncovered request: skipped" $ do
        judge cov uncoveredReq (ok "A") (ok "A") @?= Final (Skipped NotCovered)
        judge cov (payloadReqFor payloadA) { _reqTarget = Just 2_001 } (ok "A") (ok "B")
            @?= Final (Skipped NotCovered)
    , testCase "found vs not found" $
        assertBool "found vs not found" $ isDiffer (judge cov localReq (found "x") notFound)
    , testCase "keys order" $
        assertBool "keys order" $ isDiffer
            (judge cov keysReq (pinned p1 "[\"a\",\"b\"]") (pinned p1 "[\"b\",\"a\"]"))
    , testCase "status" $
        assertBool "status" $ isDiffer (judge cov headerReq (Answer 200 "x" Nothing) (Answer 500 "x" Nothing))
    , testCase "pinned answers without block metadata on both sides: bytes compared strictly" $ do
        judge cov localReq (Answer 400 "e" Nothing) (Answer 400 "e" Nothing) @?= Final Same
        assertBool "body" $ isDiffer (judge cov localReq (Answer 400 "e" Nothing) (Answer 400 "f" Nothing))
        assertBool "status" $ isDiffer (judge cov localReq (Answer 400 "e" Nothing) (Answer 500 "e" Nothing))
    , testCase "orphanVerdict: failed lookups and references on one side are differences" $ do
        let o = HeaderRef (hashText 'U') (toText payloadC)
            t = HeaderRef (hashText 'W') (toText payloadC)
            orphan = HeaderRef (hashText 'V') (toText payloadO)
            ov = orphanVerdict cid0 100 payloadO
            isD = \case
                Differ _ -> True
                _ -> False
        assertBool "headers lookup failed" $ isD (ov (Left "x") (Right o) (Right t))
        assertBool "oracle canonical lookup failed" $ isD (ov (Right [orphan]) (Left "x") (Right t))
        assertBool "test canonical lookup failed" $ isD (ov (Right [orphan]) (Right o) (Left "x"))
        assertBool "all lookups failed" $ isD (ov (Left "x") (Left "y") (Left "z"))
        assertBool "referenced on the oracle only" $
            isD (ov (Right [orphan]) (Right o { _hrPayloadHash = toText payloadO }) (Right t))
        assertBool "referenced on the test side only" $
            isD (ov (Right [orphan]) (Right o) (Right t { _hrPayloadHash = toText payloadO }))
        assertBool "no non-canonical header carries it" $ isD (ov (Right [o]) (Right o) (Right t))
        assertBool "no header at all" $ isD (ov (Right []) (Right o) (Right t))
        case ov (Right [o, orphan]) (Right o) (Right t) of
            Allowed AllowOrphanPayload ev -> assertBool "evidence names the orphan" (hashText 'V' `T.isInfixOf` ev)
            v -> assertFailure ("expected Allowed, got " <> show v)
    , testCase "pinned class without an API prefix: unpinned, never sent" $
        judge cov localReq { _reqPath = "/local?rewindDepth=" <> depthPlaceholder } (pinned p1 "x") (pinned p1 "x")
            @?= Final (Skipped Unpinned)
    , testCase "tipProbe: (+ 1 1) at depth 0, the network and chain of the prefix" $ do
        let (path, body) = tipProbe apiPrefix cid3 1_700_000_000
            cmdP :: Value -> Parser T.Text
            cmdP = withObject "command" (.: "cmd")
            fieldsP :: Value -> Parser (T.Text, T.Text, T.Text)
            fieldsP = withObject "payload" $ \c -> do
                n <- c .: "networkId"
                code <- c .: "payload" >>= withObject "rpc" (\x -> x .: "exec" >>= withObject "exec" (.: "code"))
                ch <- c .: "meta" >>= withObject "meta" (.: "chainId")
                return (n, code, ch)
        path @?= apiPrefix <> "/chain/3/pact/api/v1/local?rewindDepth=0"
        let fields = decodeStrict body >>= parseMaybe cmdP >>= decodeStrict . T.encodeUtf8 >>= parseMaybe fieldsP
        fields @?= Just ("test", "(+ 1 1)", "3")
    , testCase "pinned class without depth placeholder or target: unpinned" $ do
        let noDepth = localReq { _reqPath = apiPrefix <> "/chain/0/pact/api/v1/local" }
        judge cov noDepth (pinned p1 "x") (pinned p1 "x") @?= Final (Skipped Unpinned)
        judge cov localReq { _reqTarget = Nothing } (pinned p1 "x") (pinned p1 "x") @?= Final (Skipped Unpinned)
    , testCase "spv: equal bytes are Same, different proofs ask for a subject comparison" $ do
        let spvReq = (payloadReqFor payloadA) { _reqClass = ClsSpv }
        judge cov spvReq (ok "\"p\"") (ok "\"p\"") @?= Final Same
        judge cov spvReq (ok "\"p\"") (ok "\"q\"") @?= CompareSpvSubjects
        assertBool "spv status" $ isDiffer (judge cov spvReq (ok "\"p\"") (Answer 400 "e" Nothing))
    , testCase "answerPin reads the metadata of a local and of a preflight result" $ do
        answerPin (localBody 100 'A' "1") @?= Just p1
        answerPin ("{\"preflightResult\":" <> localBody 100 'E' "1" <> ",\"preflightWarnings\":[]}") @?= Just p2
        answerPin "{\"result\":{\"status\":\"failure\"},\"metaData\":null}" @?= Nothing
        answerPin "not json" @?= Nothing
    , testCase "depthFor" $ do
        depthFor 150000 149000 @?= Just 1000
        depthFor 100 101 @?= Nothing
        depthFor 100 100 @?= Just 0
    , testCase "a request file line round trips, UTF-8 and binary bodies" $ do
        decode (encode localReq) @?= Just localReq
        let bin = payloadReq { _reqMethod = methodPost, _reqBody = Just "\255\0" }
        decode (encode bin) @?= Just bin
    ]

-- -------------------------------------------------------------------------- --
-- Sampling

sampleCov :: Coverage
sampleCov = Coverage $ M.fromList [(cid0, [(0, 150_000)]), (cid3, [(0, 6_100_000)])]

specFor :: Word64 -> SampleSpec
specFor seed = SampleSpec seed 10_000 allRequestClasses sampleCov mainnetBoundaries boundaryShare
    (historyMaxHeight mainnet) Nothing

spec1, spec2 :: SampleSpec
spec1 = specFor 1
spec2 = specFor 2

absDiff :: BlockHeight -> BlockHeight -> BlockHeight
absDiff a b = if a > b then a - b else b - a

-- | The first 20 targets of seed 1, reviewed once (ruling 4.2).
--
seed1Targets :: [(RequestClass, ChainId, BlockHeight)]
seed1Targets =
    [ (ClsSpv, cid3, 5_861_873)
    , (ClsPoll, cid3, 512_230)
    , (ClsHistory, cid3, 627_497)
    , (ClsHistory, cid3, 300_921)
    , (ClsPayload, cid3, 1_398_763)
    , (ClsSpv, cid3, 309_454)
    , (ClsPoll, cid3, 2_291_314)
    , (ClsPreflight, cid3, 5_416_282)
    , (ClsPayload, cid0, 140_811)
    , (ClsHistory, cid3, 458_688)
    , (ClsPoll, cid0, 144_217)
    , (ClsSpv, cid3, 2_891_547)
    , (ClsSpv, cid3, 6_027_619)
    , (ClsPayload, cid3, 997_360)
    , (ClsHistory, cid3, 758_708)
    , (ClsHistory, cid3, 3_402_492)
    , (ClsPoll, cid3, 934_871)
    , (ClsHeader, cid3, 3_521_699)
    , (ClsHeader, cid0, 140_813)
    , (ClsLocal, cid3, 3_905_112)
    ]

sampleTests :: TestTree
sampleTests = testGroup "sampling"
    [ testCase "boundaries and boundary share" $ do
        mainnetBoundaries @?= [140_808, 852_054, 2_334_500, 5_555_698, 5_659_280, 6_027_616]
        boundaryShare @?= 0.2
    , testCase "the first 20 targets of seed 1" $
        take 20 (sampleTargets spec1) @?= seed1Targets
    , testCase "seeds 1 and 2 differ" $
        assertBool "seeds 1 and 2" $ sampleTargets spec1 /= sampleTargets spec2
    , testCase "share of targets within 5 heights of a boundary is 0.2 +- 0.02 over 10,000 targets" $ do
        let ts = sampleTargets spec1
            near (_, _, h) = any (\b -> absDiff h b <= 5) mainnetBoundaries
            share = fromIntegral (length (filter near ts)) / 10_000 :: Double
        length ts @?= 10_000
        assertBool ("share " <> show share) (abs (share - 0.2) <= 0.02)
    , testCase "every target is covered and of a requested class; history only below 5,555,698" $ do
        let ts = sampleTargets spec1 { _ssClasses = [ClsHistory, ClsPayload] }
        assertBool "covered" $ all (\(_, c, h) -> isCovered sampleCov c (Just h)) ts
        assertBool "classes" $ all (\(k, _, _) -> k `elem` [ClsHistory, ClsPayload]) ts
        assertBool "history below Pact 5" $ all (\(k, _, h) -> k /= ClsHistory || h < 5_555_698) ts
        assertBool "both classes drawn" $
            any (\(k, _, _) -> k == ClsHistory) ts && any (\(k, _, _) -> k == ClsPayload) ts
    , testCase "the History cap is the Pact 5 fork height minus 1: 5,555,697 on mainnet" $
        historyMaxHeight mainnet @?= Just 5_555_697
    , testCase "History targets follow the cap of the spec; without one, none is drawn" $ do
        let capped = sampleTargets spec1 { _ssHistoryMax = Just 1_000 }
            none = sampleTargets spec1 { _ssHistoryMax = Nothing }
        assertBool "capped" $ all (\(k, _, h) -> k /= ClsHistory || h <= 1_000) capped
        assertBool "History still drawn" $ any (\(k, _, _) -> k == ClsHistory) capped
        assertBool "no History" $ all (\(k, _, _) -> k /= ClsHistory) none
        length none @?= 10_000
    ]

-- -------------------------------------------------------------------------- --
-- Report

-- | An outcome without depths or transport failure.
--
out :: Int -> Request -> Verdict -> Maybe Answer -> Maybe Answer -> Int -> Outcome
out i r v a b n = Outcome i r v a b n Nothing Nothing

outcome :: Int -> Request -> Verdict -> Outcome
outcome i r v = out i r v Nothing Nothing 1

reportTests :: TestTree
reportTests = testGroup "report"
    [ testCase "an allowed verdict without evidence counts as a difference" $ do
        let rep = addOutcome emptyReport (outcome 0 payloadReq (Allowed AllowOrphanPayload "  "))
        reportVerdict rep @?= Fail
        reportDiffCount rep @?= 1
    , testCase "an allowed verdict with evidence passes" $ do
        let rep = addOutcome emptyReport (outcome 0 payloadReq (Allowed AllowOrphanPayload "no canonical header"))
        reportVerdict rep @?= PartialPass
        reportAllowedFor rep AllowOrphanPayload @?= 1
    , testCase "pin unstable above 1% of pinned requests voids the run" $ do
        let run k = foldl addOutcome emptyReport
                [ outcome i localReq (if i < k then Skipped PinUnstable else Same) | i <- [0 .. 199] ]
        reportVerdict (run 2) @?= PartialPass
        reportVerdict (run 3) @?= Void
        reportPinned (run 3) @?= 200
    , testCase "a run that compared nothing is void" $
        reportVerdict (addOutcome emptyReport (out 0 uncoveredReq (Skipped NotCovered) Nothing Nothing 0)) @?= Void
    , testCase "every History request ends Unpinned and every payload request is Same: Void, not Pass" $ do
        let rep = foldl addOutcome emptyReport $
                [ out i (historyReqAt 100) (Skipped Unpinned) Nothing Nothing 1 | i <- [0 .. 9] ]
                <> [ out i payloadReq Same (Just (ok "p")) (Just (ok "p")) 1 | i <- [10 .. 109] ]
        reportVerdict rep @?= Void
        assertBool "a reason names the hollow class" $
            any (\r -> requestClassText ClsHistory `T.isPrefixOf` r && "none compared" `T.isInfixOf` r) (reportVoidReasons rep)
        assertBool "the summary gives the reason" $ "  void: " `T.isInfixOf` reportSummary rep
        reportUnpinnedSent rep @?= 10
        reportSent rep ClsHistory @?= 10
    , testCase "Unpinned after sending counts toward the 1% threshold; Unpinned before sending does not" $ do
        let run k attempts = foldl addOutcome emptyReport
                [ out i localReq (if i < k then Skipped Unpinned else Same) Nothing Nothing (if i < k then attempts else 1)
                | i <- [0 .. 199]
                ]
        reportVerdict (run 2 1) @?= PartialPass
        reportVerdict (run 3 1) @?= Void
        reportVerdict (run 3 0) @?= PartialPass
    , testCase "1% split between PinUnstable and Unpinned after sending: Void" $ do
        let rep = foldl addOutcome emptyReport $
                [ out 0 localReq (Skipped PinUnstable) Nothing Nothing 6
                , out 1 localReq (Skipped Unpinned) Nothing Nothing 1
                , out 2 localReq (Skipped Unpinned) Nothing Nothing 1
                ]
                <> [ out i localReq Same Nothing Nothing 1 | i <- [3 .. 199] ]
        reportVerdict rep @?= Void
    , testCase "a class whose sent requests were all skipped voids the run, below the 1% threshold too" $ do
        let rep = foldl addOutcome emptyReport $
                out 0 (historyReqAt 100) (Skipped PinUnstable) Nothing Nothing 6
                : [ out i localReq Same Nothing Nothing 1 | i <- [1 .. 200] ]
        reportVerdict rep @?= Void
    , testCase "a class with requests in coverage but none compared voids the run, even if none was sent" $ do
        let rep = foldl addOutcome emptyReport $
                [ out i (historyReqAt 100) (Skipped Unpinned) Nothing Nothing 0 | i <- [0 .. 4] ]
                <> [ out i payloadReq Same Nothing Nothing 1 | i <- [5 .. 104] ]
        reportVerdict rep @?= Void
    , testCase "NotCovered never voids a run" $ do
        let rep = foldl addOutcome emptyReport $
                [ out i (historyReqAt 100) { _reqChain = cid3 } (Skipped NotCovered) Nothing Nothing 0 | i <- [0 .. 49] ]
                <> [ out i payloadReq Same Nothing Nothing 1 | i <- [50 .. 59] ]
        reportVerdict rep @?= PartialPass
        reportSkippedIn rep ClsHistory NotCovered @?= 50
        reportSkippedFor rep NotCovered @?= 50
        reportSent rep ClsHistory @?= 0
    , testCase "Same is split into 2xx and non-2xx per class, in the summary and the JSON" $ do
        let rep = foldl addOutcome emptyReport
                [ out 0 payloadReq Same (Just (ok "p")) (Just (ok "p")) 1
                , out 1 payloadReq Same (Just notFound) (Just notFound) 1
                , out 2 payloadReq Same (Just notFound) (Just notFound) 1
                ]
        reportSame2xx rep ClsPayload @?= 1
        reportSameNon2xx rep ClsPayload @?= 2
        let line = "  " <> requestClassText ClsPayload <> ": 1 / 2 / 0 / 0 / 0 ("
        assertBool "summary" $ line `T.isInfixOf` reportSummary rep
        assertBool "json" $ "\"sameNon2xx\":2" `B.isInfixOf` BL.toStrict (encode rep)
    , testCase "no answer from the oracle above 1% of the requests sent voids the run" $ do
        let run k = foldl addOutcome emptyReport
                [ if i < k
                    then Outcome i payloadReq (Skipped NoOracleAnswer) Nothing Nothing 1 Nothing (Just NoAnswerOracle)
                    else out i payloadReq Same Nothing Nothing 1
                | i <- [0 .. 199]
                ]
        reportVerdict (run 2) @?= PartialPass
        reportVerdict (run 3) @?= Void
        reportNoAnswer (run 3) NoAnswerOracle @?= 3
    , testCase "transport failures are counted per side, in the summary and the JSON" $ do
        let rep = foldl addOutcome emptyReport
                [ Outcome 0 payloadReq (Differ "no answer") (Just (ok "p")) Nothing 1 Nothing (Just NoAnswerTest)
                , Outcome 1 payloadReq (Skipped NoOracleAnswer) Nothing Nothing 1 Nothing (Just NoAnswerOracle)
                , Outcome 2 payloadReq (Skipped NoOracleAnswer) Nothing Nothing 1 Nothing (Just NoAnswerBoth)
                , Outcome 3 payloadReq (Skipped NoOracleAnswer) Nothing Nothing 1 Nothing (Just NoAnswerBoth)
                ]
        map (reportNoAnswer rep) [NoAnswerTest, NoAnswerOracle, NoAnswerBoth] @?= [1, 1, 2]
        reportVerdict rep @?= Fail
        assertBool "summary" $ "1 / 1 / 2" `T.isInfixOf` reportSummary rep
        assertBool "json" $ "\"both-sides\":2" `B.isInfixOf` BL.toStrict (encode rep)
    , testCase "a difference line shows the depth on each side and the first 120 bytes of the Pact code" $ do
        let code = "(coin.details \"k:00\") " <> T.replicate 200 "x"
            cmd = T.decodeUtf8 $ BL.toStrict $ encode $ object
                [ "payload" .= object [ "exec" .= object [ "code" .= code ] ] ]
            req = localReq { _reqBody = Just (BL.toStrict (encode (object [ "cmd" .= cmd, "hash" .= ("h" :: T.Text) ]))) }
            rep = addOutcome emptyReport
                (Outcome 7 req (Differ "d") (Just (ok "a")) (Just (ok "b")) 1 (Just (900, 890)) Nothing)
            summary = reportSummary rep
        assertBool "depths" $ "(depth oracle 900, test 890)" `T.isInfixOf` summary
        assertBool "code" $ ("body: " <> T.take 120 code <> "...") `T.isInfixOf` summary
        assertBool "cut at 120 bytes" $ not (T.take 121 code `T.isInfixOf` summary)
        assertBool "a plain body as is" $ "body: {\"code\":\"(coin.details" `T.isInfixOf`
            reportSummary (addOutcome emptyReport (out 0 localReq (Differ "d") Nothing Nothing 1))
        assertBool "json" $ "\"depths\":{\"oracle\":900,\"test\":890}" `B.isInfixOf` BL.toStrict (encode rep)
    , testCase "exit codes: 0 Pass, 2 Fail, 3 Void, 4 PartialPass" $ do
        gateExitCode Pass @?= ExitSuccess
        gateExitCode PartialPass @?= ExitFailure 4
        gateExitCode Fail @?= ExitFailure 2
        gateExitCode Void @?= ExitFailure 3
    , testCase "the first 50 differences by request index are kept, bodies cut to 4 KB" $ do
        let big = Answer 200 (B.replicate 10_000 0x61) Nothing
            rep = foldl addOutcome emptyReport
                [ out i payloadReq (Differ "d") (Just big) (Just notFound) 1 | i <- reverse [0 .. 59] ]
        reportDiffCount rep @?= 60
        map _dsIndex (reportDiffSamples rep) @?= [0 .. 49]
        map (fmap (B.length . _ansBody) . _dsOracle) (reportDiffSamples rep) @?= replicate 50 (Just 4_096)
        map _dsOracleBytes (reportDiffSamples rep) @?= replicate 50 10_000
        assertBool "summary" $ "verdict: Fail" `T.isInfixOf` reportSummary rep
        assertBool "json" $ "\"verdict\":\"Fail\"" `B.isInfixOf` BL.toStrict (encode rep)
    ]

-- -------------------------------------------------------------------------- --
-- Runner against stub services

data StubConfig = StubConfig
    { _scTip :: !BlockHeight
        -- ^ the metadata height of @/local@ at depth 0
    , _scParent :: !Char
        -- ^ the parent hash of every @/local@ answer
    , _scPayloads :: !(M.Map T.Text B.ByteString)
    , _scCanonicalPayload :: !BlockPayloadHash
        -- ^ the payload hash of the canonical header at every height
    , _scLocalMeta :: !Bool
        -- ^ whether @/local@ answers carry block metadata
    , _scOrphanHeaders :: ![(Char, BlockPayloadHash)]
        -- ^ non-canonical headers at every height: hash character and
        -- payload hash
    , _scProbeFailures :: !Int
        -- ^ the first tip probes answer 500 without block metadata
    , _scDataTag :: !B.ByteString
        -- ^ added to the data of every @/local@ answer if not empty
    , _scDelay :: !Int
        -- ^ microseconds before every answer
    , _scDropLocal :: !Bool
        -- ^ a @/local@ request that is not a tip probe gets no answer: the
        -- connection is closed after the request was read
    }

-- | Arrival time and path with query of every request a stub saw.
--
type Seen = IORef [(Double, T.Text)]

oracleConf :: StubConfig
oracleConf = StubConfig
    { _scTip = 1_000
    , _scParent = 'A'
    , _scPayloads = M.fromList
        [ (toText payloadA, "payload-A")
        , (toText payloadB, "payload-B")
        , (toText payloadO, "payload-O")
        ]
    , _scCanonicalPayload = payloadC
    , _scLocalMeta = True
    , _scOrphanHeaders = [('V', payloadO)]
    , _scProbeFailures = 0
    , _scDataTag = ""
    , _scDelay = 0
    , _scDropLocal = False
    }

-- | The same answers as the oracle at a different tip, without the orphan.
--
testConf :: StubConfig
testConf = oracleConf
    { _scTip = 990
    , _scPayloads = M.delete (toText payloadO) (_scPayloads oracleConf)
    }

-- | A stub service. History code (@txids@) fails at the tip, as on Pact 5.
--
stubApp :: Seen -> IORef Int -> StubConfig -> Application
stubApp seen probes sc req respond = do
    body <- BL.toStrict <$> strictRequestBody req
    t <- getMonotonicTime
    let full = T.decodeUtf8 (rawPathInfo req <> rawQueryString req)
        q k = lookup k (queryString req) >>= id >>= \v -> case reads (B8.unpack v) of
            [(n, "")] -> Just (n :: Integer)
            _ -> Nothing
        reply s b = respond $ responseLBS (mkStatus s "") [] (BL.fromStrict b)
        dat = if B.null (_scDataTag sc) then body else "[" <> body <> ",\"" <> _scDataTag sc <> "\"]"
        local d
            | _scLocalMeta sc = reply 200 (localBody (_scTip sc - fromIntegral d) (_scParent sc) dat)
            | otherwise = reply 200 "{\"result\":{\"status\":\"success\",\"data\":1},\"metaData\":null}"
    atomicModifyIORef' seen (\xs -> ((t, full) : xs, ()))
    when (_scDelay sc > 0) $ threadDelay (_scDelay sc)
    case drop 3 (pathInfo req) of
        ["cut"] -> reply 200 $ mconcat
            [ "{\"hashes\":{\"0\":{\"height\":", B8.pack (show (_scTip sc))
            , ",\"hash\":\"", T.encodeUtf8 (hashText 'Q'), "\"}}}"
            ]
        ["chain", "0", "header", "branch"] -> case q "minheight" of
            Just h -> reply 200 $ mconcat
                [ "{\"items\":[", headerJson h 'U' (_scCanonicalPayload sc), "],\"limit\":1,\"next\":null}" ]
            Nothing -> reply 400 "no minheight"
        ["chain", "0", "header"] -> case q "minheight" of
            Just h -> reply 200 $ mconcat
                [ "{\"items\":["
                , B.intercalate "," [ headerJson h c ph | (c, ph) <- ('U', _scCanonicalPayload sc) : _scOrphanHeaders sc ]
                , "],\"limit\":", B8.pack (show (1 + length (_scOrphanHeaders sc))), ",\"next\":null}"
                ]
            Nothing -> reply 400 "no minheight"
        ["chain", "0", "payload", h] -> maybe (reply 404 "not found") (reply 200) (M.lookup h (_scPayloads sc))
        ["chain", "0", "pact", "api", "v1", "local"] -> case q "rewindDepth" of
            Just d
                | "(+ 1 1)" `B.isInfixOf` body -> do
                    n <- atomicModifyIORef' probes (\k -> (k + 1, k))
                    if n < _scProbeFailures sc then reply 500 "probe failed" else local d
                | d == 0, "txids" `B.isInfixOf` body -> reply 500 "history functions are not available at the tip"
                | _scDropLocal sc -> respond $ responseRaw (\_ _ -> return ()) (responseLBS status500 [] "no raw")
                | otherwise -> local d
            Nothing -> reply 400 "no rewindDepth"
        _ -> reply 404 "no route"

headerJson :: Integer -> Char -> BlockPayloadHash -> B.ByteString
headerJson h c ph = mconcat
    [ "{\"height\":", B8.pack (show h)
    , ",\"hash\":\"", T.encodeUtf8 (hashText c)
    , "\",\"payloadHash\":\"", T.encodeUtf8 (toText ph), "\"}"
    ]

quietLogger :: GenericLogger
quietLogger = genericLogger Error (\_ -> return ())

withStubs
    :: Double
    -> StubConfig
    -> StubConfig
    -> (Side -> Side -> Seen -> Seen -> IO a)
    -> IO a
withStubs rate oc tc = withSides rate (Just oc) (Just tc)

-- | Like 'withStubs'; a side without a configuration is a closed port on
-- 127.0.0.1, so that every call to it fails without an HTTP answer.
--
withSides
    :: Double
    -> Maybe StubConfig
    -> Maybe StubConfig
    -> (Side -> Side -> Seen -> Seen -> IO a)
    -> IO a
withSides rate oc tc act = do
    so <- newIORef []
    st <- newIORef []
    hl <- newHostLimiters rate Nothing
    serve so oc $ \po ->
        serve st tc $ \pt -> do
            let uo = "http://127.0.0.1:" <> T.pack (show po)
                ut = "http://127.0.0.1:" <> T.pack (show pt)
            lo <- limiterFor hl uo
            lt <- limiterFor hl ut
            act (Side "oracle" uo lo) (Side "test" ut lt) so st

-- | Serves a stub on a free port, or finds a closed port.
--
serve :: Seen -> Maybe StubConfig -> (Int -> IO a) -> IO a
serve seen (Just sc) k = do
    probes <- newIORef 0
    testWithApplication (return (stubApp seen probes sc)) k
serve _ Nothing k = do
    closed <- testWithApplication (return (\_ respond -> respond (responseLBS status200 [] ""))) return
    k closed

-- | Short transport retries, so that tests with a closed port stay fast.
--
fastOptions :: RunOptions
fastOptions = defaultRunOptions { _roTransportAttempts = 2, _roRetryDelay = 10_000, _roProbeAttempts = 3 }

-- | Tip probes, the @/local@ requests at depth 0.
--
probeCount :: [T.Text] -> Int
probeCount = length . filter ("rewindDepth=0" `T.isSuffixOf`)

seenPaths :: Seen -> IO [T.Text]
seenPaths s = map snd <$> readIORef s

-- | @/local@ requests at a non-zero depth, i.e. not tip probes.
--
deepLocals :: [T.Text] -> Int
deepLocals = length . filter (\p -> "/local?rewindDepth=" `T.isInfixOf` p && not ("rewindDepth=0" `T.isSuffixOf` p))

runTests :: TestTree
runTests = testGroup "runCompare"
    [ testCase "limiterFor: one limiter per host, shared by two ports" $ do
        hl <- newHostLimiters 1_000 Nothing
        l1 <- limiterFor hl "http://127.0.0.1:1848"
        l2 <- limiterFor hl "http://127.0.0.1:1849/"
        l3 <- limiterFor hl "http://localhost:1848"
        throttled l1 (return ())
        n2 <- limiterPermits l2
        n3 <- limiterPermits l3
        n2 @?= 1
        n3 @?= 0

    , testCase "identical services: Pass; an uncovered request is sent to neither side" $
        withStubs 1_000 oracleConf testConf $ \o t so st -> do
            let reqs =
                    [ payloadReqFor payloadA, payloadReqFor payloadB, headerReq
                    , localReqAt 100, localReqAt 500, keysReqAt 900
                    , uncoveredReq
                    ]
            rep <- runCompare quietLogger cov o t reqs
            reportVerdict rep @?= PartialPass
            reportCompared rep @?= 6
            reportClassTotal rep KindSame @?= 6
            reportSkippedFor rep NotCovered @?= 1
            ps <- (<>) <$> seenPaths so <*> seenPaths st
            assertBool "nothing sent for chain 3" $ not (any ("/chain/3/" `T.isInfixOf`) ps)

    , testCase "one byte flipped in one payload body: Fail, naming that request" $ do
        let flipLast b = B.init b <> B.singleton (if B.last b == 0x41 then 0x40 else 0x41)
            flipped = M.adjust flipLast (toText payloadA) (_scPayloads testConf)
        withStubs 1_000 oracleConf testConf { _scPayloads = flipped } $ \o t _ _ -> do
            rep <- runCompare quietLogger cov o t
                [payloadReqFor payloadB, payloadReqFor payloadA, localReqAt 100]
            reportVerdict rep @?= Fail
            reportDiffCount rep @?= 1
            map _dsRequest (reportDiffSamples rep) @?= [payloadReqFor payloadA]
            map _dsIndex (reportDiffSamples rep) @?= [1]

    , testCase "an orphan whose hash a canonical header references: Fail" $
        withStubs 1_000 oracleConf { _scCanonicalPayload = payloadO } testConf { _scCanonicalPayload = payloadO } $ \o t _ _ -> do
            rep <- runCompare quietLogger cov o t [payloadReqFor payloadO]
            reportVerdict rep @?= Fail
            map _dsRequest (reportDiffSamples rep) @?= [payloadReqFor payloadO]

    , testCase "an orphan that no canonical header references: Allowed with evidence, Pass" $
        withStubs 1_000 oracleConf testConf $ \o t so st -> do
            rep <- runCompare quietLogger cov o t [payloadReqFor payloadO]
            reportVerdict rep @?= PartialPass
            reportAllowedFor rep AllowOrphanPayload @?= 1
            assertBool "evidence" $ all (not . T.null . _asEvidence) (reportAllowedSamples rep)
            pso <- seenPaths so
            pst <- seenPaths st
            assertBool "cut looked up on both sides" $
                any ("/cut" `T.isSuffixOf`) pso && any ("/cut" `T.isSuffixOf`) pst

    , testCase "an orphan that no non-canonical header of the oracle carries: Fail" $
        withStubs 1_000 oracleConf { _scOrphanHeaders = [('V', payloadA)] } testConf $ \o t _ _ -> do
            rep <- runCompare quietLogger cov o t [payloadReqFor payloadO]
            reportVerdict rep @?= Fail
            reportAllowedFor rep AllowOrphanPayload @?= 0

    , testCase "no answer from the test side only: a difference that keeps the oracle body" $
        withSides 1_000 (Just oracleConf) Nothing $ \o t _ _ -> do
            rep <- runCompareWith quietLogger fastOptions cov o t [payloadReqFor payloadA, localReqAt 100]
            reportVerdict rep @?= Fail
            map (fmap _ansBody . _dsOracle) (reportDiffSamples rep) @?= [Just "payload-A", Nothing]
            map (fmap _ansBody . _dsTest) (reportDiffSamples rep) @?= [Nothing, Nothing]
            reportNoAnswer rep NoAnswerTest @?= 2
            reportDiffCount rep @?= 2

    , testCase "no answer from the oracle only: Void, not Fail" $
        withSides 1_000 Nothing (Just testConf) $ \o t _ _ -> do
            rep <- runCompareWith quietLogger fastOptions cov o t [payloadReqFor payloadA, localReqAt 100]
            reportDiffCount rep @?= 0
            reportVerdict rep @?= Void
            reportSkippedFor rep NoOracleAnswer @?= 2
            reportNoAnswer rep NoAnswerOracle @?= 2

    , testCase "no answer from either side: Void, not Fail" $
        withSides 1_000 Nothing Nothing $ \o t _ _ -> do
            rep <- runCompareWith quietLogger fastOptions cov o t [payloadReqFor payloadA, localReqAt 100]
            reportDiffCount rep @?= 0
            reportVerdict rep @?= Void
            reportNoAnswer rep NoAnswerBoth @?= 2

    , testCase "the oracle's tip probe never gives block metadata: Unpinned after sending, Void" $
        withStubs 1_000 oracleConf { _scProbeFailures = maxBound } testConf $ \o t so _ -> do
            rep <- runCompareWith quietLogger fastOptions cov o t [localReqAt 100, payloadReqFor payloadA]
            reportSkippedIn rep ClsLocal Unpinned @?= 1
            reportUnpinnedSent rep @?= 1
            reportVerdict rep @?= Void
            n <- probeCount <$> seenPaths so
            n @?= 3

    , testCase "the test side's tip probe never gives block metadata: Fail" $
        withStubs 1_000 oracleConf testConf { _scProbeFailures = maxBound } $ \o t _ _ -> do
            rep <- runCompareWith quietLogger fastOptions cov o t [localReqAt 100, payloadReqFor payloadA]
            reportVerdict rep @?= Fail
            map _dsIndex (reportDiffSamples rep) @?= [0]

    , testCase "neither tip probe gives block metadata: Void" $
        withStubs 1_000 oracleConf { _scProbeFailures = maxBound } testConf { _scProbeFailures = maxBound } $ \o t _ _ -> do
            rep <- runCompareWith quietLogger fastOptions cov o t [localReqAt 100, payloadReqFor payloadA]
            reportDiffCount rep @?= 0
            reportVerdict rep @?= Void

    , testCase "a failed tip probe is tried again on its own: Pass after two failures" $
        withStubs 1_000 oracleConf { _scProbeFailures = 2 } testConf $ \o t so _ -> do
            rep <- runCompareWith quietLogger fastOptions cov o t [localReqAt 100]
            reportVerdict rep @?= PartialPass
            n <- probeCount <$> seenPaths so
            n @?= 3

    , testCase "a History request whose code fails at the tip is pinned by the class-neutral probe and compared" $
        withStubs 1_000 oracleConf testConf $ \o t _ _ -> do
            rep <- runCompare quietLogger cov o t [historyReqAt 100]
            reportVerdict rep @?= PartialPass
            reportSame2xx rep ClsHistory @?= 1

    , testCase "a pinned difference names the depth used on each side" $
        withStubs 1_000 oracleConf testConf { _scDataTag = "x" } $ \o t _ _ -> do
            rep <- runCompare quietLogger cov o t [localReqAt 100]
            reportVerdict rep @?= Fail
            map _dsDepths (reportDiffSamples rep) @?= [Just (900, 890)]
            assertBool "summary" $ "(depth oracle 900, test 890)" `T.isInfixOf` reportSummary rep

    , testCase "pins that never agree: 5 retries, then Skipped PinUnstable" $
        withStubs 1_000 oracleConf testConf { _scParent = 'E' } $ \o t so st -> do
            rep <- runCompare quietLogger cov o t [localReqAt 500]
            reportSkippedFor rep PinUnstable @?= 1
            reportVerdict rep @?= Void
            no <- deepLocals <$> seenPaths so
            nt <- deepLocals <$> seenPaths st
            (no, nt) @?= (6, 6)

    , testCase "a test side whose /local answers carry no block metadata: Fail, not skipped" $
        withStubs 1_000 oracleConf testConf { _scLocalMeta = False } $ \o t _ _ -> do
            rep <- runCompare quietLogger cov o t [localReqAt 100, keysReqAt 200]
            reportVerdict rep @?= Fail
            reportDiffCount rep @?= 2

    , testCase "no answer within the response timeout: each request is sent once, never again" $
        withStubs 1_000 oracleConf { _scDelay = 1_000_000 } testConf $ \o t so _ -> do
            let opts = defaultRunOptions
                    { _roTransportAttempts = 5, _roRetryDelay = 10_000
                    , _roResponseTimeout = 200_000, _roOracleLocalTimeout = 200_000
                    }
            rep <- runCompareWith quietLogger opts cov o t [payloadReqFor payloadA, localReqAt 100]
            ps <- seenPaths so
            -- the payload request once, and the tip probe of the /local request once
            assertEqual ("oracle arrivals: " <> show ps) 2 (length ps)
            reportSkippedFor rep NoOracleAnswer @?= 2
            reportVerdict rep @?= Void

    , testCase "a connection closed after the request, without an answer: the request is sent once, never again" $
        withStubs 1_000 oracleConf { _scDropLocal = True } testConf $ \o t so _ -> do
            let opts = defaultRunOptions { _roTransportAttempts = 5, _roRetryDelay = 10_000 }
            rep <- runCompareWith quietLogger opts cov o t [localReqAt 100]
            n <- deepLocals <$> seenPaths so
            n @?= 1
            reportSkippedFor rep NoOracleAnswer @?= 1
            reportVerdict rep @?= Void

    , testCase "the oracle's /local waits 30 s by default, other calls 120 s; a slow /local is not sent again" $ do
        _roOracleLocalTimeout defaultRunOptions @?= 30_000_000
        _roResponseTimeout defaultRunOptions @?= 120_000_000
        withStubs 1_000 oracleConf { _scDelay = 500_000 } testConf $ \o t so _ -> do
            let opts = defaultRunOptions
                    { _roRetryDelay = 10_000, _roResponseTimeout = 10_000_000, _roOracleLocalTimeout = 100_000 }
            rep <- runCompareWith quietLogger opts cov o t [localReqAt 100, payloadReqFor payloadA]
            reportSkippedIn rep ClsLocal NoOracleAnswer @?= 1
            reportCount rep ClsPayload KindSame @?= 1
            n <- probeCount <$> seenPaths so
            n @?= 1

    , testCase "only a call that never reached the server is tried again" $ do
        let failure = HTTP.HttpExceptionRequest HTTP.defaultRequest
        map (isPreSendFailure . failure)
            [ HTTP.ConnectionFailure (toException (userError "refused"))
            , HTTP.ConnectionTimeout
            , HTTP.ResponseTimeout
            , HTTP.NoResponseDataReceived
            , HTTP.ConnectionClosed
            , HTTP.IncompleteHeaders
            , HTTP.InternalException (toException (userError "reset"))
            ]
            @?= [True, True, False, False, False, False, False]

    , testCase "both sides on one host at rate r: at most r requests per second in total, probes and orphan checks included" $ do
        let rate = 10 :: Double
        withStubs rate oracleConf testConf $ \o t so st -> do
            rep <- runCompare quietLogger cov o t
                [ localReqAt 100, localReqAt 200, keysReqAt 300
                , payloadReqFor payloadA, payloadReqFor payloadB, payloadReqFor payloadO
                ]
            reportVerdict rep @?= PartialPass
            arrivals <- (<>) <$> readIORef so <*> readIORef st
            permitsO <- limiterPermits (_sideLimiter o)
            permitsT <- limiterPermits (_sideLimiter t)
            permitsO @?= permitsT
            fromIntegral (length arrivals) @?= permitsO
            let ps = map snd arrivals
            assertBool "tip probes seen" $ any ("rewindDepth=0" `T.isSuffixOf`) ps
            assertBool "orphan checks seen" $ any ("/cut" `T.isSuffixOf`) ps
            let ts = map fst arrivals
                window s = length (filter (\x -> x >= s && x < s + 1) ts)
                worst = maximum (map window ts)
            assertBool ("at most r + 1 arrivals in any second, saw " <> show worst) (worst <= 11)
            assertBool ("enough requests to test the rate, saw " <> show (length ts)) (length ts >= 15)
    ]
