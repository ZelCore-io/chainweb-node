{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.StorageTools.Compare.Report
--
-- The report of a @cw-compare@ run and the gate verdict it implies.
--
-- The report holds counts, not answers: counts per request class and verdict,
-- every allowed difference with its rule and evidence, skipped requests per
-- reason, and the first 50 differences (by request index) with both bodies
-- truncated to 4 KB.
--
module Chainweb.StorageTools.Compare.Report
( -- * Outcomes
  Outcome(..)
, NoAnswer(..)
, effectiveVerdict

  -- * Report
, Report
, VerdictKind(..)
, DiffSample(..)
, AllowedSample(..)
, emptyReport
, addOutcome
, reportCount
, reportClassTotal
, reportSkippedFor
, reportSkippedIn
, reportSent
, reportSentTotal
, reportComparedIn
, reportSame2xx
, reportSameNon2xx
, reportAllowedFor
, reportDiffCount
, reportDiffSamples
, reportAllowedSamples
, reportPinned
, reportPinUnstable
, reportUnpinnedSent
, reportNoAnswer
, reportInconclusive
, reportCompared
, reportPactFailures
, reportExpectClasses
, reportExpectEntries
, reportEntrySuccess
, reportHollowEntries
, reportGeneration
, reportGenTargets
, reportGenSkipped
, reportGenSkippedKind
, reportGenSkippedTotal
, reportRealKeyCompared
, reportSettings
, maxDiffSamples
, maxBodyBytes

  -- * Gate verdict
, GateVerdict(..)
, reportVerdict
, reportVoidReasons
, reportMissingClasses
, gateExitCode

  -- * Output
, reportSummary
, writeReport
) where

import Control.Applicative ((<|>))
import Data.Aeson
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import Data.Aeson.Types (parseMaybe)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Encoding.Error as T
import Data.Word (Word64)

import System.Exit (ExitCode(..))

-- internal modules

import Chainweb.StorageTools.Compare.Types

-- -------------------------------------------------------------------------- --
-- Outcomes

-- | The outcome of one request.
--
data Outcome = Outcome
    { _outIndex :: !Int
        -- ^ position of the request in the input
    , _outRequest :: !Request
    , _outVerdict :: !Verdict
    , _outOracle :: !(Maybe Answer)
    , _outTest :: !(Maybe Answer)
    , _outAttempts :: !Int
        -- ^ how often the request was sent to the sides; 0 if it was decided
        -- before anything was sent
    , _outDepths :: !(Maybe (Word64, Word64))
        -- ^ the rewind depths used on the oracle and on the test side, for a
        -- pinned request that was sent
    , _outNoAnswer :: !(Maybe NoAnswer)
        -- ^ which side gave no HTTP answer, after all attempts
    }

-- | A transport failure: no HTTP answer after all attempts. On the test side
-- only, the request is a difference ('Differ') and the oracle's answer is
-- kept. On the oracle, or on both sides, it is 'Skipped' 'NoOracleAnswer' and
-- counts toward 'Void'.
--
data NoAnswer = NoAnswerTest | NoAnswerOracle | NoAnswerBoth
    deriving (Show, Eq, Ord, Enum, Bounded)

noAnswerText :: NoAnswer -> T.Text
noAnswerText = \case
    NoAnswerTest -> "test-side-only"
    NoAnswerOracle -> "oracle-only"
    NoAnswerBoth -> "both-sides"

-- | An 'Allowed' verdict without evidence counts as 'Differ'.
--
effectiveVerdict :: Verdict -> Verdict
effectiveVerdict = \case
    Allowed rule ev | T.null (T.strip ev) ->
        Differ $ "allowed by " <> allowRuleText rule <> " without evidence, counted as a difference"
    v -> v

-- -------------------------------------------------------------------------- --
-- Report

data VerdictKind = KindSame | KindAllowed | KindDiffer | KindSkipped
    deriving (Show, Eq, Ord, Enum, Bounded)

verdictKindText :: VerdictKind -> T.Text
verdictKindText = \case
    KindSame -> "same"
    KindAllowed -> "allowed"
    KindDiffer -> "differ"
    KindSkipped -> "skipped"

data DiffSample = DiffSample
    { _dsIndex :: !Int
    , _dsRequest :: !Request
    , _dsDetail :: !T.Text
    , _dsOracle :: !(Maybe Answer)
        -- ^ body truncated to 'maxBodyBytes'
    , _dsTest :: !(Maybe Answer)
        -- ^ body truncated to 'maxBodyBytes'
    , _dsOracleBytes :: !Int
        -- ^ length of the full body
    , _dsTestBytes :: !Int
    , _dsAttempts :: !Int
    , _dsDepths :: !(Maybe (Word64, Word64))
    }

data AllowedSample = AllowedSample
    { _asIndex :: !Int
    , _asRequest :: !Request
    , _asRule :: !AllowRule
    , _asEvidence :: !T.Text
    , _asDepths :: !(Maybe (Word64, Word64))
    }

data Report = Report
    { _repCounts :: !(M.Map (RequestClass, VerdictKind) Int)
    , _repSameNon2xx :: !(M.Map RequestClass Int)
        -- ^ 'Same' answers whose status is not 2xx
    , _repSkipped :: !(M.Map (RequestClass, SkipReason) Int)
    , _repSent :: !(M.Map RequestClass Int)
        -- ^ requests sent to the sides at least once
    , _repAllowed :: !(M.Map Int AllowedSample)
        -- ^ every allowed difference
    , _repDiffCount :: !Int
    , _repDiffs :: !(M.Map Int DiffSample)
        -- ^ the differences with the lowest request indices
    , _repPinned :: !Int
        -- ^ requests of a pinned class that were sent
    , _repPinUnstable :: !Int
    , _repUnpinnedSent :: !Int
        -- ^ 'Unpinned' requests that were sent (a tip probe or the request)
    , _repNoAnswer :: !(M.Map NoAnswer Int)
    , _repPactFailures :: !(M.Map RequestClass Int)
        -- ^ oracle answers 200 with Pact status @failure@, see
        -- 'reportPactFailures'
    , _repExpected :: ![RequestClass]
        -- ^ see 'reportExpectClasses'
    , _repGenTargets :: !Int
        -- ^ see 'reportGeneration'
    , _repGenSkipped :: !(M.Map (RequestClass, T.Text) Int)
        -- ^ per class and kind, see 'reportGeneration'
    , _repSettings :: ![(T.Text, T.Text)]
        -- ^ see 'reportSettings'
    , _repRealKey :: !(M.Map RequestClass Int)
        -- ^ see 'reportRealKeyCompared'
    , _repEntrySuccess :: !(M.Map (RequestClass, T.Text) Int)
        -- ^ see 'reportEntrySuccess'
    , _repExpectedEntries :: ![(RequestClass, T.Text)]
        -- ^ see 'reportExpectEntries'
    }

-- | The number of differences kept with their bodies.
--
maxDiffSamples :: Int
maxDiffSamples = 50

-- | The number of bytes of a body kept in a difference sample.
--
maxBodyBytes :: Int
maxBodyBytes = 4_096

emptyReport :: Report
emptyReport = Report mempty mempty mempty mempty mempty 0 mempty 0 0 0 mempty mempty [] 0 mempty [] mempty mempty []

-- | Adds the outcome of a request. The result does not depend on the order
-- in which outcomes are added.
--
addOutcome :: Report -> Outcome -> Report
addOutcome rep0 o = case v of
    Same -> (counted KindSame rep)
        { _repSameNon2xx = if is2xx then _repSameNon2xx rep else M.insertWith (+) cls 1 (_repSameNon2xx rep)
        , _repEntrySuccess = case _reqEntry req of
            Just e | pactSuccess -> M.insertWith (+) (cls, e) 1 (_repEntrySuccess rep)
            _ -> _repEntrySuccess rep
        }
    Allowed rule ev -> (counted KindAllowed rep)
        { _repAllowed = M.insert idx (AllowedSample idx req rule ev (_outDepths o)) (_repAllowed rep) }
    Differ detail -> (counted KindDiffer rep)
        { _repDiffCount = _repDiffCount rep + 1
        , _repDiffs = keepLowest $ M.insert idx (sample detail) (_repDiffs rep)
        }
    Skipped reason -> (counted KindSkipped rep)
        { _repSkipped = M.insertWith (+) (cls, reason) 1 (_repSkipped rep)
        , _repPinUnstable = _repPinUnstable rep + if reason == PinUnstable then 1 else 0
        , _repUnpinnedSent = _repUnpinnedSent rep + if reason == Unpinned && sent then 1 else 0
        }
  where
    rep = rep0
        { _repNoAnswer = maybe id (\na -> M.insertWith (+) na 1) (_outNoAnswer o) (_repNoAnswer rep0)
        , _repPactFailures = if pactFailure then M.insertWith (+) cls 1 (_repPactFailures rep0) else _repPactFailures rep0
        }
    pactFailure = pactStatusIs "failure"
    pactSuccess = pactStatusIs "success"
    pactStatusIs st = isPinnedClass cls && case _outOracle o of
        Just a -> _ansStatus a == 200 && pactResultStatus (_ansBody a) == Just st
        Nothing -> False
    v = effectiveVerdict (_outVerdict o)
    idx = _outIndex o
    req = _outRequest o
    cls = _reqClass req
    sent = _outAttempts o > 0
    pinnedSent = if isPinnedClass cls && sent then 1 else 0

    -- both answers are equal for 'Same'; without an answer it is not 2xx
    is2xx = maybe False (\a -> _ansStatus a >= 200 && _ansStatus a < 300) (_outOracle o)

    counted k r = r
        { _repCounts = M.insertWith (+) (cls, k) 1 (_repCounts r)
        , _repPinned = _repPinned r + pinnedSent
        , _repSent = if sent then M.insertWith (+) cls 1 (_repSent r) else _repSent r
        , _repRealKey = if k /= KindSkipped && realKey then M.insertWith (+) cls 1 (_repRealKey r) else _repRealKey r
        }

    -- the oracle's answer holds the result of a real request key
    realKey = case (cls, _outOracle o) of
        (ClsPoll, Just a) -> _ansStatus a == 200
            && maybe False (not . KM.null) (decodeStrict (_ansBody a) :: Maybe Object)
        (ClsSpv, Just a) -> _ansStatus a >= 200 && _ansStatus a < 300
        _ -> False

    keepLowest m
        | M.size m > maxDiffSamples = M.deleteMax m
        | otherwise = m

    sample detail = DiffSample
        { _dsIndex = idx
        , _dsRequest = req
        , _dsDetail = detail
        , _dsOracle = truncateAnswer <$> _outOracle o
        , _dsTest = truncateAnswer <$> _outTest o
        , _dsOracleBytes = maybe 0 (B.length . _ansBody) (_outOracle o)
        , _dsTestBytes = maybe 0 (B.length . _ansBody) (_outTest o)
        , _dsAttempts = _outAttempts o
        , _dsDepths = _outDepths o
        }

    truncateAnswer a = a { _ansBody = B.take maxBodyBytes (_ansBody a) }

reportCount :: Report -> RequestClass -> VerdictKind -> Int
reportCount rep c k = M.findWithDefault 0 (c, k) (_repCounts rep)

reportClassTotal :: Report -> VerdictKind -> Int
reportClassTotal rep k = sum [ n | ((_, k'), n) <- M.toList (_repCounts rep), k' == k ]

-- | Skipped requests for a reason, across classes.
--
reportSkippedFor :: Report -> SkipReason -> Int
reportSkippedFor rep r = sum [ n | ((_, r'), n) <- M.toList (_repSkipped rep), r' == r ]

-- | Skipped requests of a class for a reason.
--
reportSkippedIn :: Report -> RequestClass -> SkipReason -> Int
reportSkippedIn rep c r = M.findWithDefault 0 (c, r) (_repSkipped rep)

-- | Requests of a class that were sent to the sides at least once.
--
reportSent :: Report -> RequestClass -> Int
reportSent rep c = M.findWithDefault 0 c (_repSent rep)

reportSentTotal :: Report -> Int
reportSentTotal = sum . M.elems . _repSent

-- | 'Same' answers of a class with a 2xx status.
--
reportSame2xx :: Report -> RequestClass -> Int
reportSame2xx rep c = reportCount rep c KindSame - reportSameNon2xx rep c

-- | 'Same' answers of a class with any other status, an error both sides
-- returned alike.
--
reportSameNon2xx :: Report -> RequestClass -> Int
reportSameNon2xx rep c = M.findWithDefault 0 c (_repSameNon2xx rep)

reportAllowedFor :: Report -> AllowRule -> Int
reportAllowedFor rep r = length [ () | a <- M.elems (_repAllowed rep), _asRule a == r ]

reportDiffCount :: Report -> Int
reportDiffCount = _repDiffCount

-- | The first 'maxDiffSamples' differences, by request index.
--
reportDiffSamples :: Report -> [DiffSample]
reportDiffSamples = M.elems . _repDiffs

reportAllowedSamples :: Report -> [AllowedSample]
reportAllowedSamples = M.elems . _repAllowed

reportPinned :: Report -> Int
reportPinned = _repPinned

reportPinUnstable :: Report -> Int
reportPinUnstable = _repPinUnstable

-- | 'Unpinned' requests that were sent, a tip probe or the request itself.
--
reportUnpinnedSent :: Report -> Int
reportUnpinnedSent = _repUnpinnedSent

-- | Requests for which a side gave no HTTP answer after all attempts.
--
reportNoAnswer :: Report -> NoAnswer -> Int
reportNoAnswer rep na = M.findWithDefault 0 na (_repNoAnswer rep)

-- | The inconclusive outcomes of a run, which share one budget of 1% of the
-- requests sent (ruling (b) on the Task 5 re-review): 'PinUnstable',
-- 'Unpinned' after sending, 'NoOracleAnswer', and the targets skipped at
-- generation ('reportGeneration').
--
reportInconclusive :: Report -> Int
reportInconclusive rep = sum (map snd (inconclusiveKinds rep))

-- | The kinds of 'reportInconclusive' with their counts.
--
inconclusiveKinds :: Report -> [(T.Text, Int)]
inconclusiveKinds rep =
    [ ("pin unstable", _repPinUnstable rep)
    , ("unpinned after sending", _repUnpinnedSent rep)
    , ("no oracle answer", reportSkippedFor rep NoOracleAnswer)
    , ("skipped at generation", reportGenSkippedTotal rep)
    ]

-- | The number of requests whose answers were judged: same, allowed or
-- different.
--
reportCompared :: Report -> Int
reportCompared rep = sum
    [ reportClassTotal rep KindSame
    , reportClassTotal rep KindAllowed
    , reportClassTotal rep KindDiffer
    ]

-- | The number of requests of a class whose answers were judged.
--
reportComparedIn :: Report -> RequestClass -> Int
reportComparedIn rep c = sum [ reportCount rep c k | k <- [KindSame, KindAllowed, KindDiffer] ]

-- | Requests of a class within the coverage: all but 'NotCovered'.
--
inScope :: Report -> RequestClass -> Int
inScope rep c = sum [ reportCount rep c k | k <- [minBound .. maxBound] ] - reportSkippedIn rep c NotCovered

-- | The answers of the oracle with status 200 whose Pact result status is
-- @failure@, in a class of @/local@ requests ('isPinnedClass': local,
-- history, preflight). For the reader of the report only: the comparison
-- stays byte for byte, so two equal failures are 'Same'.
--
reportPactFailures :: Report -> RequestClass -> Int
reportPactFailures rep c = M.findWithDefault 0 c (_repPactFailures rep)

-- | The classes of a run of generated requests (@--classes@). Each of them
-- must have a request compared, or the run is 'Void', even when it has no
-- request at all (for example, History on a range above the last Pact 4
-- height).
--
reportExpectClasses :: [RequestClass] -> Report -> Report
reportExpectClasses cs rep = rep { _repExpected = cs }

-- | The Pact set entries of a generated run that expect success, per class
-- of the run: @[(class, entry name)]@. Each needs at least one 'Same' answer
-- with Pact result status success ('reportEntrySuccess'), or the run is at
-- best 'PartialPass': an entry whose answers are all the same error (a gas
-- error, reads not allowed, a missing table) compares nothing.
--
reportExpectEntries :: [(RequestClass, T.Text)] -> Report -> Report
reportExpectEntries es rep = rep { _repExpectedEntries = es }

-- | The 'Same' answers of a class and Pact set entry ('_reqEntry') with
-- status 200 and Pact result status success.
--
reportEntrySuccess :: Report -> RequestClass -> T.Text -> Int
reportEntrySuccess rep c e = M.findWithDefault 0 (c, e) (_repEntrySuccess rep)

-- | The entries of 'reportExpectEntries' without a 'Same' success.
--
reportHollowEntries :: Report -> [(RequestClass, T.Text)]
reportHollowEntries rep = [ x | x@(c, e) <- _repExpectedEntries rep, reportEntrySuccess rep c e == 0 ]

entryText :: (RequestClass, T.Text) -> T.Text
entryText (c, e) = requestClassText c <> ":" <> e

-- | @reportGeneration targets skipped@: of the @targets@ sampled for a
-- generated run, the class and kind of those dropped at generation, for
-- instance because an oracle lookup failed (ruling on Task 6a) or because a
-- poll or SPV target has no transaction within 1000 blocks (ruling C3 on Task
-- 6). More than 1% of the targets dropped makes the run 'Void'.
--
reportGeneration :: Int -> [(RequestClass, T.Text)] -> Report -> Report
reportGeneration n skipped rep = rep
    { _repGenTargets = n
    , _repGenSkipped = M.fromListWith (+) [ (s, 1) | s <- skipped ]
    }

-- | The targets sampled for a generated run, see 'reportGeneration'.
--
reportGenTargets :: Report -> Int
reportGenTargets = _repGenTargets

-- | The targets of a class dropped at generation, see 'reportGeneration'.
--
reportGenSkipped :: Report -> RequestClass -> Int
reportGenSkipped rep c = sum [ n | ((c', _), n) <- M.toList (_repGenSkipped rep), c' == c ]

-- | The targets dropped at generation with a kind, over all classes.
--
reportGenSkippedKind :: Report -> T.Text -> Int
reportGenSkippedKind rep k = sum [ n | ((_, k'), n) <- M.toList (_repGenSkipped rep), k' == k ]

-- | The compared requests of a class whose oracle answer holds the result of
-- a real request key: for @/poll@ a 200 answer with at least one key, for
-- @/spv@ a 2xx answer (a proof). An answer to unknown keys only (@{}@, or
-- the same error) is the same on any two nodes, so a class of them is hollow
-- (ruling C3 on Task 6). 0 for the other classes.
--
reportRealKeyCompared :: Report -> RequestClass -> Int
reportRealKeyCompared rep c = M.findWithDefault 0 c (_repRealKey rep)

reportGenSkippedTotal :: Report -> Int
reportGenSkippedTotal = sum . _repGenSkipped

-- | Settings of the run that change answers, for the reader of the report:
-- for instance the Pact @/local@ settings of an in-process test side, which
-- must be those of the oracle.
--
reportSettings :: [(T.Text, T.Text)] -> Report -> Report
reportSettings kvs rep = rep { _repSettings = kvs }

-- | The status in @result@ (plain @/local@) or @preflightResult.result@
-- (preflight) of a Pact answer.
--
pactResultStatus :: B.ByteString -> Maybe T.Text
pactResultStatus b = decodeStrict b >>= parseMaybe (withObject "answer" $ \o ->
    (o .: "result" >>= status)
    <|> (o .: "preflightResult" >>= withObject "preflightResult" (\p -> p .: "result" >>= status)))
  where
    status = withObject "result" (.: "status")

-- -------------------------------------------------------------------------- --
-- Gate verdict

data GateVerdict
    = Pass
    | PartialPass
        -- ^ no difference, but the run does not cover every class (ruling I1
        -- on Task 6): it cannot pass the gate
    | Fail
    | Void
        -- ^ the run says nothing; rerun it
    deriving (Show, Eq)

-- | The verdict of the gate.
--
-- * 'Fail' on any difference, including an allowed one without evidence.
-- * Otherwise 'Void' if any of 'reportVoidReasons' holds.
-- * Otherwise 'PartialPass' if not every one of the 7 classes has a 'Same'
--   2xx answer or an 'Allowed' difference, with a real key for poll and spv
--   ('reportRealKeyCompared'): a run of fewer classes cannot pass the gate
--   (ruling I1 on Task 6), nor can one whose class only ever got the same
--   error (Minor 3 of the P0 review).
-- * Otherwise 'PartialPass' if a Pact set entry that expects success has no
--   'Same' success ('reportHollowEntries').
-- * 'Pass' otherwise.
--
reportVerdict :: Report -> GateVerdict
reportVerdict rep
    | _repDiffCount rep > 0 = Fail
    | not (null (reportVoidReasons rep)) = Void
    | not (null (reportMissingClasses rep)) = PartialPass
    | not (null (reportHollowEntries rep)) = PartialPass
    | otherwise = Pass

-- | The classes that keep a run without differences from 'Pass': those
-- without a 'Same' 2xx answer or an 'Allowed' difference (identical errors
-- do not cover a class), and poll and spv without a request with a real key
-- compared.
--
reportMissingClasses :: Report -> [RequestClass]
reportMissingClasses rep = filter (not . covers) allRequestClasses
  where
    covers c
        | c `elem` [ClsPoll, ClsSpv] = reportRealKeyCompared rep c > 0
        | otherwise = reportSame2xx rep c > 0 || reportCount rep c KindAllowed > 0

-- | Why the run says nothing and must be rerun; empty if it does say
-- something:
--
-- * no request was compared at all;
-- * a class has requests within the coverage, but none of them was
--   compared: a hollow class must not pass ('NotCovered' requests never
--   count here);
-- * a class of a generated run ('reportExpectClasses') has no request
--   compared, even if it has no request at all;
-- * poll or spv is a class of a generated run, but no request of it with a
--   real key was compared ('reportRealKeyCompared', ruling C3 on Task 6);
-- * the inconclusive outcomes ('reportInconclusive': 'PinUnstable',
--   'Unpinned' after sending, 'NoOracleAnswer', and the targets dropped at
--   generation) are together more than 1% of the requests sent. One budget
--   for all of them, as spec 10.1 states the 1% as one fraction (ruling (b)
--   on the Task 5 re-review); the report keeps the count of each kind.
--
reportVoidReasons :: Report -> [T.Text]
reportVoidReasons rep = concat
    [ [ "no request was compared" | reportCompared rep == 0 ]
    , [ requestClassText c <> ": " <> sh n <> " requests within the coverage, none compared"
      | c <- allRequestClasses
      , let n = inScope rep c
      , n > 0
      , reportComparedIn rep c == 0
      ]
    , [ requestClassText c <> ": a class of the run (--classes) without a request compared"
      | c <- allRequestClasses
      , c `elem` _repExpected rep
      , inScope rep c == 0
      , reportComparedIn rep c == 0
      ]
    , [ requestClassText c <> ": a class of the run (--classes) without a request with a real key compared, of "
        <> sh (reportComparedIn rep c) <> " compared"
      | c <- [ClsPoll, ClsSpv]
      , c `elem` _repExpected rep
      , reportComparedIn rep c > 0
      , reportRealKeyCompared rep c == 0
      ]
    , [ sh inc <> " inconclusive of " <> sh sent <> " requests sent, more than 1%: " <> inconclusiveText rep
      | inc > 0
      , 100 * inc > sent
      ]
    ]
  where
    sent = reportSentTotal rep
    inc = reportInconclusive rep
    sh :: Int -> T.Text
    sh = T.pack . show

-- | The inconclusive outcomes per kind, the skips at generation per kind
-- too.
--
inconclusiveText :: Report -> T.Text
inconclusiveText rep = T.intercalate ", " [ k <> " " <> T.pack (show n) | (k, n) <- inconclusiveKinds rep ]
    <> if reportGenSkippedTotal rep > 0 then " (" <> genSkipKinds rep <> ")" else ""

-- | The skips at generation per kind, for example @no tx within 1000 blocks:
-- 3, an oracle lookup failed: 1@.
--
genSkipKinds :: Report -> T.Text
genSkipKinds rep = T.intercalate ", "
    [ k <> ": " <> T.pack (show n) | (k, n) <- M.toList (M.mapKeysWith (+) snd (_repGenSkipped rep)) ]

-- | The exit code of @cw-compare@ for a gate verdict: 0 'Pass', 2 'Fail',
-- 3 'Void', 4 'PartialPass'. Exit code 1 is left to bad arguments and crashes, so that a
-- crash is never read as a verdict.
--
gateExitCode :: GateVerdict -> ExitCode
gateExitCode = \case
    Pass -> ExitSuccess
    Fail -> ExitFailure 2
    Void -> ExitFailure 3
    PartialPass -> ExitFailure 4

-- -------------------------------------------------------------------------- --
-- Output

lenient :: B.ByteString -> T.Text
lenient = T.decodeUtf8With T.lenientDecode

-- | A request on one line: class, chain, target, method, path, the rewind
-- depth used on each side, and the first 'excerptBytes' bytes of the body
-- (of the Pact code, for a @/local@ command).
--
requestLine :: Request -> Maybe (Word64, Word64) -> T.Text
requestLine r ds = T.unwords $
    [ requestClassText (_reqClass r)
    , "chain " <> T.pack (show (_reqChain r))
    , maybe "no target" (\h -> "at " <> T.pack (show h)) (_reqTarget r)
    , lenient (_reqMethod r)
    , _reqPath r
    ]
    <> [ "(depth oracle " <> T.pack (show dO) <> ", test " <> T.pack (show dT) <> ")" | Just (dO, dT) <- [ds] ]
    <> [ "body: " <> bodyExcerpt b | Just b <- [_reqBody r] ]

-- | The bytes of a request body shown in the text summary.
--
excerptBytes :: Int
excerptBytes = 120

-- | The Pact code of a command (@cmd@, then @payload.exec.code@), or the
-- body itself, cut to 'excerptBytes' bytes.
--
bodyExcerpt :: B.ByteString -> T.Text
bodyExcerpt b = cut $ maybe b T.encodeUtf8 (decodeStrict' b >>= code)
  where
    cut x = lenient (B.take excerptBytes x) <> if B.length x > excerptBytes then "..." else ""
    code :: Value -> Maybe T.Text
    code v = do
        Object o <- pure v
        String cmd <- KM.lookup "cmd" o
        Object c <- decodeStrict' (T.encodeUtf8 cmd)
        Object pl <- KM.lookup "payload" c
        Object ex <- KM.lookup "exec" pl
        String t <- KM.lookup "code" ex
        return t

-- | A text summary: the gate verdict, counts per class and verdict, allowed
-- differences per rule with their evidence, skipped requests per reason, and
-- the first differences with both bodies (truncated to 4 KB).
--
reportSummary :: Report -> T.Text
reportSummary rep = T.unlines $
    [ "verdict: " <> T.pack (show (reportVerdict rep))
    ]
    <> [ "  void: " <> r | reportVerdict rep == Void, r <- reportVoidReasons rep ]
    <> [ "  partial: no same 2xx or allowed answer (for poll and spv: no request with a real key) in "
        <> T.intercalate ", " (map requestClassText (reportMissingClasses rep))
       | reportVerdict rep == PartialPass
       , not (null (reportMissingClasses rep))
       ]
    <> [ "  partial: no same Pact success for the entries that expect one: "
        <> T.intercalate ", " (map entryText (reportHollowEntries rep))
       | reportVerdict rep == PartialPass
       , not (null (reportHollowEntries rep))
       ]
    <> [ "generation: " <> sh (_repGenTargets rep) <> " targets, "
        <> sh (reportGenSkippedTotal rep) <> " skipped"
        <> (if reportGenSkippedTotal rep > 0 then " (" <> genSkipKinds rep <> ")" else "")
        <> T.concat [ ", " <> requestClassText c <> " " <> sh n | (c, n) <- M.toList (M.mapKeysWith (+) fst (_repGenSkipped rep)) ]
       | _repGenTargets rep > 0
       ]
    <> [ "settings: " <> T.intercalate ", " [ k <> "=" <> v | (k, v) <- _repSettings rep ]
       | not (null (_repSettings rep))
       ]
    <> [ "compared: " <> sh (reportCompared rep)
        <> ", differences: " <> sh (_repDiffCount rep)
        <> ", sent: " <> sh (reportSentTotal rep)
        <> ", pinned requests sent: " <> sh (_repPinned rep)
        <> ", pin unstable: " <> sh (_repPinUnstable rep)
        <> ", unpinned after sending: " <> sh (_repUnpinnedSent rep)
    , "inconclusive: " <> sh (reportInconclusive rep) <> " of " <> sh (reportSentTotal rep)
        <> " sent (budget 1%): " <> inconclusiveText rep
    , "no HTTP answer (" <> T.intercalate ", " (map noAnswerText [minBound .. maxBound]) <> "): "
        <> T.intercalate " / " [ sh (reportNoAnswer rep na) | na <- [minBound .. maxBound] ]
    , ""
    , "per class: same 2xx / same non-2xx / allowed / differ / skipped (" <> T.intercalate ", " (map skipReasonText [minBound .. maxBound]) <> ") / sent"
    ]
    <> [ "  " <> requestClassText c <> ": " <> T.intercalate " / "
            [ sh (reportSame2xx rep c)
            , sh (reportSameNon2xx rep c)
            , sh (reportCount rep c KindAllowed)
            , sh (reportCount rep c KindDiffer)
            , sh (reportCount rep c KindSkipped)
                <> " (" <> T.intercalate ", " [ sh (reportSkippedIn rep c r) | r <- [minBound .. maxBound] ] <> ")"
            , sh (reportSent rep c)
            ]
       | c <- allRequestClasses
       , any (\k -> reportCount rep c k > 0) [minBound .. maxBound]
       ]
    <> [ ""
       , "pact failure (oracle status 200, Pact result status failure) per class: "
            <> T.intercalate ", " [ requestClassText c <> " " <> sh (reportPactFailures rep c) | c <- allRequestClasses, isPinnedClass c ]
       ]
    <> [ "", "allowed per rule:" ]
    <> [ "  " <> allowRuleText r <> ": " <> sh (reportAllowedFor rep r) | r <- [minBound .. maxBound] ]
    <> [ "    #" <> sh (_asIndex a) <> " " <> requestLine (_asRequest a) (_asDepths a) <> ": " <> _asEvidence a
       | a <- reportAllowedSamples rep
       ]
    <> [ "", "skipped per reason:" ]
    <> [ "  " <> skipReasonText r <> ": " <> sh (reportSkippedFor rep r) | r <- [minBound .. maxBound] ]
    <> [ "", "differences (first " <> sh maxDiffSamples <> "):" ]
    <> concatMap diffLines (reportDiffSamples rep)
  where
    sh :: Show a => a -> T.Text
    sh = T.pack . show

    diffLines d =
        [ "  #" <> sh (_dsIndex d) <> " " <> requestLine (_dsRequest d) (_dsDepths d)
        , "    " <> _dsDetail d
        , "    oracle: " <> answerLine (_dsOracle d) (_dsOracleBytes d)
        , "    test:   " <> answerLine (_dsTest d) (_dsTestBytes d)
        ]

    answerLine Nothing _ = "no answer"
    answerLine (Just a) n = "status " <> sh (_ansStatus a) <> ", " <> sh n <> " bytes: " <> lenient (_ansBody a)

instance ToJSON Report where
    toJSON rep = object
        [ "verdict" .= show (reportVerdict rep)
        , "voidReasons" .= reportVoidReasons rep
        , "missingClasses" .= map requestClassText (reportMissingClasses rep)
        , "hollowEntries" .= map entryText (reportHollowEntries rep)
        , "entrySameSuccess" .= object
            [ K.fromText (entryText ce) .= n
            | (ce, n) <- M.toList (M.union (_repEntrySuccess rep) (M.fromList [ (x, 0) | x <- _repExpectedEntries rep ]))
            ]
        , "classesOfRun" .= map requestClassText (_repExpected rep)
        , "generation" .= object
            [ "targets" .= _repGenTargets rep
            , "skipped" .= reportGenSkippedTotal rep
            , "skippedPerKind" .= object
                [ K.fromText k .= n | (k, n) <- M.toList (M.mapKeysWith (+) snd (_repGenSkipped rep)) ]
            ]
        , "settings" .= object [ K.fromText k .= v | (k, v) <- _repSettings rep ]
        , "compared" .= reportCompared rep
        , "differences" .= _repDiffCount rep
        , "sent" .= reportSentTotal rep
        , "pinnedSent" .= _repPinned rep
        , "pinUnstable" .= _repPinUnstable rep
        , "unpinnedSent" .= _repUnpinnedSent rep
        , "inconclusive" .= object
            [ "total" .= reportInconclusive rep
            , "sent" .= reportSentTotal rep
            , "pinUnstable" .= _repPinUnstable rep
            , "unpinnedSent" .= _repUnpinnedSent rep
            , "noOracleAnswer" .= reportSkippedFor rep NoOracleAnswer
            , "generationSkipped" .= reportGenSkippedTotal rep
            ]
        , "noAnswer" .= object
            [ K.fromText (noAnswerText na) .= reportNoAnswer rep na | na <- [minBound .. maxBound] ]
        , "perClass" .= object
            [ K.fromText (requestClassText c) .= object
                ( [ K.fromText (verdictKindText k) .= reportCount rep c k | k <- [minBound .. maxBound] ]
                <> [ "same2xx" .= reportSame2xx rep c
                   , "sameNon2xx" .= reportSameNon2xx rep c
                   , "sent" .= reportSent rep c
                   , "pactFailure200" .= reportPactFailures rep c
                   , "generationSkipped" .= reportGenSkipped rep c
                   , "realKeyCompared" .= reportRealKeyCompared rep c
                   , "skippedPerReason" .= object
                        [ K.fromText (skipReasonText r) .= reportSkippedIn rep c r | r <- [minBound .. maxBound] ]
                   ]
                )
            | c <- allRequestClasses
            ]
        , "allowedPerRule" .= object
            [ K.fromText (allowRuleText r) .= reportAllowedFor rep r | r <- [minBound .. maxBound] ]
        , "allowed" .=
            [ object
                [ "index" .= _asIndex a
                , "request" .= _asRequest a
                , "rule" .= allowRuleText (_asRule a)
                , "evidence" .= _asEvidence a
                , "depths" .= depthsJson (_asDepths a)
                ]
            | a <- reportAllowedSamples rep
            ]
        , "skippedPerReason" .= object
            [ K.fromText (skipReasonText r) .= reportSkippedFor rep r | r <- [minBound .. maxBound] ]
        , "diffSamples" .=
            [ object
                [ "index" .= _dsIndex d
                , "request" .= _dsRequest d
                , "detail" .= _dsDetail d
                , "attempts" .= _dsAttempts d
                , "depths" .= depthsJson (_dsDepths d)
                , "oracle" .= answerJson (_dsOracle d) (_dsOracleBytes d)
                , "test" .= answerJson (_dsTest d) (_dsTestBytes d)
                ]
            | d <- reportDiffSamples rep
            ]
        ]
      where
        depthsJson Nothing = Null
        depthsJson (Just (dO, dT)) = object [ "oracle" .= dO, "test" .= dT ]
        answerJson Nothing _ = Null
        answerJson (Just a) n = object
            [ "status" .= _ansStatus a
            , "bytes" .= n
            , "body" .= lenient (_ansBody a)
            , "truncated" .= (n > B.length (_ansBody a))
            ]

-- | Writes the report as JSON.
--
writeReport :: FilePath -> Report -> IO ()
writeReport f = BL.writeFile f . encode
