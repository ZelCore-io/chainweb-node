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
, reportCompared
, maxDiffSamples
, maxBodyBytes

  -- * Gate verdict
, GateVerdict(..)
, reportVerdict
, reportVoidReasons
, gateExitCode

  -- * Output
, reportSummary
, writeReport
) where

import Data.Aeson
import qualified Data.Aeson.Key as K
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Encoding.Error as T

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
    }

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
    }

data AllowedSample = AllowedSample
    { _asIndex :: !Int
    , _asRequest :: !Request
    , _asRule :: !AllowRule
    , _asEvidence :: !T.Text
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
emptyReport = Report mempty mempty mempty mempty mempty 0 mempty 0 0 0

-- | Adds the outcome of a request. The result does not depend on the order
-- in which outcomes are added.
--
addOutcome :: Report -> Outcome -> Report
addOutcome rep o = case v of
    Same -> (counted KindSame rep)
        { _repSameNon2xx = if is2xx then _repSameNon2xx rep else M.insertWith (+) cls 1 (_repSameNon2xx rep) }
    Allowed rule ev -> (counted KindAllowed rep)
        { _repAllowed = M.insert idx (AllowedSample idx req rule ev) (_repAllowed rep) }
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
        }

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

-- -------------------------------------------------------------------------- --
-- Gate verdict

data GateVerdict
    = Pass
    | Fail
    | Void
        -- ^ the run says nothing; rerun it
    deriving (Show, Eq)

-- | The verdict of the gate.
--
-- * 'Fail' on any difference, including an allowed one without evidence.
-- * Otherwise 'Void' if any of 'reportVoidReasons' holds.
-- * 'Pass' otherwise.
--
reportVerdict :: Report -> GateVerdict
reportVerdict rep
    | _repDiffCount rep > 0 = Fail
    | not (null (reportVoidReasons rep)) = Void
    | otherwise = Pass

-- | Why the run says nothing and must be rerun; empty if it does say
-- something:
--
-- * no request was compared at all;
-- * a class has requests within the coverage, but none of them was
--   compared: a hollow class must not pass ('NotCovered' requests never
--   count here);
-- * 'PinUnstable' and 'Unpinned' requests that were sent are together more
--   than 1% of the pinned requests that were sent.
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
    , [ sh pu <> " pin unstable and " <> sh up <> " unpinned after sending of "
        <> sh pinned <> " pinned requests sent, more than 1%"
      | pinned > 0
      , 100 * (pu + up) > pinned
      ]
    ]
  where
    pinned = _repPinned rep
    pu = _repPinUnstable rep
    up = _repUnpinnedSent rep
    sh :: Int -> T.Text
    sh = T.pack . show

-- | The exit code of @cw-compare@ for a gate verdict: 0 'Pass', 2 'Fail',
-- 3 'Void'. Exit code 1 is left to bad arguments and crashes, so that a
-- crash is never read as a verdict.
--
gateExitCode :: GateVerdict -> ExitCode
gateExitCode = \case
    Pass -> ExitSuccess
    Fail -> ExitFailure 2
    Void -> ExitFailure 3

-- -------------------------------------------------------------------------- --
-- Output

lenient :: B.ByteString -> T.Text
lenient = T.decodeUtf8With T.lenientDecode

requestLine :: Request -> T.Text
requestLine r = T.unwords
    [ requestClassText (_reqClass r)
    , "chain " <> T.pack (show (_reqChain r))
    , maybe "no target" (\h -> "at " <> T.pack (show h)) (_reqTarget r)
    , lenient (_reqMethod r)
    , _reqPath r
    ]

-- | A text summary: the gate verdict, counts per class and verdict, allowed
-- differences per rule with their evidence, skipped requests per reason, and
-- the first differences with both bodies (truncated to 4 KB).
--
reportSummary :: Report -> T.Text
reportSummary rep = T.unlines $
    [ "verdict: " <> T.pack (show (reportVerdict rep))
    ]
    <> [ "  void: " <> r | reportVerdict rep == Void, r <- reportVoidReasons rep ]
    <> [ "compared: " <> sh (reportCompared rep)
        <> ", differences: " <> sh (_repDiffCount rep)
        <> ", sent: " <> sh (reportSentTotal rep)
        <> ", pinned requests sent: " <> sh (_repPinned rep)
        <> ", pin unstable: " <> sh (_repPinUnstable rep)
        <> ", unpinned after sending: " <> sh (_repUnpinnedSent rep)
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
    <> [ "", "allowed per rule:" ]
    <> [ "  " <> allowRuleText r <> ": " <> sh (reportAllowedFor rep r) | r <- [minBound .. maxBound] ]
    <> [ "    #" <> sh (_asIndex a) <> " " <> requestLine (_asRequest a) <> ": " <> _asEvidence a
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
        [ "  #" <> sh (_dsIndex d) <> " " <> requestLine (_dsRequest d)
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
        , "compared" .= reportCompared rep
        , "differences" .= _repDiffCount rep
        , "sent" .= reportSentTotal rep
        , "pinnedSent" .= _repPinned rep
        , "pinUnstable" .= _repPinUnstable rep
        , "unpinnedSent" .= _repUnpinnedSent rep
        , "perClass" .= object
            [ K.fromText (requestClassText c) .= object
                ( [ K.fromText (verdictKindText k) .= reportCount rep c k | k <- [minBound .. maxBound] ]
                <> [ "same2xx" .= reportSame2xx rep c
                   , "sameNon2xx" .= reportSameNon2xx rep c
                   , "sent" .= reportSent rep c
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
                , "oracle" .= answerJson (_dsOracle d) (_dsOracleBytes d)
                , "test" .= answerJson (_dsTest d) (_dsTestBytes d)
                ]
            | d <- reportDiffSamples rep
            ]
        ]
      where
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
