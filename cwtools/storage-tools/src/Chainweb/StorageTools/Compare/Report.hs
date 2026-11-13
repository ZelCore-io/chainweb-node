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
, reportAllowedFor
, reportDiffCount
, reportDiffSamples
, reportAllowedSamples
, reportPinned
, reportPinUnstable
, reportCompared
, maxDiffSamples
, maxBodyBytes

  -- * Gate verdict
, GateVerdict(..)
, reportVerdict

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
    , _repSkipped :: !(M.Map SkipReason Int)
    , _repAllowed :: !(M.Map Int AllowedSample)
        -- ^ every allowed difference
    , _repDiffCount :: !Int
    , _repDiffs :: !(M.Map Int DiffSample)
        -- ^ the differences with the lowest request indices
    , _repPinned :: !Int
        -- ^ requests of a pinned class that were sent
    , _repPinUnstable :: !Int
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
emptyReport = Report mempty mempty mempty 0 mempty 0 0

-- | Adds the outcome of a request. The result does not depend on the order
-- in which outcomes are added.
--
addOutcome :: Report -> Outcome -> Report
addOutcome rep o = case v of
    Same -> counted KindSame rep
    Allowed rule ev -> (counted KindAllowed rep)
        { _repAllowed = M.insert idx (AllowedSample idx req rule ev) (_repAllowed rep) }
    Differ detail -> (counted KindDiffer rep)
        { _repDiffCount = _repDiffCount rep + 1
        , _repDiffs = keepLowest $ M.insert idx (sample detail) (_repDiffs rep)
        }
    Skipped reason -> (counted KindSkipped rep)
        { _repSkipped = M.insertWith (+) reason 1 (_repSkipped rep)
        , _repPinUnstable = _repPinUnstable rep + if reason == PinUnstable then 1 else 0
        }
  where
    v = effectiveVerdict (_outVerdict o)
    idx = _outIndex o
    req = _outRequest o
    sent = _outAttempts o > 0
    pinnedSent = if isPinnedClass (_reqClass req) && sent then 1 else 0

    counted k r = r
        { _repCounts = M.insertWith (+) (_reqClass req, k) 1 (_repCounts r)
        , _repPinned = _repPinned r + pinnedSent
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

reportSkippedFor :: Report -> SkipReason -> Int
reportSkippedFor rep r = M.findWithDefault 0 r (_repSkipped rep)

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

-- | The number of requests whose answers were judged: same, allowed or
-- different.
--
reportCompared :: Report -> Int
reportCompared rep = sum
    [ reportClassTotal rep KindSame
    , reportClassTotal rep KindAllowed
    , reportClassTotal rep KindDiffer
    ]

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
-- * 'Void' if more than 1% of the pinned requests that were sent ended
--   'PinUnstable', or if no request was compared at all.
-- * 'Pass' otherwise.
--
reportVerdict :: Report -> GateVerdict
reportVerdict rep
    | _repDiffCount rep > 0 = Fail
    | _repPinned rep > 0 && 100 * _repPinUnstable rep > _repPinned rep = Void
    | reportCompared rep == 0 = Void
    | otherwise = Pass

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
    , "compared: " <> sh (reportCompared rep)
        <> ", differences: " <> sh (_repDiffCount rep)
        <> ", pinned requests sent: " <> sh (_repPinned rep)
        <> ", pin unstable: " <> sh (_repPinUnstable rep)
    , ""
    , "per class: same / allowed / differ / skipped"
    ]
    <> [ "  " <> requestClassText c <> ": " <> T.intercalate " / " [ sh (reportCount rep c k) | k <- [minBound .. maxBound] ]
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
        , "compared" .= reportCompared rep
        , "differences" .= _repDiffCount rep
        , "pinnedSent" .= _repPinned rep
        , "pinUnstable" .= _repPinUnstable rep
        , "perClass" .= object
            [ K.fromText (requestClassText c) .= object
                [ K.fromText (verdictKindText k) .= reportCount rep c k | k <- [minBound .. maxBound] ]
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
