{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module: Chainweb.StorageTools.Compare.Sample
--
-- Seeded sampling of the targets of @cw-compare@: (class, chain, height)
-- triples drawn from the covered windows, so that a run can be repeated.
--
-- Each target draws its class uniformly from '_ssClasses', then, with
-- probability '_ssBoundaryShare', a height within 'boundaryDistance' heights of
-- a boundary, and otherwise a height uniformly from the covered heights. A
-- class whose covered windows have no height near a boundary always draws
-- uniformly. History targets are drawn only up to '_ssHistoryMax', the last
-- Pact 4 height of the version ('historyMaxHeight'), because Pact 5 has no
-- history functions (spec 10.1).
--
module Chainweb.StorageTools.Compare.Sample
( SampleSpec(..)
, mainnetBoundaries
, boundaryShare
, boundaryDistance
, historyMaxHeight
, sampleTargets
) where

import Control.Lens (at, (^?), _Just)

import qualified Data.HashSet as HS
import Data.List (sortOn)
import qualified Data.Map.Strict as M
import Data.Word

import System.Random (StdGen, mkStdGen, uniformR)

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.StorageTools.Compare.Types
import Chainweb.Version (ChainwebVersion, Fork(Pact5Fork), ForkHeight(..), chainIds, versionForks)

data SampleSpec = SampleSpec
    { _ssSeed :: !Word64
    , _ssCount :: !Int
        -- ^ the number of targets
    , _ssClasses :: ![RequestClass]
    , _ssCoverage :: !Coverage
    , _ssBoundaries :: ![BlockHeight]
    , _ssBoundaryShare :: !Double
        -- ^ the share of targets within 'boundaryDistance' of a boundary
    , _ssHistoryMax :: !(Maybe BlockHeight)
        -- ^ the highest height of a History target, from 'historyMaxHeight';
        -- 'Nothing': no History target is drawn
    }

-- | The fork boundaries of mainnet: the coin v2 upgrade, the genesis of the
-- ten added chains, Pact42, Pact 5, the serialiser change and the @keys@
-- dedup fork. Freeze boundaries join in P3.
--
mainnetBoundaries :: [BlockHeight]
mainnetBoundaries = [140_808, 852_054, 2_334_500, 5_555_698, 5_659_280, 6_027_616]

-- | The default share of targets near a boundary (spec 10.1, "a fixed
-- share").
--
boundaryShare :: Double
boundaryShare = 0.2

-- | A target near a boundary is at most this many heights away from it.
--
boundaryDistance :: Word64
boundaryDistance = 5

-- | The highest height at which a version runs Pact 4 on every chain: its
-- Pact 5 fork height minus 1, the lowest across chains. History targets stay
-- at or below it; 5,555,697 on mainnet.
--
-- 'Nothing' if some chain has no Pact 4 height, or its Pact 5 fork height is
-- not known in advance (a fork number) or not listed: no History target is
-- drawn then. A version that never forks to Pact 5 has no cap.
--
historyMaxHeight :: ChainwebVersion -> Maybe BlockHeight
historyMaxHeight v = minimum . (maxBound :) <$> traverse lastPact4 (HS.toList (chainIds v))
  where
    lastPact4 cid = case v ^? versionForks . at Pact5Fork . _Just . atChain cid of
        Just (ForkAtBlockHeight h) | h > 0 -> Just (h - 1)
        Just ForkNever -> Just maxBound
        _ -> Nothing

-- | A chain and an inclusive, non-empty range of heights.
--
data Window = Window !ChainId !Word64 !Word64

windowSize :: Window -> Word64
windowSize (Window _ lo hi) = hi - lo + 1

-- | The covered windows, per chain in ascending order, overlaps merged.
--
windows :: Coverage -> [Window]
windows (Coverage m) =
    [ Window c lo hi
    | (c, rs) <- M.toAscList m
    , (lo, hi) <- merge $ sortOn fst
        [ (getBlockHeight lo, getBlockHeight hi) | (lo, hi) <- rs, lo <= hi ]
    ]
  where
    merge ((a, b) : (c, d) : rest)
        | c <= b || c - b == 1 = merge ((a, max b d) : rest)
    merge (x : rest) = x : merge rest
    merge [] = []

clip :: Word64 -> Word64 -> [Window] -> [Window]
clip lo' hi' ws =
    [ Window c lo hi
    | Window c a b <- ws
    , let lo = max a lo'
    , let hi = min b hi'
    , lo <= hi
    ]

classWindows :: SampleSpec -> RequestClass -> [Window] -> [Window]
classWindows spec = \case
    ClsHistory -> maybe (const []) (clip 0 . getBlockHeight) (_ssHistoryMax spec)
    _ -> id

nearBoundaries :: [BlockHeight] -> [Window] -> [Window]
nearBoundaries bs ws =
    [ w
    | b <- map getBlockHeight bs
    , w <- clip (b - min b boundaryDistance) (b + boundaryDistance) ws
    ]

-- | The height at a position counted across the windows.
--
pick :: Word64 -> [Window] -> (ChainId, BlockHeight)
pick i (w@(Window c lo _) : rest)
    | i < windowSize w = (c, BlockHeight (lo + i))
    | otherwise = pick (i - windowSize w) rest
pick _ [] = error "Chainweb.StorageTools.Compare.Sample.pick: position out of range"

drawFrom :: [Window] -> StdGen -> ((ChainId, BlockHeight), StdGen)
drawFrom ws g =
    let (i, g') = uniformR (0, sum (map windowSize ws) - 1) g
    in (pick i ws, g')

-- | '_ssCount' targets, the same for the same spec. Classes without covered
-- heights are left out; without any class the result is empty.
--
sampleTargets :: SampleSpec -> [(RequestClass, ChainId, BlockHeight)]
sampleTargets spec = go (mkStdGen (fromIntegral (_ssSeed spec))) (_ssCount spec)
  where
    covered = windows (_ssCoverage spec)
    pools =
        [ (cls, regular, nearBoundaries (_ssBoundaries spec) regular)
        | cls <- _ssClasses spec
        , let regular = classWindows spec cls covered
        , not (null regular)
        ]
    nPools = length pools

    go g k
        | k <= 0 || nPools == 0 = []
        | otherwise =
            let (p, g1) = uniformR (0, nPools - 1) g
                (cls, regular, near) = pools !! p
                (u, g2) = uniformR (0, 1 :: Double) g1
                ws = if u < _ssBoundaryShare spec && not (null near) then near else regular
                ((c, h), g3) = drawFrom ws g2
            in (cls, c, h) : go g3 (k - 1)
