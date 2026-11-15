{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Module: Chainweb.StorageTools.FileLimit
--
-- The open-files limit of a tool. @cw-compare@ keeps many connections and, with
-- an in-process test side, many RocksDB files open; a run of hours died of
-- "Too many open files" under the default soft limit of its container (M0, W3).
-- The tool raises its own soft limit of @RLIMIT_NOFILE@ to the hard limit at
-- start, so that it no longer depends on a @ulimit@ in the command that
-- starts it.
--
module Chainweb.StorageTools.FileLimit
( FileLimits(..)
, raiseFileLimit
, fileLimitCandidates
, describeFileLimits
) where

import Control.Exception (IOException, try)

import qualified Data.Text as T

import System.Posix.Resource

-- | The soft limit before and after 'raiseFileLimit', and the hard limit.
--
data FileLimits = FileLimits
    { _flSoftBefore :: !ResourceLimit
    , _flHard :: !ResourceLimit
    , _flSoftAfter :: !ResourceLimit
        -- ^ read back after the change
    }
    deriving (Show, Eq)

limitText :: ResourceLimit -> T.Text
limitText = \case
    ResourceLimitInfinity -> "unlimited"
    ResourceLimitUnknown -> "unknown"
    ResourceLimit n -> T.pack (show n)

-- | For the log: @open files: soft 1024 -> 524288, hard 524288@.
--
describeFileLimits :: FileLimits -> T.Text
describeFileLimits (FileLimits before hard after) =
    "open files: soft " <> limitText before <> " -> " <> limitText after <> ", hard " <> limitText hard

-- | The values to try for the new soft limit, best first: the hard limit; if
-- the hard limit is infinite, some systems refuse an infinite soft limit
-- (macOS: above @kern.maxfilesperproc@) and the finite values after it are
-- tried. A value that is not above the current soft limit is left out, so
-- that the limit is never lowered.
--
fileLimitCandidates :: ResourceLimit -> ResourceLimit -> [ResourceLimit]
fileLimitCandidates soft hard = filter (above soft) $ case hard of
    ResourceLimit _ -> [hard]
    ResourceLimitInfinity -> hard : map ResourceLimit [1_048_576, 524_288, 65_536, 10_240]
    ResourceLimitUnknown -> []
  where
    above (ResourceLimit s) (ResourceLimit c) = c > s
    above ResourceLimitInfinity _ = False
    above ResourceLimitUnknown _ = False
    above (ResourceLimit _) _ = True

-- | Raises the soft limit of open files to the hard limit (see
-- 'fileLimitCandidates'). Never fails: if no value is accepted the limit stays
-- as it is, and the result says so.
--
raiseFileLimit :: IO FileLimits
raiseFileLimit = do
    ResourceLimits soft hard <- getResourceLimit ResourceOpenFiles
    let attempt [] = return ()
        attempt (c : cs) = try (setResourceLimit ResourceOpenFiles (ResourceLimits c hard)) >>= \case
            Right () -> return ()
            Left (_ :: IOException) -> attempt cs
    attempt (fileLimitCandidates soft hard)
    ResourceLimits soft' _ <- getResourceLimit ResourceOpenFiles
    return (FileLimits soft hard soft')
