{-# LANGUAGE NumericUnderscores #-}

-- |
-- Module: Chainweb.StorageTools.Test.FileLimit
--
-- Tests of the open-files limit of the tools.
--
module Chainweb.StorageTools.Test.FileLimit
( tests
) where

import qualified Data.Text as T

import System.Exit (ExitCode(..))
import System.Posix.Process (ProcessStatus(..), exitImmediately, forkProcess, getProcessStatus)
import System.Posix.Resource

import Test.Tasty
import Test.Tasty.HUnit

-- internal modules

import Chainweb.StorageTools.FileLimit

tests :: TestTree
tests = testGroup "FileLimit"
    [ testCase "candidates: the hard limit, only if it is above the soft limit" $ do
        fileLimitCandidates (ResourceLimit 1_024) (ResourceLimit 524_288) @?= [ResourceLimit 524_288]
        fileLimitCandidates (ResourceLimit 524_288) (ResourceLimit 524_288) @?= []
        fileLimitCandidates (ResourceLimit 600_000) (ResourceLimit 524_288) @?= []
        fileLimitCandidates ResourceLimitInfinity (ResourceLimit 524_288) @?= []
    , testCase "candidates: an infinite hard limit is tried first, then finite values above the soft limit" $ do
        let cs = fileLimitCandidates (ResourceLimit 256) ResourceLimitInfinity
        take 1 cs @?= [ResourceLimitInfinity]
        assertBool ("finite values follow: " <> show cs) (length cs > 1)
        fileLimitCandidates (ResourceLimit 100_000) ResourceLimitInfinity
            @?= [ResourceLimitInfinity, ResourceLimit 1_048_576, ResourceLimit 524_288]
        fileLimitCandidates ResourceLimitInfinity ResourceLimitInfinity @?= []
    , testCase "raiseFileLimit raises the soft limit of this process to the hard limit" $ do
        ResourceLimits softBefore hard <- getResourceLimit ResourceOpenFiles
        fl <- raiseFileLimit
        ResourceLimits softNow hardNow <- getResourceLimit ResourceOpenFiles
        _flSoftBefore fl @?= softBefore
        _flHard fl @?= hard
        _flSoftAfter fl @?= softNow
        hardNow @?= hard
        case (softBefore, hard, softNow) of
            -- a finite hard limit is reached
            (_, ResourceLimit h, ResourceLimit s) -> s @?= h
            -- an infinite one: the system may refuse infinity, the soft limit is
            -- still not lower than it was
            (ResourceLimit b, _, ResourceLimit s) -> assertBool ("raised from " <> show b <> " to " <> show s) (s >= b)
            (_, _, s) -> assertBool ("unlimited or unknown: " <> show s) (s /= ResourceLimit 0)
        -- a second call changes nothing
        fl2 <- raiseFileLimit
        _flSoftAfter fl2 @?= softNow
    , testCase "raiseFileLimit raises a low soft limit: a child process starts at 256 and ends at the hard limit" $ do
        -- a child process, so that the limit of the test process, which other
        -- tests share, is never lowered
        ResourceLimits _ hard <- getResourceLimit ResourceOpenFiles
        let child = do
                setResourceLimit ResourceOpenFiles (ResourceLimits (ResourceLimit 256) hard)
                fl <- raiseFileLimit
                ResourceLimits now _ <- getResourceLimit ResourceOpenFiles
                let ok = case (hard, now) of
                        (ResourceLimit h, ResourceLimit s) -> s == h
                        (_, ResourceLimit s) -> s > 256
                        (_, _) -> True
                exitImmediately $ if ok && _flSoftBefore fl == ResourceLimit 256 && _flSoftAfter fl == now
                    then ExitSuccess
                    else ExitFailure 3
        case hard of
            ResourceLimit h | h < 256 -> return ()
            _ -> do
                pid <- forkProcess child
                getProcessStatus True False pid >>= \st -> st @?= Just (Exited ExitSuccess)
    , testCase "the log line names the soft limit before and after, and the hard limit" $
        describeFileLimits (FileLimits (ResourceLimit 1_024) (ResourceLimit 524_288) (ResourceLimit 524_288))
            @?= T.pack "open files: soft 1024 -> 524288, hard 524288"
    ]
