{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Main
--
-- The differential harness: sends the same requests to the oracle (the
-- production node) and to a test node, and compares the answers byte for byte
-- (spec 10.1).
--
-- Requests are read from a file with one JSON request per line. Requests are
-- rate limited per host across all threads; two sides on one host share the
-- limit. While the pause file exists, no request is made.
--
-- Exit codes: 0 if the run passes, 2 if it fails (a difference), 3 if it is
-- void and must be rerun, 1 on bad arguments or a crash.
--
module Main (main) where

import Control.Monad

import Data.Aeson (eitherDecodeStrict')
import qualified Data.ByteString.Char8 as B8
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Word

import Options.Applicative

import System.Exit
import System.IO
import System.LogLevel

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.StorageTools.Compare.Report
import Chainweb.StorageTools.Compare.Run
import Chainweb.StorageTools.Compare.Types

-- -------------------------------------------------------------------------- --
-- Options

data Options = Options
    { _optOracle :: !T.Text
    , _optTest :: !T.Text
    , _optRequests :: !FilePath
    , _optChains :: ![ChainId]
    , _optHeights :: !(BlockHeight, BlockHeight)
    , _optRate :: !Double
    , _optPauseFile :: !(Maybe FilePath)
    , _optReport :: !FilePath
    }

chainsReader :: ReadM [ChainId]
chainsReader = eitherReader $ \s -> forM (splitOn ',' s) $ \c -> case reads c of
    [(n, "")] -> Right (unsafeChainId (n :: Word32))
    _ -> Left $ "invalid chain id: " <> c

heightsReader :: ReadM (BlockHeight, BlockHeight)
heightsReader = eitherReader $ \s -> case splitOn '-' s of
    [a, b] | [(lo, "")] <- reads a, [(hi, "")] <- reads b, lo <= hi ->
        Right (BlockHeight (lo :: Word64), BlockHeight hi)
    _ -> Left $ "invalid height range, expected LOW-HIGH: " <> s

splitOn :: Char -> String -> [String]
splitOn c s = case break (== c) s of
    (a, []) -> [a]
    (a, _ : rest) -> a : splitOn c rest

options :: Parser Options
options = Options
    <$> strOption
        ( long "oracle"
        <> metavar "URL"
        <> help "base URL of the service API of the oracle, for instance https://host:1848"
        )
    <*> strOption
        ( long "test"
        <> metavar "URL"
        <> help "base URL of the service API of the test node"
        )
    <*> strOption
        ( long "requests"
        <> metavar "FILE"
        <> help "the requests, one JSON object per line"
        )
    <*> option chainsReader
        ( long "chains"
        <> metavar "0,3,..."
        <> help "the chains the test node holds"
        )
    <*> option heightsReader
        ( long "heights"
        <> metavar "LOW-HIGH"
        <> help "the heights the test node holds on these chains, inclusive"
        )
    <*> option auto
        ( long "rate"
        <> metavar "REQUESTS_PER_SECOND"
        <> value 20
        <> showDefault
        <> help "requests per second to each host, across all threads; both sides on one host share it"
        )
    <*> optional (strOption
        ( long "pause-file"
        <> metavar "FILE"
        <> help "no request is made while this file exists"
        ))
    <*> strOption
        ( long "report"
        <> metavar "FILE"
        <> help "where to write the JSON report; a text summary goes to stdout"
        )

-- -------------------------------------------------------------------------- --
-- Main

readRequests :: FilePath -> IO [Request]
readRequests f = do
    ls <- B8.lines <$> B8.readFile f
    fmap concat $ forM (zip [1 :: Int ..] ls) $ \(i, l) ->
        if B8.all (`elem` (" \t\r" :: String)) l
        then return []
        else case eitherDecodeStrict' l of
            Right r -> return [r]
            Left e -> die $ f <> ":" <> show i <> ": " <> e

main :: IO ()
main = do
    hSetBuffering stderr LineBuffering
    o <- execParser $ info (options <**> helper)
        ( fullDesc
        <> progDesc "Compare the answers of a test node with those of the oracle"
        <> footer "Exit codes: 0 pass; 2 fail (a difference); 3 void (rerun); 1 bad arguments or a crash."
        )
    reqs <- readRequests (_optRequests o)
    let logger = genericLogger Info (T.hPutStrLn stderr)
        coverage = Coverage $ M.fromList [ (c, [_optHeights o]) | c <- _optChains o ]
    hl <- newHostLimiters (_optRate o) (_optPauseFile o)
    oracle <- Side "oracle" (_optOracle o) <$> limiterFor hl (_optOracle o)
    test <- Side "test" (_optTest o) <$> limiterFor hl (_optTest o)
    rep <- runCompare logger coverage oracle test reqs
    writeReport (_optReport o) rep
    T.putStr (reportSummary rep)
    exitWith (gateExitCode (reportVerdict rep))
