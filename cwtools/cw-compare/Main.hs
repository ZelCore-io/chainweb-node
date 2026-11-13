{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Main
--
-- The differential harness: sends the same requests to the oracle (the
-- production node) and to a test node, and compares the answers byte for byte
-- (spec 10.1).
--
-- By default the requests are generated: @--count@ targets are sampled with
-- @--seed@ over the classes, chains and heights given (each chain from its
-- genesis height at the earliest), and each target becomes a request built
-- from the oracle's canonical block there. The same seed, ranges and oracle
-- give the same requests. With @--requests FILE@, the requests are read from
-- a file with one JSON request per line instead.
--
-- Requests and the oracle lookups that build them are rate limited per host
-- across all threads; two sides on one host share the limit. While the pause
-- file exists, no request is made.
--
-- Exit codes: 0 if the run passes, 2 if it fails (a difference), 3 if it is
-- void and must be rerun, 1 on bad arguments or a crash.
--
module Main (main) where

import Control.Monad
import Data.Aeson (eitherDecodeStrict')
import qualified Data.ByteString.Char8 as B8
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
import Chainweb.StorageTools.Compare.PactSet
import Chainweb.StorageTools.Compare.Report
import Chainweb.StorageTools.Compare.Requests
import Chainweb.StorageTools.Compare.Run
import Chainweb.StorageTools.Compare.Sample
import Chainweb.StorageTools.Compare.Types
import Chainweb.Version
import Chainweb.Version.Mainnet (mainnet)
import Chainweb.Version.Registry (findKnownVersion)

-- -------------------------------------------------------------------------- --
-- Options

data Options = Options
    { _optOracle :: !T.Text
    , _optTest :: !T.Text
    , _optVersion :: !ChainwebVersion
    , _optChains :: ![ChainId]
    , _optHeights :: !(BlockHeight, BlockHeight)
    , _optSource :: !Source
    , _optRate :: !Double
    , _optPauseFile :: !(Maybe FilePath)
    , _optReport :: !FilePath
    }

data Source
    = Generated !Generate
    | RequestFile !FilePath

data Generate = Generate
    { _genClasses :: ![RequestClass]
    , _genCount :: !Int
    , _genSeed :: !Word64
    , _genPactSet :: !(Maybe FilePath)
    , _genSameSnapshot :: !Bool
    }

chainsReader :: ReadM [ChainId]
chainsReader = eitherReader $ \s -> forM (splitOn ',' s) $ \c -> case reads c of
    [(n, "")] -> Right (unsafeChainId (n :: Word32))
    _ -> Left $ "invalid chain id: " <> c

classesReader :: ReadM [RequestClass]
classesReader = eitherReader $ \s -> forM (splitOn ',' s) $ \c ->
    maybe (Left $ "unknown request class: " <> c) Right (requestClassFromText (T.pack c))

heightsReader :: ReadM (BlockHeight, BlockHeight)
heightsReader = eitherReader $ \s -> case splitOn '-' s of
    [a, b] | [(lo, "")] <- reads a, [(hi, "")] <- reads b, lo <= hi ->
        Right (BlockHeight (lo :: Word64), BlockHeight hi)
    _ -> Left $ "invalid height range, expected LOW-HIGH: " <> s

splitOn :: Char -> String -> [String]
splitOn c s = case break (== c) s of
    (a, []) -> [a]
    (a, _ : rest) -> a : splitOn c rest

versionOption :: Parser ChainwebVersion
versionOption = option (eitherReader readVersion)
    ( long "version"
    <> metavar "NAME"
    <> value mainnet
    <> showDefaultWith (T.unpack . getChainwebVersionName . _versionName)
    <> help "chainweb version of both nodes"
    )
  where
    readVersion s = case findKnownVersion (ChainwebVersionName $ T.pack s) of
        Just v -> Right v
        Nothing -> Left $ "unknown chainweb version: " <> s

generateOptions :: Parser Generate
generateOptions = Generate
    <$> option classesReader
        ( long "classes"
        <> metavar "payload,header,..."
        <> value allRequestClasses
        <> showDefaultWith (const (T.unpack (T.intercalate "," (map requestClassText allRequestClasses))))
        <> help "the request classes; a class without a request compared voids the run"
        )
    <*> option auto
        ( long "count"
        <> metavar "N"
        <> help "the number of requests to generate"
        )
    <*> option auto
        ( long "seed"
        <> metavar "S"
        <> help "the seed of the sampling and of every random choice in a request"
        )
    <*> optional (strOption
        ( long "pact-set"
        <> metavar "FILE"
        <> help "the Pact set (YAML) for /local, history and preflight requests; default: the built-in data/pact-set.yaml"
        ))
    <*> switch
        ( long "same-snapshot"
        <> help "both nodes were copied from one RocksDB snapshot: also compare plain header ranges (orphans included)"
        )

sourceOptions :: Parser Source
sourceOptions = RequestFile <$> strOption
        ( long "requests"
        <> metavar "FILE"
        <> help "compare the requests of this file, one JSON object per line, instead of generating them"
        )
    <|> Generated <$> generateOptions

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
    <*> versionOption
    <*> option chainsReader
        ( long "chains"
        <> metavar "0,3,..."
        <> help "the chains the test node holds"
        )
    <*> option heightsReader
        ( long "heights"
        <> metavar "LOW-HIGH"
        <> help "the heights the test node holds on these chains, inclusive; each chain from its genesis at the earliest"
        )
    <*> sourceOptions
    <*> option auto
        ( long "rate"
        <> metavar "REQUESTS_PER_SECOND"
        <> value 20
        <> showDefault
        <> help "requests per second to each host, across all threads, oracle lookups included; both sides on one host share it"
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
        <> footer (unwords
            [ "Exit codes: 0 pass; 2 fail (a difference); 3 void (rerun); 1 bad arguments or a crash."
            , "Not yet available: --test-db DIR, --pact-chains and --payload-compression (an in-process test side);"
            , "use --test URL."
            ])
        )
    let logger = genericLogger Info (T.hPutStrLn stderr)
        v = _optVersion o
        coverage = genesisCoverage v (_optChains o) (_optHeights o)
    hl <- newHostLimiters (_optRate o) (_optPauseFile o)
    oracle <- Side "oracle" (_optOracle o) <$> limiterFor hl (_optOracle o)
    test <- Side "test" (_optTest o) <$> limiterFor hl (_optTest o)
    (reqs, classes) <- case _optSource o of
        RequestFile f -> (\rs -> (rs, [])) <$> readRequests f
        Generated g -> do
            pset <- maybe (return defaultPactSet) loadPactSet (_genPactSet g)
            api <- oracleServiceApi hl v (_optOracle o)
            let spec = SampleSpec
                    { _ssSeed = _genSeed g
                    , _ssCount = _genCount g
                    , _ssClasses = _genClasses g
                    , _ssCoverage = coverage
                    , _ssBoundaries = if _versionName v == _versionName mainnet then mainnetBoundaries else []
                    , _ssBoundaryShare = boundaryShare
                    , _ssHistoryMax = historyMaxHeight v
                    }
                targets = sampleTargets spec
            logFunctionText logger Info $ "generating " <> T.pack (show (length targets)) <> " requests"
            rs <- generateRequests (GenOptions (_genSameSnapshot g)) api pset (_genSeed g) targets
            return (rs, _genClasses g)
    rep <- reportExpectClasses classes <$> runCompare logger coverage oracle test reqs
    writeReport (_optReport o) rep
    T.putStr (reportSummary rep)
    exitWith (gateExitCode (reportVerdict rep))
