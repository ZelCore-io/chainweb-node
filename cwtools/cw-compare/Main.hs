{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
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
-- The test side is a node's service API (@--test URL@), or the service API
-- served in process from a node's database directory (@--test-db DIR@, see
-- "Chainweb.StorageTools.Compare.InProcess"). In-process, only the
-- @--pact-chains@ get a Pact service: the classes Pact answers (@/local@,
-- History, preflight, @/poll@, @/spv@) are covered on those chains only, and
-- their requests on other chains are skipped as not covered, never sent.
--
-- Requests and the oracle lookups that build them are rate limited per host
-- across all threads; two sides on one host share the limit. While the pause
-- file exists, no request is made, to either side.
--
-- A target whose oracle lookups fail is dropped and counted as skipped at
-- generation; more than 1% of the targets dropped voids the run.
--
-- A poll or SPV target without a transaction within 1000 blocks from the
-- sampled height is dropped at generation too, with that reason.
--
-- Only a run of all 7 classes can pass: one of fewer classes (@--classes@),
-- or one without a poll or SPV request with a real key compared, is at best
-- a partial pass (exit code 4), never a pass.
--
-- Exit codes: 0 if the run passes, 4 if it partially passes (no difference,
-- but not every class), 2 if it fails (a difference), 3 if it is void and
-- must be rerun, 1 on bad arguments or a crash.
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
import Chainweb.Storage.Table.RocksDB.Tuning (defaultTuning, payloadZstdTuning)
import Chainweb.StorageTools.Compare.InProcess
import Chainweb.StorageTools.Compare.PactSet
import Chainweb.StorageTools.Compare.Report
import Chainweb.StorageTools.Compare.Requests
import Chainweb.StorageTools.Compare.Run
import Chainweb.StorageTools.Compare.Sample
import Chainweb.StorageTools.Compare.Types
import Chainweb.Utils (toText)
import Chainweb.Version
import Chainweb.Version.Mainnet (mainnet)
import Chainweb.Version.Registry (findKnownVersion)

-- -------------------------------------------------------------------------- --
-- Options

data Options = Options
    { _optOracle :: !T.Text
    , _optTest :: !TestSide
    , _optVersion :: !ChainwebVersion
    , _optChains :: ![ChainId]
    , _optHeights :: !(BlockHeight, BlockHeight)
    , _optSource :: !Source
    , _optRate :: !Double
    , _optPauseFile :: !(Maybe FilePath)
    , _optReport :: !FilePath
    , _optOracleLocalTimeout :: !Int
        -- ^ seconds
    }

data TestSide
    = TestUrl !T.Text
    | TestDb !DbSide

data DbSide = DbSide
    { _tdDir :: !FilePath
    , _tdPactChains :: ![ChainId]
    , _tdZstd :: !Bool
    , _tdLocal :: !LocalSettings
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

positiveSeconds :: String -> Either String Int
positiveSeconds s = case reads s of
    [(n, "")] | n > 0, n <= 3_600 -> Right n
    _ -> Left $ "expected a number of seconds from 1 to 3600: " <> s

splitOn :: Char -> String -> [String]
splitOn c s = case break (== c) s of
    (a, []) -> [a]
    (a, _ : rest) -> a : splitOn c rest

compressionReader :: ReadM Bool
compressionReader = eitherReader $ \case
    "snappy" -> Right False
    "zstd-dict" -> Right True
    s -> Left $ "unknown payload compression, expected snappy or zstd-dict: " <> s

testDbOptions :: Parser DbSide
testDbOptions = DbSide
    <$> strOption
        ( long "test-db"
        <> metavar "DIR"
        <> help "serve the test side in process from this node database directory (the one that holds rocksDb and sqlite, or its parent); RocksDB is opened read-only, but Pact opens the SQLite files read-write: use a copy or a stopped node's files only"
        )
    <*> option chainsReader
        ( long "pact-chains"
        <> metavar "0,3,..."
        <> help "with --test-db: the chains that get a Pact service; /local, history, preflight, poll and spv are covered on these chains only"
        )
    <*> option compressionReader
        ( long "payload-compression"
        <> metavar "snappy|zstd-dict"
        <> value False
        <> help "with --test-db: the payload compression the database was written with (default: snappy)"
        )
    <*> localSettingsParser

testSideOptions :: Parser TestSide
testSideOptions = TestUrl <$> strOption
        ( long "test"
        <> metavar "URL"
        <> help "base URL of the service API of the test node"
        )
    <|> TestDb <$> testDbOptions

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
        <> help "the request classes; a class without a request compared voids the run. Only all 7 classes can pass; fewer give at best a partial pass (exit 4)"
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
        <> help "both nodes were copied from one RocksDB snapshot: also compare plain header ranges (orphans included) and send the Pact set entries marked sameSnapshotOnly (History txids and keylog, keys on a large table), whose cost has no bound on a live node. Never against a production node"
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
    <*> testSideOptions
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
        <> help "no request is made, to either side (an in-process one too), while this file exists"
        ))
    <*> strOption
        ( long "report"
        <> metavar "FILE"
        <> help "where to write the JSON report; a text summary goes to stdout"
        )
    <*> option (eitherReader positiveSeconds)
        ( long "oracle-local-timeout"
        <> metavar "SECONDS"
        <> value 30
        <> showDefault
        <> help "how long a /local on the oracle (tip probes included) waits for its answer. A request without an answer is never sent again: it is skipped as no-oracle-answer"
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
            [ "Exit codes: 0 pass; 4 partial pass (no difference, but not all 7 classes); 2 fail (a difference); 3 void (rerun); 1 bad arguments or a crash."
            , "--test-db: use a copy or a stopped node's files only (Pact opens the SQLite files read-write)."
            ])
        )
    let logger = genericLogger Info (T.hPutStrLn stderr)
        v = _optVersion o
        coverage = genesisCoverage v (_optChains o) (_optHeights o)
        pactChains = case _optTest o of
            TestUrl _ -> Nothing
            TestDb td -> Just (_tdPactChains td)
        classCov = classCoverage pactChains coverage
        settings = case _optTest o of
            TestUrl _ -> []
            TestDb td -> localSettingsText (_tdLocal td)
    hl <- newHostLimiters (_optRate o) (_optPauseFile o)
    oracle <- Side "oracle" (_optOracle o) <$> limiterFor hl (_optOracle o)
    (reqs, classes, gen) <- case _optSource o of
        RequestFile f -> (\rs -> (rs, [], id)) <$> readRequests f
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
                    , _ssPactChains = pactChains
                    }
                targets = sampleTargets spec
            logFunctionText logger Info $ "generating " <> T.pack (show (length targets)) <> " requests"
            (rs, skips) <- generateRequestsSkipping (GenOptions (_genSameSnapshot g)) api pset (_genSeed g) targets
            forM_ skips $ \(GenSkip (cls, cid, h) k e) -> logFunctionText logger Warn $
                "target skipped at generation (" <> requestClassText cls <> ", chain " <> toText cid
                <> ", height " <> T.pack (show h) <> "): " <> k <> ": " <> e
            return (rs, _genClasses g, reportGeneration (length targets) [ (c, k) | GenSkip (c, _, _) k _ <- skips ])
    let runOpts = defaultRunOptions { _roOracleLocalTimeout = 1_000_000 * _optOracleLocalTimeout o }
        compareWith test = runCompareClasses logger runOpts classCov oracle test reqs
    rep0 <- case _optTest o of
        TestUrl url -> limiterFor hl url >>= compareWith . Side "test" url
        TestDb td -> withInProcessSide logger (inProcessSpec o td) compareWith
    let rep = reportSettings settings $ gen $ reportExpectClasses classes rep0
    writeReport (_optReport o) rep
    T.putStr (reportSummary rep)
    exitWith (gateExitCode (reportVerdict rep))

-- | The in-process test side of @--test-db@.
--
inProcessSpec :: Options -> DbSide -> InProcessSpec
inProcessSpec o td = InProcessSpec
        { _ipDbDir = _tdDir td
        , _ipVersion = _optVersion o
        , _ipPactChains = _tdPactChains td
        , _ipTuning = if _tdZstd td then payloadZstdTuning else defaultTuning
        , _ipLocal = _tdLocal td
        , _ipPauseFile = _optPauseFile o
        }
