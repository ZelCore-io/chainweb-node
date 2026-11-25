{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
-- a file with one JSON request per line instead. With @--save-requests FILE@,
-- the generated requests are written to such a file before the comparison
-- starts. Generation logs its progress (a line every 1000 targets, at the
-- latest every 5 minutes, and at the end), and so does the comparison.
--
-- The test side is a node's service API (@--test URL@), or the service API
-- served in process from a node's database directory (@--test-db DIR@, see
-- "Chainweb.StorageTools.Compare.InProcess"). In-process, only the
-- @--pact-chains@ get a Pact service: the classes Pact answers (@/local@,
-- History, preflight, @/poll@, @/spv@) are covered on those chains only, and
-- their requests on other chains are skipped as not covered, never sent.
--
-- With @--pact-history-store split@ or @shadow@, the in-process side opens a
-- split or shadow database as a node does (the cold store read-only, no
-- freezer), and each covered chain's verified frontier height joins the
-- sampling boundaries, so that a fixed share of the targets sits within 5
-- heights of it (spec 10.1). @--frontier-targets N@, instead of @--count@,
-- generates only such targets, as a supplement to a saved request set.
-- @--frontier-from DIR@ takes the frontier heights of another database, so
-- that a sqlite-mode run is sampled and split at the same heights as its
-- split twin. The report counts, per class, the targets below and at or
-- above the frontier and gives the test side's latency (p50, p99, max in
-- microseconds) on each side of it.
--
-- Requests and the oracle lookups that build them are rate limited per host
-- across all threads; two sides on one host share the limit. While the pause
-- file exists, no request is made, to either side.
--
-- A target whose oracle lookups fail is dropped and counted as skipped at
-- generation; it counts toward the 1% budget of inconclusive outcomes over the
-- requests sent.
--
-- A poll or SPV target without a transaction within 1000 blocks from the
-- sampled height is redrawn from its class until the class has its number of
-- targets, with at most 10 times that many attempts. The skips are recorded
-- per class in the report; they are not inconclusive, and a class that the
-- attempts leave short is reported short, not void.
--
-- The soft limit of open files is raised to the hard limit at start, and both
-- are logged.
--
-- Only a run of all 7 classes can pass: one of fewer classes (@--classes@),
-- one with a class that never got a same 2xx answer (identical errors only),
-- one without a poll or SPV request with a real key compared, or one with a
-- Pact set entry that expects success and never got a same success, is at
-- best a partial pass (exit code 4), never a pass.
--
-- Exit codes: 0 if the run passes, 4 if it partially passes (no difference,
-- but not every class), 2 if it fails (a difference), 3 if it is void and
-- must be rerun, 1 on bad arguments or a crash.
--
module Main (main) where

import Control.Exception (IOException, catch)
import Control.Monad
import Data.Function ((&))
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
import Chainweb.Pact.Backend.Store.Config (PactHistoryConfig(..), PactHistoryStore(..), defaultPactHistoryConfig, pactHistoryStoreReader, pactHistoryStoreToText)
import Chainweb.Storage.Table.RocksDB.Tuning (defaultTuning, payloadZstdTuning)
import Chainweb.StorageTools.Compare.InProcess
import Chainweb.StorageTools.Compare.PactSet
import Chainweb.StorageTools.Compare.Report
import Chainweb.StorageTools.Compare.Requests
import Chainweb.StorageTools.Compare.Rules (parseRequestFile, renderRequestFile)
import Chainweb.StorageTools.Compare.Run
import Chainweb.StorageTools.Compare.Sample
import Chainweb.StorageTools.Compare.Types
import Chainweb.StorageTools.FileLimit
import Chainweb.StorageTools.Measure.Split (readFrontierHeights)
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
    , _optRunOptions :: !RunOptions
        -- ^ 'runOptionsParser': @--oracle-local-timeout@
    , _optFrontierFrom :: !(Maybe FilePath)
        -- ^ @--frontier-from DIR@: the frontier heights of another database
    }

data TestSide
    = TestUrl !T.Text
    | TestDb !DbSide

data DbSide = DbSide
    { _tdDir :: !FilePath
    , _tdPactChains :: ![ChainId]
    , _tdZstd :: !Bool
    , _tdLocal :: !LocalSettings
    , _tdPactHistory :: !PactHistoryStore
    , _tdColdBlockCache :: !Int
    }

data Source
    = Generated !Generate
    | RequestFile !FilePath

data Generate = Generate
    { _genClasses :: ![RequestClass]
    , _genCount :: !GenCount
    , _genSeed :: !Word64
    , _genPactSet :: !(Maybe FilePath)
    , _genSameSnapshot :: !Bool
    , _genSave :: !(Maybe FilePath)
    }

-- | @--count N@: N sampled targets; @--frontier-targets N@: N targets within
-- 5 heights of a frontier only, as a supplement to a saved request set.
data GenCount = Count !Int | FrontierTargets !Int

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
        <> help "serve the test side in process from this node database directory (the one that holds rocksDb and sqlite, or its parent); RocksDB is opened read-only, but Pact opens the SQLite files read-write and rewinds them to the newest header at start: a copy or a snapshot only, never a production node's files"
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
    <*> option pactHistoryStoreReader
        ( long "pact-history-store"
        <> metavar "sqlite|split|shadow"
        <> value HistorySqlite
        <> showDefaultWith (T.unpack . pactHistoryStoreToText)
        <> help "with --test-db: the Pact history store of the database. sqlite refuses a split or shadow layout; split and shadow open it as a node does, the cold store read-only with the node's block cache and no freezer, and add each chain's verified frontier height to the sampling boundaries"
        )
    <*> option (eitherReader positiveBytes)
        ( long "pact-cold-block-cache"
        <> metavar "BYTES"
        <> value (_phColdBlockCacheBytes defaultPactHistoryConfig)
        <> showDefault
        <> help "with --test-db and --pact-history-store split or shadow: the block cache of the cold store, in bytes"
        )

-- | A positive number of bytes.
--
positiveBytes :: String -> Either String Int
positiveBytes s = case reads s of
    [(n, "")] | n > 0 -> Right n
    _ -> Left $ "expected a positive number of bytes: " <> s

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
    <*> (Count <$> option auto
            ( long "count"
            <> metavar "N"
            <> help "the number of requests to generate"
            )
        <|> FrontierTargets <$> option auto
            ( long "frontier-targets"
            <> metavar "N"
            <> help "instead of --count: generate N targets, each within 5 heights of a frontier (of --test-db in split or shadow mode, or of --frontier-from), as a supplement to a saved request set"
            ))
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
    <*> optional (strOption
        ( long "save-requests"
        <> metavar "FILE"
        <> help "write the generated requests to this file, one JSON object per line, in the format of --requests, before the comparison starts; a later run can compare them with --requests FILE, without generating them again (only the requests, not the skips and redraws of the generation, which a run from the file does not report)"
        ))

sourceOptions :: Parser Source
sourceOptions = RequestFile <$> strOption
        ( long "requests"
        <> metavar "FILE"
        <> help "compare the requests of this file, one JSON object per line, instead of generating them. Only the read requests of the 7 classes are allowed (GET header, payload and cut reads; POST header branch, payload batches, /local with rewindDepth={rewindDepth} and a target, /poll, /spv); any other line stops the run before anything is sent"
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
    <*> runOptionsParser
    <*> optional (strOption
        ( long "frontier-from"
        <> metavar "DIR"
        <> help "split the report below and above the verified frontier heights of this split or shadow database (its cold store is opened read-only), and sample near them, so that a sqlite-mode run is split at the same heights as its split twin"
        ))

-- -------------------------------------------------------------------------- --
-- Main

-- | The requests of @--requests FILE@. Only the read requests of the 7
-- classes are allowed ('allowedRequest'); the first line that does not decode
-- or is not allowed stops the run before anything is sent.
--
readRequests :: ChainwebVersion -> FilePath -> IO [Request]
readRequests v f = B8.readFile f >>= either (\e -> die (f <> ":" <> e)) return . parseRequestFile (apiPrefix v)

main :: IO ()
main = do
    hSetBuffering stderr LineBuffering
    o <- execParser $ info (options <**> helper)
        ( fullDesc
        <> progDesc "Compare the answers of a test node with those of the oracle"
        <> footer (unwords
            [ "Exit codes: 0 pass; 4 partial pass (no difference, but not all 7 classes); 2 fail (a difference); 3 void (rerun); 1 bad arguments or a crash."
            , "--test-db: a copy or a snapshot only, never a production node's files (Pact opens the SQLite files read-write and rewinds them at start)."
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
                <> [("pactHistoryStore", pactHistoryStoreToText (_tdPactHistory td)), ("pactColdBlockCacheBytes", T.pack (show (_tdColdBlockCache td)))]
        covered = M.keys (let Coverage m = coverage in m)
    -- the frontier heights: of --frontier-from, else of a split or shadow
    -- test database; read before the side opens (its freezer is off, so they
    -- do not move)
    frontiers <- case (_optFrontierFrom o, _optTest o) of
        (Just d, _) -> readFrontierHeights d covered
        (Nothing, TestDb td) | _tdPactHistory td /= HistorySqlite -> readFrontierHeights (_tdDir td) covered
        _ -> return mempty
    unless (M.null frontiers) $ logFunctionText logger Info $ "frontier heights: " <> T.intercalate ", "
        [ toText c <> " " <> T.pack (show h) | (c, h) <- M.toList frontiers ]
    raiseFileLimit >>= logFunctionText logger Info . describeFileLimits
    hl <- newHostLimiters (_optRate o) (_optPauseFile o)
    oracle <- Side "oracle" (_optOracle o) <$> limiterFor hl (_optOracle o)
    (reqs, classes, gen) <- case _optSource o of
        RequestFile f -> (\rs -> (rs, [], id)) <$> readRequests v f
        Generated g -> do
            pset <- maybe (return defaultPactSet) loadPactSet (_genPactSet g)
            forM_ (_genPactSet g) $ \f -> logCustomSet logger f pset
            api <- oracleServiceApi hl v (_optOracle o)
            let spec = SampleSpec
                    { _ssSeed = _genSeed g
                    , _ssCount = case _genCount g of
                        Count n -> n
                        FrontierTargets n -> n
                    , _ssClasses = _genClasses g
                    , _ssCoverage = coverage
                    , _ssBoundaries = if _versionName v == _versionName mainnet then mainnetBoundaries else []
                    , _ssBoundaryShare = boundaryShare
                    , _ssHistoryMax = historyMaxHeight v
                    , _ssPactChains = pactChains
                    }
                    & withFrontierBoundaries (M.elems frontiers)
            -- with --frontier-targets, the redraws of poll and spv targets
            -- come from the windows near a frontier too
            (targets, redraws) <- case _genCount g of
                Count _ -> return (sampleTargets spec, redrawTargets spec)
                FrontierTargets n -> do
                    when (M.null frontiers) $ die
                        "--frontier-targets: no frontier height (use --test-db with --pact-history-store split or shadow, or --frontier-from)"
                    return (frontierTargets (M.elems frontiers) n spec, frontierRedrawTargets (M.elems frontiers) spec)
            logFunctionText logger Info $ "generating " <> T.pack (show (length targets)) <> " requests"
            progress <- newGenProgress (logFunctionText logger Info) (length targets)
            (rs, skips, draws) <- generateRequestsRedrawingP (Just progress) (GenOptions (_genSameSnapshot g)) api pset (_genSeed g)
                redraws targets
            forM_ skips $ \(GenSkip (cls, cid, h) k e) -> logFunctionText logger Warn $
                "target skipped at generation (" <> requestClassText cls <> ", chain " <> toText cid
                <> ", height " <> T.pack (show h) <> "): " <> k <> ": " <> e
            forM_ (_genSave g) $ saveRequests logger v rs
            let entries =
                    [ (c, _peName e)
                    | c <- _genClasses g
                    , e <- generatedEntries (_genSameSnapshot g) pset c
                    , expectsSuccess e
                    ]
            return
                ( rs
                , _genClasses g
                , reportExpectEntries entries
                    . reportGenerationDraws [ (c, _drRequested d, _drAttempts d, _drShort d) | (c, d) <- M.toList draws ]
                    . reportGeneration (length targets) [ (c, k) | GenSkip (c, _, _) k _ <- skips ]
                )
    let compareWith test = runCompareClasses logger (_optRunOptions o) classCov oracle test reqs
    rep0 <- case _optTest o of
        TestUrl url -> limiterFor hl url >>= compareWith . Side "test" url
        TestDb td -> withInProcessSide logger (inProcessSpec o td) compareWith
    let rep = reportFrontiers frontiers $ reportSettings settings $ gen $ reportExpectClasses classes rep0
    logFunctionText logger Info $ "verdict: " <> T.pack (show (reportVerdict rep))
    writeReport (_optReport o) rep
    T.putStr (reportSummary rep)
    exitWith (gateExitCode (reportVerdict rep))

-- | Writes the requests to the file (@--save-requests@) before the comparison
-- starts, and checks that @--requests@ would read them back: if it would not,
-- the file is still written and a warning logged, since the generation is
-- not to be lost over it. A file that cannot be written stops the run.
--
saveRequests :: GenericLogger -> ChainwebVersion -> [Request] -> FilePath -> IO ()
saveRequests logger v rs f = do
    let bytes = renderRequestFile rs
    B8.writeFile f bytes `catch` \(e :: IOException) ->
        die ("--save-requests " <> f <> ": " <> show e)
    case parseRequestFile (apiPrefix v) bytes of
        Right back | back == rs ->
            logFunctionText logger Info $ "saved " <> T.pack (show (length rs)) <> " requests to " <> T.pack f
        Right _ -> logFunctionText logger Warn $
            "saved " <> T.pack (show (length rs)) <> " requests to " <> T.pack f
            <> ", but --requests reads back a different list"
        Left e -> logFunctionText logger Warn $
            "saved " <> T.pack (show (length rs)) <> " requests to " <> T.pack f
            <> ", but --requests would refuse the file: " <> T.pack e

-- | Logs the entries of a custom Pact set (@--pact-set@) that may go to a
-- production oracle: those not marked sameSnapshotOnly, sent without
-- @--same-snapshot@ too.
--
logCustomSet :: GenericLogger -> FilePath -> PactSet -> IO ()
logCustomSet logger f pset = do
    let live = liveEntries pset
        logg = logFunctionText logger Info
    logg $ "Pact set " <> T.pack f <> ": " <> T.pack (show (length live)) <> " of "
        <> T.pack (show (length (_pactEntries pset)))
        <> " entries may be sent to a production oracle (not sameSnapshotOnly):"
    forM_ live $ \e -> logg $ "  " <> _peName e
        <> " [" <> T.intercalate "," (map requestClassText (_peClasses e)) <> "] "
        <> (case _peKind e of
                PactExec -> "exec " <> _peCode e
                PactCont -> "cont " <> _peCode e <> " step " <> T.pack (show (_peStep e)))

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
        , _ipPactHistory = _tdPactHistory td
        , _ipColdBlockCacheBytes = _tdColdBlockCache td
        }
