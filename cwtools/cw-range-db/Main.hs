{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Main
--
-- Builds, inspects, compares and corrupts range databases: small RocksDB
-- databases with a slice of the headers, payloads and cuts of a node, copied
-- through the read-only service API of the node.
--
-- Requests are rate limited across all threads of the process. While the pause
-- file exists, no request is made.
--
module Main (main) where

import Control.Monad

import qualified Data.ByteString.Char8 as B8
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Word

import Options.Applicative

import System.Directory (doesFileExist)
import System.Exit
import System.FilePath ((</>))
import System.IO
import System.LogLevel

-- internal modules

import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.Storage.Table.RocksDB
import Chainweb.StorageTools.Http
import Chainweb.StorageTools.RangeDb
import Chainweb.Version
import Chainweb.Version.Mainnet (mainnet)
import Chainweb.Version.Registry

-- -------------------------------------------------------------------------- --
-- Options

data Service = Service
    { _serviceUrl :: !T.Text
    , _serviceVersion :: !ChainwebVersion
    , _serviceRate :: !Double
    , _servicePauseFile :: !(Maybe FilePath)
    }

data Command
    = Range Service FilePath BlockHeight
    | Sample Service FilePath Word64 Int
    | Contiguous Service FilePath ChainId BlockHeight BlockHeight
    | ShowInfo FilePath
    | Diff ChainwebVersion FilePath FilePath [B8.ByteString]
    | Corrupt FilePath B8.ByteString Int

versionOption :: Parser ChainwebVersion
versionOption = option (eitherReader readVersion)
    ( long "version"
    <> metavar "NAME"
    <> value mainnet
    <> showDefaultWith (T.unpack . getChainwebVersionName . _versionName)
    <> help "chainweb version of the databases and of the service"
    )
  where
    readVersion s = case findKnownVersion (ChainwebVersionName $ T.pack s) of
        Just v -> Right v
        Nothing -> Left $ "unknown chainweb version: " <> s

serviceOptions :: Parser Service
serviceOptions = Service
    <$> strOption
        ( long "service"
        <> metavar "URL"
        <> help "base URL of the service API, for instance https://host:1848"
        )
    <*> versionOption
    <*> option auto
        ( long "rate"
        <> metavar "REQUESTS_PER_SECOND"
        <> value 20
        <> showDefault
        <> help "requests per second, across all threads"
        )
    <*> optional (strOption
        ( long "pause-file"
        <> metavar "FILE"
        <> help "no request is made while this file exists"
        ))

heightReader :: ReadM BlockHeight
heightReader = fromIntegral <$> (auto :: ReadM Word64)

dbOption :: Parser FilePath
dbOption = strOption (long "db" <> metavar "DIR" <> help "the range database")

-- | A database that is only read.
--
readDbOption :: String -> String -> Parser FilePath
readDbOption name what = strOption
    ( long name
    <> metavar "DIR"
    <> help (what <> ", opened read-only: a copy or a snapshot only, never a production node's files")
    )

commands :: Parser Command
commands = hsubparser $ mconcat
    [ cmd "range" "headers and payloads of all chains up to a height, and one cut"
        $ Range <$> serviceOptions <*> dbOption
            <*> option heightReader (long "max-height" <> metavar "HEIGHT" <> help "the height of the cut")
    , cmd "sample" "payloads of a random sample of (chain, height) pairs across the history"
        $ Sample <$> serviceOptions <*> dbOption
            <*> option auto (long "seed" <> metavar "SEED")
            <*> option auto (long "count" <> metavar "N" <> help "number of (chain, height) pairs")
    , cmd "contiguous" "payloads of one chain in a range of heights"
        $ Contiguous <$> serviceOptions <*> dbOption
            <*> option (unsafeChainId <$> auto) (long "chain" <> metavar "CHAIN")
            <*> option heightReader (long "from" <> metavar "HEIGHT")
            <*> option heightReader (long "to" <> metavar "HEIGHT" <> help "inclusive")
    , cmd "info" "highest cut and sizes of a range database (opens it read-only; a write ahead log is replayed in memory)"
        $ ShowInfo <$> readDbOption "db" "the range database"
    , cmd "diff" "compare two databases (exit code 1 if they differ)"
        $ Diff <$> versionOption
            <*> readDbOption "left" "a database"
            <*> readDbOption "right" "a database"
            <*> many (B8.pack <$> strOption
                ( long "namespace"
                <> metavar "NAMESPACE"
                <> help "a table to compare, for instance BlockPayload2 or BlockHeader/0/header; default: all header and payload tables"
                ))
    , cmd "corrupt" "flip the last byte of the value of the n-th entry of a table"
        $ Corrupt <$> dbOption
            <*> (B8.pack <$> strOption (long "namespace" <> metavar "NAMESPACE"))
            <*> option auto (long "index" <> metavar "N" <> help "counted from 0, in key order")
    ]
  where
    cmd name desc p = command name (info p (progDesc desc))

-- -------------------------------------------------------------------------- --
-- Main

main :: IO ()
main = do
    hSetBuffering stderr LineBuffering
    c <- execParser $ info (commands <**> helper)
        ( fullDesc
        <> progDesc "Range databases of a chainweb node"
        )
    run c

logger :: GenericLogger
logger = genericLogger Info (T.hPutStrLn stderr)

withService :: Service -> (ServiceApi -> IO a) -> IO a
withService (Service url v rate pause) act = do
    registerVersion v
    lim <- newLimiter rate pause
    api <- newServiceApi url v lim
    act api

-- | Writes the new data to SST files before the database is closed. A reader
-- that opens the database read-only replays the write ahead log anyway; the
-- flush leaves a database that needs no replay.
--
writing :: FilePath -> (RocksDb -> IO RangeSummary) -> IO ()
writing path act = withRocksDb path modernDefaultOptions $ \db -> do
    s <- act db
    flushRocksDb db
    print s

requireDb :: FilePath -> IO ()
requireDb path = do
    ok <- doesFileExist (path </> "CURRENT")
    unless ok $ die $ "not a RocksDB database: " <> path

run :: Command -> IO ()
run = \case
    Range s db h -> withService s $ \api ->
        writing db $ \rdb -> buildRangeDb logger api (RangeSpec h) rdb
    Sample s db seed n -> withService s $ \api ->
        writing db $ \rdb -> sampleRangeDb logger api seed n rdb
    Contiguous s db c a b -> withService s $ \api ->
        writing db $ \rdb -> contiguousRangeDb logger api c a b rdb
    ShowInfo db -> do
        requireDb db
        T.putStr =<< rangeDbInfo db
    Diff v l r nss -> do
        registerVersion v
        requireDb l
        requireDb r
        let namespaces = if null nss then allNamespaces v else nss
        d <- withReadOnlyRocksDb l modernDefaultOptions $ \ldb ->
            withReadOnlyRocksDb r modernDefaultOptions $ \rdb ->
                diffRangeDbs ldb rdb namespaces
        T.putStr $ renderDiff d
        unless (diffIsEmpty d) $ exitWith (ExitFailure 1)
    Corrupt db ns n -> do
        requireDb db
        withRocksDb db modernDefaultOptions $ \rdb -> do
            corruptEntry rdb ns n
            flushRocksDb rdb
        putStrLn $ "corrupted entry " <> show n <> " of " <> B8.unpack ns
