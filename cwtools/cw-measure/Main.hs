{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Main
--
-- Measures the size on disk of the payload, header and metadata tables of a
-- database under different RocksDB tunings, and the latency of payload reads.
-- Builds and measures the cold store of one chain's Pact SQLite file, and
-- projects the cold-store measurements over all chains (spec 4.3, 11.3).
--
module Main (main) where

import Control.Monad (forM_, unless, when)
import Data.Aeson.Types (parseEither)

import Data.Aeson
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Word

import Options.Applicative

import System.Exit (exitFailure)
import System.IO (stderr)
import System.LogLevel (LogLevel(..))
import Text.Printf (printf)

import Chainweb.ChainId
import Chainweb.Logger (genericLogger)
import Chainweb.Storage.Table.RocksDB.Tuning
import Chainweb.StorageTools.Measure.Cold
import Chainweb.StorageTools.Measure.Payload
import Chainweb.Version
import Chainweb.Version.Mainnet (mainnet)
import Chainweb.Version.Registry (findKnownVersion)

data Command
    = PayloadSize ChainwebVersion FilePath FilePath
    | PayloadRead FilePath RocksDbTuning Int Word64
    | Cold ColdBuildSpec ColdTuning Int Word64 Int (Maybe FilePath)
    | Project ProjectArgs

data ProjectArgs = ProjectArgs
    { _paChain3 :: FilePath
    , _paChain19 :: FilePath
    , _paChain0Walk :: FilePath
    , _paOthersToday :: Word64
    , _paChain0Ratio :: Maybe Double
    , _paChain0Hot :: Word64
    , _paJson :: Maybe FilePath
    }

chainReader :: ReadM ChainId
chainReader = eitherReader $ \s -> either (Left . show) Right $ chainIdFromText (T.pack s)

versionOption :: Parser ChainwebVersion
versionOption = option (eitherReader readVersion)
    ( long "version"
    <> metavar "NAME"
    <> value mainnet
    <> showDefaultWith (T.unpack . getChainwebVersionName . _versionName)
    <> help "chainweb version of the database; its chains name the header tables"
    )
  where
    readVersion s = case findKnownVersion (ChainwebVersionName $ T.pack s) of
        Just v -> Right v
        Nothing -> Left $ "unknown chainweb version: " <> s

tuningReader :: ReadM RocksDbTuning
tuningReader = eitherReader $ \s -> case s of
    "default" -> Right defaultTuning
    "zstd" -> Right payloadZstdTuning
    "cold" -> Right coldStoreTuning
    _ -> Left $ "unknown tuning: " <> s <> " (default, zstd or cold)"

nonNegative :: ReadM Int
nonNegative = eitherReader $ \s -> case reads s of
    [(n, "")] | n >= 0 -> Right n
    _ -> Left $ "expected a non-negative integer: " <> s

boolReader :: ReadM Bool
boolReader = eitherReader $ \case
    "true" -> Right True
    "false" -> Right False
    s -> Left $ "expected true or false: " <> s

-- | The RocksDB options of a cold run; the defaults are M0's.
coldTuningOptions :: Parser ColdTuning
coldTuningOptions = ColdTuning
    <$> option nonNegative (long "bloom-bits" <> metavar "N" <> value (_ctBloomBits d) <> showDefault <> help "bloom filter bits per key; 0 writes no filter")
    <*> option (eitherReader pos) (long "block-size" <> metavar "BYTES" <> value (_ctBlockSize d) <> showDefault)
    <*> optional (option nonNegative (long "bottommost-zstd-level" <> metavar "N" <> help "zstd level of the bottommost level; default: off, the level of the other levels"))
    <*> option boolReader (long "dynamic-level-bytes" <> metavar "true|false" <> value (_ctDynamicLevelBytes d) <> showDefault <> help "level_compaction_dynamic_level_bytes")
    <*> option boolReader (long "cache-index-and-filter" <> metavar "true|false" <> value (_ctCacheIndexAndFilter d) <> showDefault <> help "index and filter blocks in the block cache, level 0 pinned")
    <*> option positive (long "cold-cache-bytes" <> metavar "N" <> value (_ctColdCacheBytes d) <> showDefault <> help "block cache of the cold passes (direct reads). With --cache-index-and-filter true the index and filter blocks compete for it; the node's cache is 1 GiB, so read cold figures of those runs with the size in mind")
  where
    d = defaultColdTuning
    pos s = case reads s of
        [(n, "")] | n > 0 -> Right n
        _ -> Left $ "expected a positive integer: " <> s
    positive = eitherReader pos

commands :: Parser Command
commands = hsubparser $ mconcat
    [ cmd "payload-size" "size of each table on disk, under the default and the zstd tuning"
        $ PayloadSize
            <$> versionOption
            <*> strOption (long "db" <> metavar "DIR" <> help "the database, opened read-only: a copy or a snapshot only, never a production node's files. Flush it before; a write ahead log is replayed in memory only")
            <*> strOption (long "work" <> metavar "DIR" <> help "where the copies are written; removed afterwards")
    , cmd "payload-read" "latency of warm payload reads"
        $ PayloadRead
            <$> strOption (long "db" <> metavar "DIR" <> help "the database, opened read-only: a copy or a snapshot only, never a production node's files")
            <*> option tuningReader (long "tuning" <> metavar "TUNING" <> value defaultTuning <> help "default, zstd or cold; default: default")
            <*> option auto (long "samples" <> metavar "N" <> value 1000 <> showDefault)
            <*> option auto (long "seed" <> metavar "SEED" <> value 1 <> showDefault)
    , cmdF "cold" "copy one chain's Pact SQLite file into a cold store (spec 4.3), measure its size and its point reads"
        coldFooter
        $ Cold
            <$> (ColdBuildSpec
                <$> strOption (long "sqlite" <> metavar "FILE" <> help "the chain's Pact SQLite file, opened read-only: a copy or a snapshot only, never a production node's files. Every table is read in long statements, which on a live WAL database would hold back the node's checkpoints")
                <*> option chainReader (long "chain" <> metavar "CHAIN")
                <*> optional (some (strOption (long "table" <> metavar "NAME" <> help "copy only this table (repeatable; TransactionIndex included); default: all")))
                <*> strOption (long "out" <> metavar "DIR" <> help "the cold store; must not exist")
                <*> option auto (long "batch-bytes" <> metavar "N" <> value defaultBatchBytes <> showDefault))
            <*> coldTuningOptions
            <*> option auto (long "samples" <> metavar "N" <> value 1000 <> showDefault <> help "cold reads, checked against SQLite")
            <*> option auto (long "seed" <> metavar "SEED" <> value 1 <> showDefault)
            <*> option nonNegative (long "txid-reads" <> metavar "N" <> value 0 <> showDefault <> help "time N by-txid reads on sampled user-table txids (the T get, the I gets and the exact R gets), warm and after the direct-I/O reopen, each checked against SQLite's WHERE txid = ? rows")
            <*> optional (strOption (long "json" <> metavar "FILE" <> help "write {sizes, latency, tuning, buildSeconds, txidReadLatency} as JSON; project reads sizes and latency"))
    , cmdF "project" "project the cold-store measurements over all 20 chains: the R or S verdict"
        projectFooter
        $ fmap Project $ ProjectArgs
            <$> strOption (long "chain3" <> metavar "FILE" <> help "JSON of cw-measure cold on chain 3")
            <*> strOption (long "chain19" <> metavar "FILE" <> help "JSON of cw-measure cold on chain 19")
            <*> strOption (long "chain0-walk" <> metavar "FILE" <> help "the report's SQLite walk of chain 0 (cw0.json)")
            <*> option auto (long "others-today-bytes" <> metavar "N" <> help "summed size of the SQLite files of the 17 chains other than 0, 3 and 19")
            <*> optional (option auto (long "chain0-ratio" <> metavar "X" <> help "real-engine ratio for chain 0's history tables; default: chain 19's history ratio, with the transaction index, SYS:Modules and SYS:ModuleSources out of both sides as in chain 0's history"))
            <*> option auto (long "chain0-hot-bytes" <> metavar "N" <> value specChain0HotBytes <> showDefault <> help "chain 0's anchors and system tables (spec 4.3)")
            <*> optional (strOption (long "json" <> metavar "FILE" <> help "write the projection as JSON"))
    ]
  where
    cmd name desc p = command name (info p (progDesc desc))
    cmdF name desc foot p = command name (info p (progDesc desc <> footer foot))

    coldFooter = unwords
        [ "Exits non-zero if any cold read differs from SQLite."
        , "Memory: Task 17 runs it as cw-measure +RTS -M5g -N4 -RTS cold ..."
        , "--json writes {\"sizes\": ColdSizes, \"latency\": ColdLatency, \"tuning\": the settings used,"
        , "\"buildSeconds\", and with --txid-reads \"txidReadLatency\": {reads, coldReads, rows, mismatches,"
        , "warmP50, warmP99, coldP50, coldP99} in microseconds}."
        , "ColdSizes: {chain, sqliteFileBytes, totalSst, hotKeptBytes, hotAnchorBytes,"
        , "keyspaceSst: {I, K, N, R, T, X}, tables: {NAME: {versions, distinctKeys,"
        , "keyBytes, valueBytes, sstBytes, sqliteBytes}}}."
        , "ColdLatency: {reads, coldReads, found, mismatches, warmP50Us, warmP99Us,"
        , "coldP50Us, coldP99Us, coldMaxUs}. Bytes are integers, latencies microseconds."
        ]

    projectFooter = unwords
        [ "Chain 0 = history bytes / ratio + transaction-index entries x chain 19's X bytes"
        , "per entry + hot bytes. Fails if a cold run had a read mismatch."
        , "--json writes {projection: {chain0GB, chain0TxIndexGB, chain19GB, chain3GB,"
        , "othersGB, chain3Ratio, pactTotalGB, limitGB, sizeVerdict}, verdict: {size,"
        , "latencyChain3, latencyChain19, overall}, chain0Ratio, chain0RatioSource,"
        , "chain0HistoryLogicalBytes, chain0TxIndexEntries, chain19TxIndexBytesPerEntry,"
        , "wholeStoreRatio: {chain0Ratio, chain0GB, pactTotalGB}}. A verdict is \"R\" or \"S\"."
        ]

main :: IO ()
main = do
    c <- execParser $ info (commands <**> helper) (fullDesc <> progDesc "Storage measurements")
    case c of
        PayloadSize v db work -> T.putStr . renderPayloadSizes =<< measurePayloadSizes v db work
        PayloadRead db tuning n seed -> T.putStr . renderLatencies =<< measurePayloadReads db tuning n seed
        Cold spec tuning n seed txidReads json -> do
            let logger = genericLogger Info (T.hPutStrLn stderr)
            run <- runColdMeasure logger tuning spec n seed txidReads
            T.putStrLn (renderColdTuning tuning)
            T.putStr (renderColdSizes (_crSizes run))
            T.putStrLn $ "build: " <> T.pack (printf "%.1f s" (_crBuildSeconds run))
            T.putStr (renderColdLatency (_crLatency run))
            forM_ (_crTxid run) $ T.putStr . renderTxidLatency
            forM_ json $ \f -> BL.writeFile f (encode run)
            -- a mismatch means the cold store or the measurement is wrong
            let bad = _clMismatches (_crLatency run) + maybe 0 _tlMismatches (_crTxid run)
            when (bad > 0) $ do
                T.hPutStrLn stderr $ "error: " <> T.pack (show bad)
                    <> " cold reads differ from SQLite"
                exitFailure
        Project pa -> do
            (c3, l3) <- readCold (_paChain3 pa)
            (c19, l19) <- readCold (_paChain19 pa)
            walk <- either fail return . chain0FromWalk =<< BL.readFile (_paChain0Walk pa)
            perEntry <- either
                (\e -> fail $ e <> " (--chain0-walk " <> _paChain0Walk pa <> ", --chain19 " <> _paChain19 pa <> ")")
                return (chain0TxIndexCost walk c19)
            let (ratio, source) = case _paChain0Ratio pa of
                    Just r -> (r, "given" :: T.Text)
                    Nothing -> (historyRatio c19, "chain 19 history tables; X, SYS:Modules and SYS:ModuleSources out of both sides")
                inputsAt r = ProjectionInputs
                    { _piChain0Logical = _c0wHistory walk
                    , _piChain0TxIndexEntries = _c0wTxIndexEntries walk
                    , _piChain0Ratio = r
                    , _piChain3 = c3
                    , _piChain19 = c19
                    , _piHot = _paChain0Hot pa
                    , _piOthersTodayBytes = _paOthersToday pa
                    }
                p = projectCold (inputsAt ratio)
                -- sensitivity: chain 19's whole-store ratio (X in both sides)
                pWhole = projectCold (inputsAt (coldRatio c19))
            unless (ratio > 0 && not (isInfinite ratio)) $ fail $ "invalid chain 0 ratio: " <> show ratio
            v <- either fail return (verdict p l3 l19)
            T.putStrLn $ "chain 0 ratio: " <> T.pack (show ratio) <> " (" <> source <> ")"
            T.putStrLn $ "chain 0 transaction index: " <> T.pack (show (_c0wTxIndexEntries walk))
                <> " entries at " <> T.pack (show perEntry) <> " bytes (chain 19's X)"
            T.putStr (renderProjection p)
            T.putStrLn $ "at chain 19's whole-store ratio " <> T.pack (show (coldRatio c19))
                <> ": chain 0 " <> T.pack (show (_projChain0GB pWhole))
                <> " GB, Pact state total " <> T.pack (show (_projPactTotalGB pWhole)) <> " GB"
            T.putStrLn $ "latency verdict: chain 3 " <> T.pack (show (_vLatencyChain3 v))
                <> ", chain 19 " <> T.pack (show (_vLatencyChain19 v))
            T.putStrLn $ "overall verdict: " <> T.pack (show (_vOverall v))
            forM_ (_paJson pa) $ \f -> BL.writeFile f $ encode $ object
                [ "projection" .= p
                , "verdict" .= v
                , "chain0Ratio" .= ratio
                , "chain0RatioSource" .= source
                , "chain0HistoryLogicalBytes" .= sum (uncurry (+) <$> _c0wHistory walk)
                , "chain0TxIndexEntries" .= _c0wTxIndexEntries walk
                , "chain19TxIndexBytesPerEntry" .= perEntry
                , "wholeStoreRatio" .= object
                    [ "chain0Ratio" .= coldRatio c19
                    , "chain0GB" .= _projChain0GB pWhole
                    , "pactTotalGB" .= _projPactTotalGB pWhole
                    ]
                ]
  where
    readCold f = BL.readFile f >>= \bytes -> case eitherDecode bytes of
        Left e -> fail $ f <> ": " <> e
        Right o -> either (fail . ((f <> ": ") <>)) return $ flip parseEither o $ \v ->
            withObject "cold" (\x -> (,) <$> x .: "sizes" <*> x .: "latency") v
