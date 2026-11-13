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

import Control.Monad (forM_)
import Data.Aeson.Types (parseEither)

import Data.Aeson
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Word

import Options.Applicative

import System.IO (stderr)
import System.LogLevel (LogLevel(..))

import Chainweb.ChainId
import Chainweb.Logger (genericLogger)
import Chainweb.Storage.Table.RocksDB.Tuning
import Chainweb.StorageTools.Measure.Cold
import Chainweb.StorageTools.Measure.Payload

data Command
    = PayloadSize FilePath FilePath
    | PayloadRead FilePath RocksDbTuning Int Word64
    | Cold ColdBuildSpec Int Word64 (Maybe FilePath)
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

tuningReader :: ReadM RocksDbTuning
tuningReader = eitherReader $ \s -> case s of
    "default" -> Right defaultTuning
    "zstd" -> Right payloadZstdTuning
    "cold" -> Right coldStoreTuning
    _ -> Left $ "unknown tuning: " <> s <> " (default, zstd or cold)"

commands :: Parser Command
commands = hsubparser $ mconcat
    [ cmd "payload-size" "size of each table on disk, under the default and the zstd tuning"
        $ PayloadSize
            <$> strOption (long "db" <> metavar "DIR" <> help "the database; flush it before, the write ahead log is not read")
            <*> strOption (long "work" <> metavar "DIR" <> help "where the copies are written; removed afterwards")
    , cmd "payload-read" "latency of warm payload reads"
        $ PayloadRead
            <$> strOption (long "db" <> metavar "DIR")
            <*> option tuningReader (long "tuning" <> metavar "TUNING" <> value defaultTuning <> help "default, zstd or cold; default: default")
            <*> option auto (long "samples" <> metavar "N" <> value 1000 <> showDefault)
            <*> option auto (long "seed" <> metavar "SEED" <> value 1 <> showDefault)
    , cmd "cold" "copy one chain's Pact SQLite file into a cold store (spec 4.3), measure its size and its point reads"
        $ Cold
            <$> (ColdBuildSpec
                <$> strOption (long "sqlite" <> metavar "FILE" <> help "the chain's Pact SQLite file; opened read-only")
                <*> option chainReader (long "chain" <> metavar "CHAIN")
                <*> optional (some (strOption (long "table" <> metavar "NAME" <> help "copy only this table (repeatable; TransactionIndex included); default: all")))
                <*> strOption (long "out" <> metavar "DIR" <> help "the cold store; must not exist")
                <*> option auto (long "batch-bytes" <> metavar "N" <> value defaultBatchBytes <> showDefault))
            <*> option auto (long "samples" <> metavar "N" <> value 1000 <> showDefault <> help "cold reads, checked against SQLite")
            <*> option auto (long "seed" <> metavar "SEED" <> value 1 <> showDefault)
            <*> optional (strOption (long "json" <> metavar "FILE" <> help "write {sizes, latency} as JSON, the input of project"))
    , cmd "project" "project the cold-store measurements over all 20 chains: the R or S verdict"
        $ fmap Project $ ProjectArgs
            <$> strOption (long "chain3" <> metavar "FILE" <> help "JSON of cw-measure cold on chain 3")
            <*> strOption (long "chain19" <> metavar "FILE" <> help "JSON of cw-measure cold on chain 19")
            <*> strOption (long "chain0-walk" <> metavar "FILE" <> help "the report's SQLite walk of chain 0 (cw0.json)")
            <*> option auto (long "others-today-bytes" <> metavar "N" <> help "summed size of the SQLite files of the 17 chains other than 0, 3 and 19")
            <*> optional (option auto (long "chain0-ratio" <> metavar "X" <> help "real-engine ratio for chain 0; default: chain 19's measured ratio"))
            <*> option auto (long "chain0-hot-bytes" <> metavar "N" <> value specChain0HotBytes <> showDefault <> help "chain 0's anchors and system tables (spec 4.3)")
            <*> optional (strOption (long "json" <> metavar "FILE" <> help "write the projection as JSON"))
    ]
  where
    cmd name desc p = command name (info p (progDesc desc))

main :: IO ()
main = do
    c <- execParser $ info (commands <**> helper) (fullDesc <> progDesc "Storage measurements")
    case c of
        PayloadSize db work -> T.putStr . renderPayloadSizes =<< measurePayloadSizes db work
        PayloadRead db tuning n seed -> T.putStr . renderLatencies =<< measurePayloadReads db tuning n seed
        Cold spec n seed json -> do
            let logger = genericLogger Info (T.hPutStrLn stderr)
            sizes <- buildColdFromSqlite logger spec
            T.putStr (renderColdSizes sizes)
            latency <- measureColdReads (_cbSqlite spec) (_cbOut spec) n seed
            T.putStr (renderColdLatency latency)
            forM_ json $ \f -> BL.writeFile f $ encode $ object
                [ "sizes" .= sizes, "latency" .= latency ]
        Project pa -> do
            (c3, l3) <- readCold (_paChain3 pa)
            (c19, l19) <- readCold (_paChain19 pa)
            walk <- either fail return . chain0LogicalFromWalk =<< BL.readFile (_paChain0Walk pa)
            let (ratio, source) = case _paChain0Ratio pa of
                    Just r -> (r, "given" :: T.Text)
                    Nothing -> (coldRatio c19, "chain 19 measured")
                inputs = ProjectionInputs
                    { _piChain0Logical = walk
                    , _piChain0Ratio = ratio
                    , _piChain3 = c3
                    , _piChain19 = c19
                    , _piHot = _paChain0Hot pa
                    , _piOthersTodayBytes = _paOthersToday pa
                    }
                p = projectCold inputs
            T.putStrLn $ "chain 0 ratio: " <> T.pack (show ratio) <> " (" <> source <> ")"
            T.putStr (renderProjection p)
            -- spec 11.3: option S if the size or the cold-read latency misses
            let overall = maximum [_projVerdict p, latencyVerdict l3, latencyVerdict l19]
            T.putStrLn $ "latency verdict: chain 3 " <> T.pack (show (latencyVerdict l3))
                <> ", chain 19 " <> T.pack (show (latencyVerdict l19))
            T.putStrLn $ "overall verdict: " <> T.pack (show overall)
            forM_ (_paJson pa) $ \f -> BL.writeFile f $ encode $ object
                [ "projection" .= p
                , "chain0Ratio" .= ratio
                , "chain0RatioSource" .= source
                , "chain0LogicalBytes" .= sum [ k + v | (k, v) <- foldr (:) [] walk ]
                , "latencyVerdictChain3" .= latencyVerdict l3
                , "latencyVerdictChain19" .= latencyVerdict l19
                , "overallVerdict" .= overall
                ]
  where
    readCold f = BL.readFile f >>= \bytes -> case eitherDecode bytes of
        Left e -> fail $ f <> ": " <> e
        Right o -> either (fail . ((f <> ": ") <>)) return $ flip parseEither o $ \v ->
            withObject "cold" (\x -> (,) <$> x .: "sizes" <*> x .: "latency") v
