{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Main
--
-- Measures the size on disk of the payload, header and metadata tables of a
-- database under different RocksDB tunings, and the latency of payload reads.
--
module Main (main) where

import qualified Data.Text.IO as T
import Data.Word

import Options.Applicative

import Chainweb.Storage.Table.RocksDB.Tuning
import Chainweb.StorageTools.Measure.Payload

data Command
    = PayloadSize FilePath FilePath
    | PayloadRead FilePath RocksDbTuning Int Word64

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
    ]
  where
    cmd name desc p = command name (info p (progDesc desc))

main :: IO ()
main = do
    c <- execParser $ info (commands <**> helper) (fullDesc <> progDesc "Storage measurements")
    case c of
        PayloadSize db work -> T.putStr . renderPayloadSizes =<< measurePayloadSizes db work
        PayloadRead db tuning n seed -> T.putStr . renderLatencies =<< measurePayloadReads db tuning n seed
