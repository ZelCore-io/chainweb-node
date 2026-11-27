{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.Freezer
-- License: See LICENSE file
--
-- The freezer step of the Pact history split (storage redesign, spec 6.2 and
-- 6.3): read the rows of whole blocks from SQLite, write them to the cold
-- store in one synced batch, verify them, publish the verified frontier, and
-- trim SQLite below it.
--
-- Crash safety (spec 6.3): a row leaves SQLite only after a synced, verified
-- copy exists in the cold store. 'freezeOnce' first finishes what a previous
-- run left: a written but unverified step is verified again and published,
-- and a trim cut short resumes at its stored position ('_cmTrimFrom'; deletes
-- are idempotent). A verification failure or a changed block hash at the
-- frontier halts the chain's freezer: SQLite is not trimmed and the node
-- keeps running on SQLite.
--
-- Scheduling (spec 6.5): 'runFreezer' runs the steps of one chain with the
-- node-wide semaphore held during read, write and verify; 'trimGate' decides
-- when a trim chunk may take the Pact thread; 'drainFreezer' runs the steps
-- to the end (@--only-sync-pact@).
--
-- All SQL is in "Chainweb.Pact.Backend.Store.SQLite".
--
module Chainweb.Pact.Backend.Store.Freezer
( -- * Environment
  FreezerEnv(..)
, FreezerHooks(..)
, noHooks
, FreezerStats(..)
, emptyFreezerStats
, isTransientSqliteError
, trimChunkPercentile
, freezerStatsJson
, TrimWindow(..)
, defaultTrimWindow
, fixedTrimWindow
, nextTrimWindow

  -- * Steps
, StepPlan(..)
, planStep
, readStep
, VerifyFailure(..)
, verifyStep
, TrimOutcome(..)
, trimStep
, StepOutcome(..)
, freezeOnce
, withFreezerReadConnection

  -- * Scheduling
, defaultFreezerPollMicros
, GateClock(..)
, realGateClock
, TrimDutyWindow
, newTrimDutyWindow
, trimTimeInWindow
, TrimGateEnv(..)
, trimGate
, recordTrimChunk
, runFreezer
, drainFreezer
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.QSem
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except

import Data.Aeson (Value, object, (.=))
import qualified Data.Aeson as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import Data.IORef
import Data.List (sort)
import qualified Data.Sequence as Seq
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word (Word64)

import GHC.Clock (getMonotonicTime)
import GHC.Stats (RTSStats(gc_elapsed_ns), getRTSStats, getRTSStatsEnabled)

import System.FilePath ((</>))
import System.LogLevel

import qualified Pact.Types.Persistence as Pact4

-- internal modules

import Chainweb.BlockHash
import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Logger
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Config
import Chainweb.Pact.Backend.Store.Layout
import Chainweb.Pact.Backend.Store.SQLite
import Chainweb.Pact.Backend.Store.Split (SplitCounters(..))
import Chainweb.Pact.Backend.Store.Types (PactStore(..))
import Chainweb.Pact.Backend.Store.WalCheckpointer (WalCheckpointerLiveness(..), WriterCheckpoint(..))
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils (chainDbFileName)
import Chainweb.Time (Micros(..), Time(..), TimeSpan(..), getCurrentTimeIntegral)

type TxId = Pact4.TxId

-- -------------------------------------------------------------------------- --
-- Environment

-- | Actions run after each step, for crash tests.
--
data FreezerHooks = FreezerHooks
    { _fhAfterRead :: !(IO ())
    , _fhAfterWrite :: !(IO ())
    , _fhAfterVerify :: !(IO ())
    , _fhAfterPublish :: !(IO ())
    , _fhAfterTrimChunk :: !(IO ())
    , _fhAfterBaseChunk :: !(Text -> Int -> Int -> IO ())
        -- ^ after each chunk of the first step's base is written, before it
        -- is verified: the table, its rows and its bytes ('rowBytes')
    }

noHooks :: FreezerHooks
noHooks = FreezerHooks (return ()) (return ()) (return ()) (return ()) (return ()) (\_ _ _ -> return ())

-- | What the freezer of a chain has done since it started. 'runFreezer'
-- logs it as JSON ('freezerStatsJson') every 10 minutes and when it stops.
--
-- The last three fields feed the decision on incremental auto-vacuum (ruling
-- 4): the hot file's bytes and live bytes after the last step, and the
-- largest file size seen.
--
data FreezerStats = FreezerStats
    { _fsSteps :: !Int
        -- ^ steps published
    , _fsRowsUser :: !Int
        -- ^ rows frozen of user tables
    , _fsRowsSys :: !Int
        -- ^ rows frozen of @SYS:*@ tables
    , _fsBytes :: !Int
        -- ^ bytes frozen, as the step cap counts them
    , _fsWriteMs :: !Double
        -- ^ time in the cold writes of the steps
    , _fsVerifyMs :: !Double
        -- ^ time in the verify of the steps
    , _fsRowsTrimmed :: !Int
    , _fsTrimChunks :: !Int
    , _fsTrimMs :: !Double
        -- ^ time of the trim chunks under the Pact connection's lock,
        -- savepoint included
    , _fsTrimMaxMs :: !Double
    , _fsTrimSamples :: !(Seq.Seq Double)
        -- ^ the times of the last 'trimSampleCap' chunks, oldest first, for
        -- the percentiles
    , _fsTrimPositionMs :: !Double
        -- ^ time of the synced trim position writes, one per chunk
    , _fsTrimWindow :: !(Maybe Word64)
        -- ^ the current trim window in txids, adapted to the 20 ms target;
        -- 'Nothing' before the first chunk ('_twStart')
    , _fsHaltedHash :: !(Maybe Frontier)
        -- ^ the trim found another block hash at this frontier; the freezer
        -- of the chain is halted
    , _fsSplitCounters :: !(Maybe SplitCounters)
        -- ^ the split store's read counters, as of the last log. The
        -- @SYS:Pacts@ cold hits include key-set reads ('SplitCounters').
    , _fsCacheHits :: !(Maybe Word64)
        -- ^ the cold store's block cache, as of the last log
    , _fsCacheMisses :: !(Maybe Word64)
    , _fsShadowDifferences :: !(Maybe Word64)
        -- ^ shadow mode: the differences found on the chain so far
    , _fsHotFileBytes :: !Integer
        -- ^ @page_count * page_size@ of the hot file after the last step
    , _fsHotLiveBytes :: !Integer
        -- ^ @(page_count - freelist_count) * page_size@ after the last step
    , _fsHotPeakBytes :: !Integer
        -- ^ the largest '_fsHotFileBytes' seen
    , _fsTransientRetries :: !Int
        -- ^ retries of 'runFreezer' after a transient SQLite error (BUSY or
        -- LOCKED)
    , _fsHaltedReason :: !(Maybe Text)
        -- ^ 'runFreezer' halted the chain's freezer, for this reason; it
        -- stays halted until the next start
    , _fsTrimCommitMaxMs :: !Double
        -- ^ the longest commit (savepoint release) of a trim chunk, part of
        -- its time: a commit that takes the WAL over the connection's
        -- @wal_autocheckpoint@ runs the checkpoint, which copies the whole
        -- WAL to the database file and syncs both, whatever the chunk wrote
    , _fsTrimGcMaxMs :: !(Maybe Double)
        -- ^ the most GC time inside one trim chunk, part of its time (a GC
        -- stops every thread, the writer's too); 'Nothing' unless the RTS
        -- keeps stats (@+RTS -T@)
    , _fsWalCheckpointer :: !(Maybe WalCheckpointerLiveness)
        -- ^ the node's background WAL checkpointer, as of the last log:
        -- whether its thread runs and the time since its last checkpoint
        -- call; 'Nothing' when the node runs without it
    , _fsTrimMaxExGcMs :: !(Maybe Double)
        -- ^ the longest trim chunk net of the GC time inside it: a chunk
        -- over the bar only by a GC pause is under it here; 'Nothing'
        -- unless the RTS keeps stats (@+RTS -T@)
    , _fsTrimCheckpoints :: !Int
        -- ^ PASSIVE checkpoints run before a trim chunk, under the Pact
        -- connection's lock ('_feTrimCheckpoint'); not part of any chunk's
        -- time
    , _fsTrimCheckpointSkips :: !Int
        -- ^ trim chunks that needed none (the WAL was short at a recent
        -- check) or found the checkpointer's thread at the file
    , _fsTrimCheckpointMs :: !Double
    , _fsTrimCheckpointMaxMs :: !Double
    , _fsTrimCheckpointWalFramesMax :: !Int
        -- ^ the longest WAL, in frames, a checkpoint before a trim chunk
        -- found: the WAL a chunk's commit would have added to
    }
    deriving (Eq, Show)

emptyFreezerStats :: FreezerStats
emptyFreezerStats = FreezerStats
    0 0 0 0 0 0 0 0 0 0 Seq.empty 0 Nothing Nothing Nothing Nothing Nothing Nothing 0 0 0 0 Nothing 0 Nothing Nothing
    Nothing 0 0 0 0 0

trimSampleCap :: Int
trimSampleCap = 8192

-- | A percentile (in [0, 1]) of the recent trim chunk times, in ms.
trimChunkPercentile :: Double -> FreezerStats -> Maybe Double
trimChunkPercentile q st = case sort (foldr (:) [] (_fsTrimSamples st)) of
    [] -> Nothing
    xs -> Just $ xs !! min (length xs - 1) (floor (q * fromIntegral (length xs)))

freezerStatsJson :: ChainId -> FreezerStats -> Value
freezerStatsJson c st = object
    [ "FreezerStats" .= object
        [ "chain" .= chainIdToText c
        , "steps" .= _fsSteps st
        , "rowsUser" .= _fsRowsUser st
        , "rowsSys" .= _fsRowsSys st
        , "bytes" .= _fsBytes st
        , "writeMs" .= _fsWriteMs st
        , "verifyMs" .= _fsVerifyMs st
        , "rowsTrimmed" .= _fsRowsTrimmed st
        , "trimChunks" .= _fsTrimChunks st
        , "trimMs" .= _fsTrimMs st
        , "trimMaxMs" .= _fsTrimMaxMs st
        , "trimP99Ms" .= trimChunkPercentile 0.99 st
        , "trimPositionMs" .= _fsTrimPositionMs st
        , "trimWindow" .= _fsTrimWindow st
        , "trimCommitMaxMs" .= _fsTrimCommitMaxMs st
        , "trimGcMaxMs" .= _fsTrimGcMaxMs st
        , "trimMaxExGcMs" .= _fsTrimMaxExGcMs st
        , "trimCheckpoints" .= _fsTrimCheckpoints st
        , "trimCheckpointSkips" .= _fsTrimCheckpointSkips st
        , "trimCheckpointMs" .= _fsTrimCheckpointMs st
        , "trimCheckpointMaxMs" .= _fsTrimCheckpointMaxMs st
        , "trimCheckpointWalFramesMax" .= _fsTrimCheckpointWalFramesMax st
        , "halted" .= fmap (T.pack . show) (_fsHaltedHash st)
        , "split" .= fmap splitJson (_fsSplitCounters st)
        , "coldCacheHits" .= _fsCacheHits st
        , "coldCacheMisses" .= _fsCacheMisses st
        , "shadowDifferences" .= _fsShadowDifferences st
        , "hotFileBytes" .= _fsHotFileBytes st
        , "hotLiveBytes" .= _fsHotLiveBytes st
        , "hotPeakBytes" .= _fsHotPeakBytes st
        , "transientRetries" .= _fsTransientRetries st
        , "haltedReason" .= _fsHaltedReason st
        , "walCheckpointer" .= fmap walJson (_fsWalCheckpointer st)
        ]
    ]
  where
    walJson l = object
        [ "alive" .= _wclAlive l
        , "lastCheckpointAgeSeconds" .= _wclLastCheckpointAgeSeconds l
        ]
    splitJson sc = object
        [ "coldReads" .= _scColdReads sc
        , "sysPactsHotHits" .= _scSysPactsHotHits sc
        , "sysPactsColdReads" .= _scSysPactsColdReads sc
        -- counts key-set reads (distinctKeys) as hits too, not only point hits
        , "sysPactsColdHits" .= _scSysPactsColdHits sc
        , "sysPactsColdMicros" .= _scSysPactsColdMicros sc
        , "sysPactsColdMaxMicros" .= _scSysPactsColdMaxMicros sc
        ]

-- | The trim window: each trim chunk covers the txids @[lo, lo + w)@ of the
-- step, cut short so that it holds at most '_twMaxRows' rows over the
-- tables the chunk trims (or a single txid, which is never split). The
-- row cap bounds a chunk by construction: its statements cost what its
-- window's rows cost, and a window grown over sparse txids can't take in a
-- dense range whole. @w@ adapts to the chunk time target '_twTargetMs'
-- (spec 6.2 step 5; 'nextTrimWindow'): halved on an overrun, doubled under
-- half the target, within '_twMin' and '_twMax'. Tests fix the window with
-- 'fixedTrimWindow'.
--
data TrimWindow = TrimWindow
    { _twStart :: !Word64
    , _twMin :: !Word64
    , _twMax :: !Word64
    , _twMaxRows :: !Int
        -- ^ the rows of a chunk's window, over all its tables
    , _twTargetMs :: !Double
        -- ^ the chunk time target: the gate's bound is 20 ms
    }
    deriving (Eq, Show)

-- | Starts at 256 txids, at most 1,000 rows per chunk, target 10 ms.
defaultTrimWindow :: TrimWindow
defaultTrimWindow = TrimWindow 256 1 1048576 1000 10

-- | A window of @w@ txids that does not adapt and has no practical row cap.
fixedTrimWindow :: Word64 -> TrimWindow
fixedTrimWindow w = TrimWindow w w w 1_000_000_000 10

-- | The window after a chunk: @nextTrimWindow tw w e capped ms@, where @w@
-- is the chunk's window, @e@ the txids it covered (@e < w@ when the row
-- cap or the step's end cut it short), @capped@ whether the row cap cut
-- it, and @ms@ its time.
--
-- * Over the target: half of @e@ (a capped chunk's @w@ says nothing of its
--   cost).
-- * A capped chunk under the target: @e@, the extent the cap allows.
-- * Under half the target: @2w@, only when the chunk covered all of @w@
--   (a chunk cut by the step's end measured less than @w@).
-- * Otherwise @w@.
--
-- Always within '_twMin' and '_twMax'.
nextTrimWindow :: TrimWindow -> Word64 -> Word64 -> Bool -> Double -> Word64
nextTrimWindow tw w e capped ms = max (_twMin tw) $ min (_twMax tw) $ if
    | ms > _twTargetMs tw -> max 1 (e `div` 2)
    | capped -> e
    | ms < _twTargetMs tw / 2 && e >= w -> w * 2
    | otherwise -> w

data FreezerEnv = FreezerEnv
    { _feChain :: !ChainId
    , _feCold :: !ColdChain
    , _feLayout :: !PactLayout
    , _feConfig :: !PactHistoryConfig
    , _feWithPactDb :: forall a. (SQLiteEnv -> IO a) -> IO a
        -- ^ runs on the Pact connection, under @cpModuleCacheVar@
    , _feBeforeTrimChunk :: !(IO ())
        -- ^ runs before every trim chunk: 'trimGate'
    , _feAfterTrimChunk :: !(Double -> IO ())
        -- ^ runs after every trim chunk with its duty time in seconds: the
        -- chunk under the Pact connection's lock plus the synced trim
        -- position write ('recordTrimChunk')
    , _fePollMicros :: !Int
        -- ^ how often 'runFreezer' polls the tip when there is nothing to do
        -- ('defaultFreezerPollMicros')
    , _feSplitCounters :: !(Maybe (IORef SplitCounters))
        -- ^ the split store's counters of the chain, for the stats log
    , _feCacheStats :: !(IO (Maybe (Word64, Word64)))
        -- ^ the cold block cache hits and misses ('coldCacheStats')
    , _feShadowDifferences :: !(IO (Maybe Word64))
        -- ^ shadow mode: the differences of the chain ('_shDifferences')
    , _feWalCheckpointer :: !(IO (Maybe WalCheckpointerLiveness))
        -- ^ the node's WAL checkpointer ('walCheckpointerLiveness')
    , _feTrimCheckpoint :: !(SQLiteEnv -> IO (Maybe WriterCheckpoint))
        -- ^ runs under the Pact connection's lock before each trim chunk,
        -- given the connection: the WAL checkpointer's
        -- 'checkpointBeforeWrite' ('Nothing' without one)
    , _feTrimWindow :: !TrimWindow
    , _feTrimClock :: !(IO Double)
        -- ^ seconds, for the trim chunk times: 'getMonotonicTime'; tests
        -- inject a clock
    , _feLogger :: !GenericLogger
    , _feHooks :: !FreezerHooks
    , _feStats :: !(IORef FreezerStats)
    }

logg :: FreezerEnv -> LogLevel -> Text -> IO ()
logg env level msg = logFunctionText (_feLogger env) level $
    "freezer, chain " <> chainIdToText (_feChain env) <> ": " <> msg

-- | A read-only connection to the chain's SQLite file. It opens under the
-- rename lock of the SQLite directory and is used outside it.
--
withFreezerReadConnection :: FreezerEnv -> (SQLiteEnv -> IO a) -> IO a
withFreezerReadConnection env = bracket
    (withSqliteDirLock (_feLayout env) $ \dir ->
        openSqliteReadOnly (dir </> chainDbFileName (_feChain env)))
    closeSqliteReadOnly

-- -------------------------------------------------------------------------- --
-- Plan and read

-- | The blocks of a step: @(height, hash, endingtxid)@, ascending.
--
newtype StepPlan = StepPlan
    { _spBlocks :: [(BlockHeight, BlockHash, TxId)]
    }
    deriving (Eq, Show)

-- | Plans a step when the SQLite tip is more than
-- @_phFreezeDepth + _phFreezeStepHeights@ heights above the verified
-- frontier (spec 6.1): the heights above the frontier, up to
-- @tip - _phFreezeDepth@, at most @_phFreezeStepHeights@ of them.
--
planStep :: FreezerEnv -> SQLiteEnv -> IO (Maybe StepPlan)
planStep env db = sqliteTip db >>= \case
    Nothing -> return Nothing
    Just (tip, _, _) -> do
        ver <- _ccVerified (_feCold env)
        -- The step starts at the first block above the frontier, not at the
        -- frontier's height + 1: a chain's history can start far above
        -- height 0 (a late genesis or a compacted database), and nothing
        -- would ever be planned if the step counted from height 0.
        sqliteFirstBlockAbove db (_frHeight <$> ver) >>= \case
          Nothing -> return Nothing
          Just first -> do
            let cfg = _feConfig env
                depth = toInteger (_phFreezeDepth cfg)
                stepHeights = max 1 (toInteger (_phFreezeStepHeights cfg))
                vh = toInteger first - 1
                t = toInteger tip
            if t - vh <= depth + stepHeights
            then return Nothing
            else do
                let to = min (vh + stepHeights) (t - depth)
                -- not empty: @first <= to@ by the guard, and @first@ is a
                -- block of BlockHistory
                blocks <- sqliteBlocks db (_frHeight <$> ver) (fromIntegral to)
                return $ if null blocks then Nothing else Just (StepPlan blocks)

-- | Reads a step (spec 6.2 step 1) in one read transaction, block by block:
-- for each block, the rows of the tables of @VersionedTableMutation@ at its
-- height that 'frozenTable' admits, in rowid order. The step ends after the
-- block at which it reaches '_phFreezeStepBytes'; a single block larger than
-- that is a step of its own.
--
-- The first step (no written frontier) starts with the base: the rows below
-- the end of the first block of every table of the schema ('forBaseChunks'),
-- which count towards the cap like a first block. 'readStep' holds the whole
-- step, base included, in memory: it is for tests. The freezer writes the
-- base chunk by chunk ('freezeOnce').
--
readStep :: FreezerEnv -> SQLiteEnv -> StepPlan -> IO ColdStep
readStep env db (StepPlan blocks) = do
    start <- frontierTxId <$> _ccWritten (_feCold env)
    let cap = _phFreezeStepBytes (_feConfig env)
    case blocks of
        (h1, hash1, end1) : rest | start == 0 -> sqliteReadTransaction db $ do
            ref <- newIORef []
            forBaseChunks db cap h1 end1 $ \c -> modifyIORef' ref (c :)
            chunks <- reverse <$> readIORef ref
            let base = foldl (\acc c -> mergeTables acc [(_bkTable c, _bkRows c)]) [] chunks
                bytes = sum [ rowBytes r | c <- chunks, r <- _bkRows c ]
            tl <- readTail db (Frontier h1 hash1 end1) bytes (Just cap) rest
            return tl { _stpTables = mergeTables base (_stpTables tl) }
        _ -> sqliteReadTransaction db $ readBlocksIn db start 0 (Just cap) blocks

-- | The tables the first step's base reads ('forBaseChunks') and trims
-- besides those of @VersionedTableMutation@: every Pact table of the SQLite
-- schema at that moment ('sqliteSchemaTables'). A history that does not
-- start at genesis (a compacted database) holds rows below its first block
-- that no @VersionedTableMutation@ row of a planned height names, and the
-- compaction may keep no @VersionedTableCreation@ row for their table. The
-- base covers the txids below the end of the first block, so it covers those
-- rows; a table first changed after the first step would otherwise join the
-- cold catalog in a later step whose range misses them, and reads below F
-- would go to cold and miss them. On a history from genesis this adds
-- nothing: every genesis row is named.
firstStepTables :: SQLiteEnv -> IO [Text]
firstStepTables = sqliteSchemaTables

-- | The bytes a row counts for in the step cap.
rowBytes :: StepRow -> Int
rowBytes r = B.length (_srRowKey r) + B.length (_srRowData r) + 8

-- | A chunk of the first step's base: the rows of one table with
-- @_bkFrom <= txid < _bkTo@, in @(txid, rowid)@ order. A chunk holds whole
-- txids, so a user table's @T@ list of a txid is written in one batch.
data BaseChunk = BaseChunk
    { _bkTable :: !Text
    , _bkFrom :: !TxId
    , _bkTo :: !TxId
    , _bkRows :: ![StepRow]
    }

-- | The rows of a page of the base read.
basePageRows :: Int
basePageRows = 1024

-- | Streams the base of the first step (spec 6.2 step 1 on a history that
-- does not start at genesis, I4): for each table of the schema and of
-- @VersionedTableMutation@ at the first block that 'frozenTable' admits, the
-- rows with @txid < end@ (the end txid of the first block), in pages, cut
-- into chunks of about @cap@ bytes. A chunk ends at the first txid boundary
-- at or after the cap, so it holds less than @cap@ plus the rows of one
-- txid. The memory is a chunk and a page, whatever the size of the base.
--
-- The chunks of a table cover @[0, end)@ without gaps; a table without rows
-- below @end@ gives none. Runs inside the caller's read transaction.
--
forBaseChunks :: MonadIO m => SQLiteEnv -> Int -> BlockHeight -> TxId -> (BaseChunk -> m ()) -> m ()
forBaseChunks db cap h1 end k = do
    tables <- liftIO $ do
        mutated <- sqliteMutatedTables db h1
        schema <- firstStepTables db
        return (dedupTables (filter frozenTable (mutated ++ schema)))
    forM_ tables $ \t -> table t Nothing 0 0 []
  where
    -- after: the page position; lo: the start of the chunk; n: its bytes;
    -- acc: its rows, reversed
    table t after lo !n acc = do
        page <- liftIO $ sqliteBaseRowsPage db t end after basePageRows
        (lo', n', acc') <- foldM (add t) (lo, n, acc) (snd <$> page)
        case page of
            _ | length page < basePageRows ->
                unless (null acc') $ k (BaseChunk t lo' end (reverse acc'))
            _ -> let (i, r) = last page in table t (Just (_srTxId r, i)) lo' n' acc'
    add t (lo, n, acc) r = case acc of
        p : _ | n >= cap && _srTxId r /= _srTxId p -> do
            k (BaseChunk t lo (_srTxId r) (reverse acc))
            return (_srTxId r, rowBytes r, [r])
        _ -> return (lo, n + rowBytes r, r : acc)

-- | The tables in first-seen order, each once by its folded name.
dedupTables :: [Text] -> [Text]
dedupTables = go' S.empty
  where
    go' _ [] = []
    go' seen (t : ts)
        | S.member (foldTableName t) seen = go' seen ts
        | otherwise = t : go' (S.insert (foldTableName t) seen) ts

-- | Appends the rows of the second list to the first, table by table (folded
-- names); new tables go at the end.
mergeTables :: [(Text, [StepRow])] -> [(Text, [StepRow])] -> [(Text, [StepRow])]
mergeTables = foldl add
  where
    add acc (t, rows)
        | any ((== foldTableName t) . foldTableName . fst) acc =
            [ if foldTableName n == foldTableName t then (n, old ++ rows) else (n, old) | (n, old) <- acc ]
        | otherwise = acc ++ [(t, rows)]

-- | The blocks of the first step after its base: the step ends at the first
-- block when there are no more blocks or the base reached the cap.
readTail :: SQLiteEnv -> Frontier -> Int -> Maybe Int -> [(BlockHeight, BlockHash, TxId)] -> IO ColdStep
readTail db first bytes cap rest
    | null rest || maybe False (bytes >=) cap = return (ColdStep first [])
    | otherwise = readBlocksIn db (_frEndTxId first) bytes cap rest

-- | The blocks of a step from @start@, inside the caller's read transaction,
-- with @bytes0@ bytes already in the step. The tables of each block are those
-- of @VersionedTableMutation@ at its height (the base of the first step is
-- read by 'forBaseChunks').
readBlocksIn :: SQLiteEnv -> TxId -> Int -> Maybe Int -> [(BlockHeight, BlockHash, TxId)] -> IO ColdStep
readBlocksIn _ _ _ _ [] = throwIO $ ColdStoreException "Chainweb.Pact.Backend.Store.Freezer: a step without blocks"
readBlocksIn db start0 bytes0 cap blocks0 = go start0 bytes0 HM.empty [] blocks0
  where
    -- acc: folded name to (name as first seen, rows in reverse); order: the
    -- folded names in first-seen order, reversed
    go _ _ _ _ [] = error "readBlocksIn: impossible"
    go start !bytes acc order ((h, hash, end) : rest) = do
        mutated <- sqliteMutatedTables db h
        let tables = dedupTables (filter frozenTable mutated)
        (acc', order', bytes') <- foldM (\(a, o, n) t -> do
            rows <- sqliteBlockRows db t start end
            let k = foldTableName t
                n' = n + sum (rowBytes <$> rows)
            return $ case HM.lookup k a of
                Just (name, old) -> (HM.insert k (name, reverse rows ++ old) a, o, n')
                Nothing
                    | null rows -> (a, o, n')
                    | otherwise -> (HM.insert k (t, reverse rows) a, k : o, n')
            ) (acc, order, bytes) tables
        let done = null rest || maybe False (bytes' >=) cap
        if done
        then return ColdStep
            { _stpTo = Frontier h hash end
            , _stpTables =
                [ (name, reverse rows)
                | k <- reverse order', Just (name, rows) <- [HM.lookup k acc']
                ]
            }
        else go end bytes' acc' order' rest

-- -------------------------------------------------------------------------- --
-- Verify

data VerifyFailure = VerifyFailure
    { _vfTable :: !Text
    , _vfTxId :: !(Maybe TxId)
    , _vfWhat :: !Text
    }
    deriving (Eq, Show)

-- | Verifies a written step against SQLite (spec 6.2 step 3) through the
-- cold store's freezer view, which sees the written frontier and the step's
-- unpublished tables:
--
-- * every row of the step has an exact @R@ entry with the same bytes;
-- * for each txid of a user table, the cold by-txid answer equals SQLite's
--   @rowsAtTxId@ on this connection, order included;
-- * for each (table, key) of the step, the number of @R@ entries in the
--   step's txid range equals SQLite's row count.
--
-- The step starts at the verified frontier.
--
verifyStep :: FreezerEnv -> SQLiteEnv -> ColdStep -> IO (Either VerifyFailure ())
verifyStep env db (ColdStep to tables) = do
    lo <- frontierTxId <$> _ccVerified (_feCold env)
    runExceptT $ forM_ tables $ \(t, rows) ->
        verifyRows (_ccFreezerView (_feCold env)) db t rows lo (_frEndTxId to)

-- | Verifies a chunk of the first step's base ('BaseChunk') as 'verifyStep'
-- verifies a step, through the freezer view clamped to @hi@ (the end txid of
-- the first block), with the key counts over the chunk's txids: the chunks of
-- the base are written before the step's frontier, and a key's later chunks
-- may not be written yet.
verifyChunk :: FreezerEnv -> SQLiteEnv -> TxId -> BaseChunk -> IO (Either VerifyFailure ())
verifyChunk env db hi c = runExceptT $
    verifyRows (_ccFreezerViewBelow (_feCold env) hi) db (_bkTable c) (_bkRows c) (_bkFrom c) (_bkTo c)

verifyRows :: ColdFreezerView -> SQLiteEnv -> Text -> [StepRow] -> TxId -> TxId -> ExceptT VerifyFailure IO ()
verifyRows fv db t rows lo hi = do
    let ps = sqlitePactStore db
    forM_ rows $ \r -> do
        v <- lift $ _fvRowAt fv t (_srRowKey r) (_srTxId r)
        case v of
            Just d | d == _srRowData r -> return ()
            Just _ -> throwE $ VerifyFailure t (Just (_srTxId r)) $
                "the R entry of key " <> showKey (_srRowKey r) <> " differs from SQLite's row"
            Nothing -> throwE $ VerifyFailure t (Just (_srTxId r)) $
                "no R entry for key " <> showKey (_srRowKey r)
    unless (isSysTable t) $ forM_ (distinct (_srTxId <$> rows)) $ \x -> do
        c <- lift $ _fvRowsAtTxId fv t x
        s <- lift $ _psRowsAtTxId ps t x
        case s of
            Left e -> throwE $ VerifyFailure t (Just x) $ "SQLite rowsAtTxId failed: " <> T.pack (show e)
            Right sr -> unless (c == Just sr) $ throwE $ VerifyFailure t (Just x)
                "the cold by-txid answer differs from SQLite's (rows or order)"
    forM_ (distinct (_srRowKey <$> rows)) $ \k -> do
        c <- lift $ _fvKeyVersions fv t k lo hi
        s <- lift $ sqliteKeyRowCount db t k lo hi
        unless (c == s) $ throwE $ VerifyFailure t Nothing $
            "key " <> showKey k <> ": " <> T.pack (show c) <> " cold versions, "
                <> T.pack (show s) <> " SQLite rows"
  where
    showKey = T.pack . show :: B.ByteString -> Text

distinct :: Ord a => [a] -> [a]
distinct = go S.empty
  where
    go _ [] = []
    go seen (x : xs)
        | S.member x seen = go seen xs
        | otherwise = x : go (S.insert x seen) xs

-- -------------------------------------------------------------------------- --
-- Trim

data TrimOutcome = Trimmed !Int | TrimHashMismatch
    deriving (Eq, Show)

-- | Trims SQLite below the frontier @F@ (spec 6.2 step 5) for the rows
-- written in @[from, F)@: for the tables mutated in those blocks, and for
-- the tables of the first step's base over the txids below the end of the
-- first block ('forBaseChunks'), every row
-- below @F@ that is not the newest below @F@ of its key ('KeepAnchors'), and
-- every row below @F@ of @SYS:Pacts@ ('DropAll', 'keepsNoAnchors').
--
-- 'ensureSplitDir' runs first. The trim walks @[from, F)@ in ascending
-- windows of txids ('TrimWindow'); one chunk is one window over every table.
-- Each chunk runs '_feBeforeTrimChunk', then, on the Pact connection in the
-- savepoint @trim@, checks that @BlockHistory@ still has the frontier's hash
-- at its height ('TrimHashMismatch' and nothing deleted otherwise) and runs
-- the window's statements. Then '_ccTrimProgress' stores the window's end,
-- so a trim cut short resumes there.
--
-- The windows ascend and a chunk is atomic, so the trim deletes
-- oldest-first: after every chunk, a key's rows in SQLite are a newest-suffix
-- of its legacy rows, and a SQLite hit is the legacy answer (spec 4.3, 6.3).
-- Rowids of the rows that stay do not change; there is no @VACUUM@.
--
trimStep :: FreezerEnv -> Frontier -> TxId -> IO TrimOutcome
trimStep env f from = do
    ensureSplitDir (_feLogger env) (_feLayout env) (_feChain env) (_frHeight f)
    tables <- _feWithPactDb env $ \db -> do
        prev <- sqliteBlockAtOrBelow db (_frHeight f) from
        mutated <- sqliteMutatedTablesBetween db prev (_frHeight f)
        -- the base of the first step: the tables of the schema over the
        -- txids below the end of the first block, the range its read covers
        -- ('forBaseChunks')
        first <- sqliteFirstBlockAbove db Nothing >>= \case
            Nothing -> return []
            Just h -> sqliteBlocks db Nothing h
        base <- case first of
            (_, _, end1) : _ | from < end1 -> fmap (, Just end1) <$> firstStepTables db
            _ -> return []
        return (fmap (, Nothing) mutated ++ base)
    let work = distinctOn foldTableName
            [ (t, if keepsNoAnchors t then DropAll else KeepAnchors, lim)
            | (t, lim) <- tables, frozenTable t
            ]
    go work from 0
  where
    fe = _frEndTxId f
    tw = _feTrimWindow env

    distinctOn g = go' S.empty
      where
        go' _ [] = []
        go' seen (x@(t, _, _) : xs)
            | S.member (g t) seen = go' seen xs
            | otherwise = x : go' (S.insert (g t) seen) xs

    clock = _feTrimClock env

    -- the RTS's GC time so far, when it keeps stats (+RTS -T)
    gcSeconds = getRTSStatsEnabled >>= \case
        False -> return Nothing
        True -> Just . (\st -> fromIntegral (gc_elapsed_ns st) / 1e9 :: Double) <$> getRTSStats

    go work lo !total
        | lo >= fe = return (Trimmed total)
        | otherwise = do
            _feBeforeTrimChunk env
            w <- maybe (_twStart tw) id . _fsTrimWindow <$> readIORef (_feStats env)
            let hiW = min fe (lo + Pact4.TxId w)
            r <- _feWithPactDb env $ \db -> do
                -- a PASSIVE checkpoint while no Pact transaction is open:
                -- the chunk's write then restarts the WAL, and its commit
                -- cannot reach the writer's backstop autocheckpoint. Timed
                -- apart from the chunk.
                k0 <- clock
                ck <- _feTrimCheckpoint env db
                k1 <- clock
                g0 <- gcSeconds
                t0 <- clock
                r <- sqliteWithTrimSavepoint db $ sqliteBlockHashAt db (_frHeight f) >>= \case
                    Just h | h == _frHash f -> do
                        (hi, capped, live) <- bound db hiW
                        n <- sum <$> forM live (\(t, rule, lim) ->
                            sqliteTrimChunk db t rule lo (maybe hi (min hi) lim) fe)
                        tc <- clock
                        return (Just (n, hi, capped, tc))
                    _ -> return Nothing
                t1 <- clock
                g1 <- gcSeconds
                return ((\(n, hi, capped, tc) -> ((n, hi, capped), t1 - t0, t1 - tc, (-) <$> g1 <*> g0, (ck, k1 - k0))) <$> r)
            case r of
                Nothing -> return TrimHashMismatch
                Just ((n, hi, capped), dt, cm, gc, (ck, kt)) -> do
                    let ms = dt * 1000
                        Pact4.TxId e = hi - lo
                        w' = nextTrimWindow tw w e capped ms
                        ckMs = kt * 1000
                        (ran, skipped, ckFrames) = case ck of
                            Just (WriterCheckpointRan _ l _) -> (1, 0, l)
                            Just _ -> (0, 1, 0)
                            Nothing -> (0, 0, 0 :: Int)
                    p0 <- clock
                    _ccTrimProgress (_feCold env) hi
                    p1 <- clock
                    modifyIORef' (_feStats env) $ \s -> s
                        { _fsTrimWindow = Just w'
                        , _fsRowsTrimmed = _fsRowsTrimmed s + n
                        , _fsTrimChunks = _fsTrimChunks s + 1
                        , _fsTrimMs = _fsTrimMs s + ms
                        , _fsTrimMaxMs = max (_fsTrimMaxMs s) ms
                        , _fsTrimSamples = Seq.drop (Seq.length (_fsTrimSamples s) + 1 - trimSampleCap)
                            (_fsTrimSamples s Seq.|> ms)
                        , _fsTrimPositionMs = _fsTrimPositionMs s + (p1 - p0) * 1000
                        , _fsTrimCommitMaxMs = max (_fsTrimCommitMaxMs s) (cm * 1000)
                        , _fsTrimGcMaxMs = case gc of
                            Nothing -> _fsTrimGcMaxMs s
                            Just g -> Just (max (g * 1000) (maybe 0 id (_fsTrimGcMaxMs s)))
                        , _fsTrimMaxExGcMs = case gc of
                            Nothing -> _fsTrimMaxExGcMs s
                            Just g -> Just (max (ms - g * 1000) (maybe 0 id (_fsTrimMaxExGcMs s)))
                        , _fsTrimCheckpoints = _fsTrimCheckpoints s + ran
                        , _fsTrimCheckpointSkips = _fsTrimCheckpointSkips s + skipped
                        , _fsTrimCheckpointMs = _fsTrimCheckpointMs s + ckMs
                        , _fsTrimCheckpointMaxMs = max (_fsTrimCheckpointMaxMs s) ckMs
                        , _fsTrimCheckpointWalFramesMax = max (_fsTrimCheckpointWalFramesMax s) ckFrames
                        }
                    -- a chunk over the gate's bar (twice the target): its
                    -- split, so that a GC pause or the checkpoint before it
                    -- tells apart from trim work
                    when (ms > 2 * _twTargetMs tw) $ logg env Info $ "trim chunk over "
                        <> T.pack (show (2 * _twTargetMs tw)) <> " ms: " <> T.pack (show ms)
                        <> " ms (commit " <> T.pack (show (cm * 1000))
                        <> " ms, GC " <> maybe "n/a" (\g -> T.pack (show (g * 1000))) gc
                        <> " ms); the checkpoint before it, not counted: " <> T.pack (show ckMs) <> " ms"
                    -- the duty time: the lock was held for the checkpoint too
                    _feAfterTrimChunk env (dt + kt + (p1 - p0))
                    _fhAfterTrimChunk (_feHooks env)
                    go work hi (total + n)
      where
        -- Cuts @[lo, hiW)@ to at most '_twMaxRows' rows over the tables
        -- ('sqliteTrimWindowEnd'), table by table: each table's rows are
        -- counted under the end so far, and a later table can only move the
        -- end down, so the sum stays within the cap. Returns the end, whether
        -- the cap cut the window, and the tables to run: a 'KeepAnchors'
        -- table without rows in the window has nothing to delete (both its
        -- statements start from the window's rows); 'DropAll' always runs.
        bound db hiW = goB hiW (_twMaxRows tw) [] work
          where
            goB hi _ acc [] = return (hi, hi < hiW, reverse acc)
            goB hi b acc (x@(t, rule, lim) : xs) = case lim of
                Just l | lo >= l -> goB hi b acc xs
                _ -> do
                    let tHi = maybe hi (min hi) lim
                    (c, tHi') <- sqliteTrimWindowEnd db t lo tHi b
                    let hi' = if tHi' < tHi then tHi' else hi
                        acc' = if c > 0 || rule == DropAll then x : acc else acc
                    goB hi' (max 0 (b - c)) acc' xs

-- -------------------------------------------------------------------------- --
-- One step

data StepOutcome
    = NothingToDo
    | Froze !Frontier
        -- ^ a step was published and trimmed; the new verified frontier
    | HaltedVerify !VerifyFailure
        -- ^ verification failed: the freezer of the chain halts, SQLite is
        -- not trimmed
    | HaltedHash !Frontier
        -- ^ @BlockHistory@ has another hash at this frontier: the freezer
        -- of the chain halts, nothing is deleted
    | StepDropped
        -- ^ a rewind of the hot store began or ended while the step was
        -- read; nothing was written, the step is read again later
        -- ('RewindGuard')
    deriving (Eq, Show)

-- | The rows and bytes of the base chunks of a first step, for the stats.
data BaseCounts = BaseCounts
    { _bcUser :: !Int
    , _bcSys :: !Int
    , _bcBytes :: !Int
    }

noCounts :: BaseCounts
noCounts = BaseCounts 0 0 0

addCounts :: BaseChunk -> BaseCounts -> BaseCounts
addCounts ch (BaseCounts u y b)
    | isSysTable (_bkTable ch) = BaseCounts u (y + n) b'
    | otherwise = BaseCounts (u + n) y b'
  where
    n = length (_bkRows ch)
    b' = b + sum (rowBytes <$> _bkRows ch)

-- | One freezer step (spec 6.2, 6.3):
--
-- 1. if the written frontier is ahead of the verified one, re-read that range
--    on a fresh read connection, verify it, publish it and trim it;
-- 2. if the trim of the verified frontier is not complete (its stored
--    position '_cmTrimFrom' is below @F@), resume it there: this finishes a
--    trim that a crash, or an exception in this process, cut short;
-- 3. otherwise plan, read, write, verify, publish and trim one step.
--
-- A write failure (an exception of the cold store) propagates; nothing is
-- trimmed then.
--
-- The read, write and verify of a step take the chain's 'RewindGuard': a
-- step whose snapshot a rewind may have changed is dropped ('StepDropped').
--
freezeOnce :: FreezerEnv -> IO StepOutcome
freezeOnce = freezeOnceWith id

-- | 'freezeOnce', with the read, write and verify of a step (not its trim)
-- run inside the given slot: 'runFreezer' passes the node-wide semaphore.
freezeOnceWith :: (forall a. IO a -> IO a) -> FreezerEnv -> IO StepOutcome
freezeOnceWith slot env = do
    st <- readIORef (_feStats env)
    meta <- _ccMeta cold
    case _fsHaltedHash st of
        Just f -> return (HaltedHash f)
        Nothing
            | _cmWritten meta /= _cmVerified meta -> recover meta
            | Just f <- _cmVerified meta, _cmTrimFrom meta < _frEndTxId f -> do
                logg env Info $ "resuming the trim below " <> T.pack (show f)
                    <> " at txid " <> T.pack (show (_cmTrimFrom meta))
                trimAndReport f (_cmTrimFrom meta) Nothing >>= \case
                    o@(HaltedHash _) -> return o
                    _ -> step
            | otherwise -> step
  where
    cold = _feCold env
    hooks = _feHooks env
    rg = _ccRewindGuard cold
    cap = _phFreezeStepBytes (_feConfig env)

    -- (1) a written but unverified step: verify it again and publish it. A
    -- rewind below the written frontier is refused, so the guard is not
    -- needed here. The first step's base is read and verified again chunk by
    -- chunk ('forBaseChunks'), so memory stays bounded by the cap.
    recover meta = case _cmWritten meta of
        Nothing -> throwIO $ ColdStoreException
            "Chainweb.Pact.Backend.Store.Freezer: a verified frontier without a written one"
        Just w -> do
            let v = _cmVerified meta
            logg env Info $ "verifying the written but unpublished step to " <> T.pack (show w)
            r <- slot $ withFreezerReadConnection env $ \db -> sqliteReadTransaction db $
                sqliteBlockHashAt db (_frHeight w) >>= \case
                    Just h | h == _frHash w -> do
                        blocks <- sqliteBlocks db (_frHeight <$> v) (_frHeight w)
                        rb <- case blocks of
                            (h1, hash1, end1) : rest | frontierTxId v == 0 -> do
                                c <- newIORef noCounts
                                e <- runExceptT $ forBaseChunks db cap h1 end1 $ \ch -> do
                                    ExceptT (verifyChunk env db end1 ch)
                                    liftIO $ modifyIORef' c (addCounts ch)
                                cs <- readIORef c
                                fmap (, cs) <$> traverse (\_ -> readTail db (Frontier h1 hash1 end1) 0 Nothing rest) e
                            [] -> return $ Left $ VerifyFailure "" Nothing "SQLite has no block up to the written frontier"
                            _ -> Right . (, noCounts) <$> readBlocksIn db (frontierTxId v) 0 Nothing blocks
                        _fhAfterRead hooks
                        case rb of
                            Left e -> return (Left (HaltedVerify e))
                            Right (s, cs)
                                | _stpTo s /= w -> return $ Left $ HaltedVerify $ VerifyFailure "" Nothing $
                                    "SQLite's blocks end at " <> T.pack (show (_stpTo s))
                                        <> ", not at the written frontier"
                                | otherwise -> timed (verifyStep env db s) >>= \case
                                    (Left e, _) -> return (Left (HaltedVerify e))
                                    (Right (), vt) -> do
                                        addStats $ \st -> st { _fsVerifyMs = _fsVerifyMs st + vt }
                                        return (Right (s, cs))
                    _ -> return (Left (HaltedHash w))
            case r of
                Left o -> halted o
                Right sc -> do
                    _fhAfterVerify hooks
                    _ccPublish cold
                    _fhAfterPublish hooks
                    trimAndReport w (frontierTxId v) (Just sc)

    -- (3) a new step
    step = do
        r <- slot $ do
            gen <- guardGeneration rg
            start <- frontierTxId <$> _ccWritten cold
            withFreezerReadConnection env $ \db -> planStep env db >>= \case
                Nothing -> return (Left NothingToDo)
                Just (StepPlan ((h1, hash1, end1) : rest)) | start == 0 ->
                    baseStep gen db (Frontier h1 hash1 end1) rest
                Just plan -> do
                    s <- readStep env db plan
                    _fhAfterRead hooks
                    claimed <- guardClaimStep rg gen (_frEndTxId (_stpTo s))
                    if not claimed
                    then dropped (_stpTo s)
                    else flip finally (guardReleaseStep rg) $
                        writeVerifyPublish db s noCounts
        case r of
            Left NothingToDo -> return NothingToDo
            Left StepDropped -> return StepDropped
            Left o -> halted o
            Right (s, lo, cs) -> trimAndReport (_stpTo s) lo (Just (s, cs))

    dropped to = do
        logg env Info $ "a rewind of the hot store overlapped the read of the step to "
            <> T.pack (show to) <> "; the step is dropped and read again"
        return (Left StepDropped)

    writeVerifyPublish db s cs = do
        lo <- frontierTxId <$> _ccVerified cold
        ((), wt) <- timed (_ccWriteStep cold s)
        addStats $ \st -> st { _fsWriteMs = _fsWriteMs st + wt }
        _fhAfterWrite hooks
        timed (verifyStep env db s) >>= \case
            (Left e, _) -> return (Left (HaltedVerify e))
            (Right (), vt) -> do
                addStats $ \st -> st { _fsVerifyMs = _fsVerifyMs st + vt }
                _fhAfterVerify hooks
                _ccPublish cold
                _fhAfterPublish hooks
                return (Right (s, lo, cs))

    -- (3) the first step (I4): the base, the rows below the end of the first
    -- block of every table of the schema, is written and verified chunk by
    -- chunk under the cap ('forBaseChunks', '_ccWriteRows'), all in one read
    -- transaction. The step's frontier is written after the last chunk, with
    -- the rows of the step's later blocks, and published after their verify.
    -- A crash before that leaves the written frontier where it was: the next
    -- run writes the base again from its start (the same entries).
    --
    -- The claim on the first block's end comes before the base read: a
    -- rewind that began or ended since 'guardGeneration' drops the step, and
    -- a rewind below that end waits for the step. The later blocks are
    -- claimed after the base; if a rewind came in the meantime, the step
    -- ends at the first block.
    baseStep gen db first rest = sqliteReadTransaction db $ do
        claimed <- guardClaimStep rg gen (_frEndTxId first)
        if not claimed
        then dropped first
        else flip finally (guardReleaseStep rg) $ do
            let end1 = _frEndTxId first
            c <- newIORef noCounts
            e <- runExceptT $ forBaseChunks db cap (_frHeight first) end1 $ \ch -> do
                let rows = _bkRows ch
                ((), wt) <- liftIO $ timed (_ccWriteRows cold end1 [(_bkTable ch, rows)])
                liftIO $ do
                    addStats $ \st -> st { _fsWriteMs = _fsWriteMs st + wt }
                    _fhAfterBaseChunk hooks (_bkTable ch) (length rows) (sum (rowBytes <$> rows))
                (r, vt) <- liftIO $ timed (verifyChunk env db end1 ch)
                ExceptT (return r)
                liftIO $ do
                    addStats $ \st -> st { _fsVerifyMs = _fsVerifyMs st + vt }
                    modifyIORef' c (addCounts ch)
            case e of
                Left err -> return (Left (HaltedVerify err))
                Right () -> do
                    cs <- readIORef c
                    tailOk <- case rest of
                        [] -> return False
                        _ -> guardClaimStep rg gen (maximum [ x | (_, _, x) <- rest ])
                    s <- if tailOk
                        then readTail db first (_bcBytes cs) (Just cap) rest
                        else return (ColdStep first [])
                    _fhAfterRead hooks
                    writeVerifyPublish db s cs

    addStats = modifyIORef' (_feStats env)

    trimAndReport f lo ms = trimStep env f lo >>= \case
        TrimHashMismatch -> halted (HaltedHash f)
        Trimmed n -> do
            fs <- withFreezerReadConnection env sqliteFileStats
            addStats $ \st -> st
                { _fsHotFileBytes = _sfsFileBytes fs
                , _fsHotLiveBytes = _sfsLiveBytes fs
                , _fsHotPeakBytes = max (_fsHotPeakBytes st) (_sfsFileBytes fs)
                }
            forM_ ms $ \(s, cs) -> do
                let count p = sum [ length rs | (t, rs) <- _stpTables s, p (isSysTable t) ]
                    user = count not + _bcUser cs
                    sys = count id + _bcSys cs
                    bytes = sum [ rowBytes r | (_, rs) <- _stpTables s, r <- rs ] + _bcBytes cs
                addStats $ \st -> st
                    { _fsSteps = _fsSteps st + 1
                    , _fsRowsUser = _fsRowsUser st + user
                    , _fsRowsSys = _fsRowsSys st + sys
                    , _fsBytes = _fsBytes st + bytes
                    }
                logg env Info $ "froze " <> T.pack (show (user + sys)) <> " rows to " <> T.pack (show f)
                    <> "; trimmed " <> T.pack (show n) <> " rows"
            return (Froze f)

    halted o = do
        case o of
            HaltedHash f -> do
                modifyIORef' (_feStats env) $ \s -> s { _fsHaltedHash = Just f }
                logg env Error $ "HALTED: BlockHistory has another block hash at the frontier "
                    <> T.pack (show f) <> "; nothing was deleted. SQLite is not trimmed and the node keeps running on it. An operator decides."
            HaltedVerify e -> logg env Error $ "HALTED: verification of the step failed: "
                <> T.pack (show e) <> ". SQLite is not trimmed and the node keeps running on it. An operator decides."
            _ -> return ()
        return o

-- | The result and its time in ms.
timed :: IO a -> IO (a, Double)
timed act = do
    t0 <- getMonotonicTime
    a <- act
    t1 <- getMonotonicTime
    return (a, (t1 - t0) * 1000)

-- -------------------------------------------------------------------------- --
-- Scheduling

-- | The default '_fePollMicros': 2 s.
defaultFreezerPollMicros :: Int
defaultFreezerPollMicros = 2_000_000

-- | The clocks of the trim gate. Tests use a virtual clock.
data GateClock = GateClock
    { _gcMonotonic :: !(IO Double)
        -- ^ seconds, for the duty cycle
    , _gcWall :: !(IO (Time Micros))
        -- ^ to compare with the latest block's creation time
    , _gcSleep :: !(Double -> IO ())
        -- ^ sleeps this many seconds
    }

realGateClock :: GateClock
realGateClock = GateClock
    { _gcMonotonic = getMonotonicTime
    , _gcWall = getCurrentTimeIntegral
    , _gcSleep = \d -> threadDelay (max 1 (ceiling (d * 1_000_000)))
    }

-- | The trim chunks of the last 60 s: (end time, duty seconds), oldest
-- first.
--
-- (The plan named this type @TrimWindow@; that name already belongs to the
-- trim's txid window.)
newtype TrimDutyWindow = TrimDutyWindow (Seq.Seq (Double, Double))

newTrimDutyWindow :: IO (IORef TrimDutyWindow)
newTrimDutyWindow = newIORef (TrimDutyWindow Seq.empty)

-- | The span of the duty cycle: 60 s.
dutySpan :: Double
dutySpan = 60

pruneWindow :: Double -> TrimDutyWindow -> TrimDutyWindow
pruneWindow now (TrimDutyWindow xs) = TrimDutyWindow (Seq.dropWhileL (\(e, _) -> e <= now - dutySpan) xs)

-- | The trim time recorded in the last 60 s, in seconds. A chunk that
-- ended in the span counts whole.
trimTimeInWindow :: TrimGateEnv -> IO Double
trimTimeInWindow tg = do
    now <- _gcMonotonic (_tgClock tg)
    TrimDutyWindow xs <- pruneWindow now <$> readIORef (_tgWindow tg)
    return (sum (snd <$> xs))

-- | The trim gate of a chain (spec 6.5; controller's call 7).
--
-- (The plan's record has no clock; '_tgClock' is added so that tests of the
-- 60 s span run on a virtual clock.)
data TrimGateEnv = TrimGateEnv
    { _tgBlockWaiting :: !(STM Bool)
        -- ^ a block waits for validation ('pactQueueBlockWaiting')
    , _tgLatestBlockTime :: !(IO (Maybe (Time Micros)))
        -- ^ the creation time of the latest committed block
    , _tgConfig :: !PactHistoryConfig
    , _tgWindow :: !(IORef TrimDutyWindow)
    , _tgClock :: !GateClock
    }

-- | Returns when a trim chunk may run:
--
-- * at the tip, when no block waits. A block that arrives while the chunk
--   runs waits for at most one chunk (about 20 ms).
-- * while catching up, when the trim time of the last 60 s is below
--   @_phTrimDutyCycle * 60 s@, so it stays within that plus one chunk.
--   Blocks always wait then, and the rule above would starve the trim.
--
-- The chain is catching up when the latest committed block was created more
-- than '_phCatchUpLagSeconds' ago, or no block is known. The mode is checked
-- again at least every second while the gate waits.
--
trimGate :: TrimGateEnv -> IO ()
trimGate tg = catchingUp >>= \case
    True -> duty
    False -> tip
  where
    clock = _tgClock tg
    cfg = _tgConfig tg

    catchingUp = _tgLatestBlockTime tg >>= \case
        Nothing -> return True
        Just t -> do
            now <- _gcWall clock
            return $ micros now - micros t > fromIntegral (_phCatchUpLagSeconds cfg) * 1_000_000

    micros (Time (TimeSpan (Micros m))) = m

    tip = do
        timer <- registerDelay 1_000_000
        free <- atomically $
            (True <$ (_tgBlockWaiting tg >>= check . not))
            `orElse` (False <$ (readTVar timer >>= check))
        unless free $ trimGate tg

    duty = do
        now <- _gcMonotonic clock
        TrimDutyWindow xs <- atomicModifyIORef' (_tgWindow tg) $ \w ->
            let w' = pruneWindow now w in (w', w')
        let budget = max 0 (min 1 (_phTrimDutyCycle cfg)) * dutySpan
            used = sum (snd <$> xs)
        if used < budget
        then return ()
        else do
            -- wait until enough of the oldest chunks leave the span
            let excess = used - budget
                go _ [] = now + 1
                go acc ((e, d) : rest)
                    | acc + d > excess = e + dutySpan
                    | otherwise = go (acc + d) rest
                until' = go 0 (foldr (:) [] xs)
            _gcSleep clock (min 1 (max 0.001 (until' - now)))
            trimGate tg

-- | Records a trim chunk's duty time ('_feAfterTrimChunk').
recordTrimChunk :: TrimGateEnv -> Double -> IO ()
recordTrimChunk tg d = do
    now <- _gcMonotonic (_tgClock tg)
    atomicModifyIORef' (_tgWindow tg) $ \w ->
        let TrimDutyWindow xs = pruneWindow now w
        in (TrimDutyWindow (xs Seq.|> (now, d)), ())

-- | The stats interval of 'runFreezer': 10 minutes.
statsIntervalSeconds :: Double
statsIntervalSeconds = 600

logJson :: FreezerEnv -> LogLevel -> Value -> IO ()
logJson env level v = logFunctionText (_feLogger env) level
    (T.decodeUtf8 (BL.toStrict (A.encode v)))

-- | The stats with the split counters and the cache counters of now, logged.
logStats :: FreezerEnv -> IO ()
logStats env = do
    sc <- traverse readIORef (_feSplitCounters env)
    cache <- _feCacheStats env
    diffs <- _feShadowDifferences env
    wal <- _feWalCheckpointer env
    st <- atomicModifyIORef' (_feStats env) $ \st ->
        let st' = st
                { _fsShadowDifferences = diffs
                , _fsWalCheckpointer = wal
                , _fsSplitCounters = sc
                , _fsCacheHits = fst <$> cache
                , _fsCacheMisses = snd <$> cache
                }
        in (st', st')
    logJson env Info (freezerStatsJson (_feChain env) st)

logHalted :: FreezerEnv -> Text -> IO ()
logHalted env reason = do
  atomicModifyIORef' (_feStats env) $ \st -> (st { _fsHaltedReason = Just reason }, ())
  logJson env Error $ object
    [ "FreezerHalted" .= object
        [ "chain" .= chainIdToText (_feChain env)
        , "reason" .= reason
        ]
    ]

-- | Runs the freezer of a chain until it halts (spec 6.3, last row): steps
-- while there is work, with the node-wide semaphore
-- ('_phFreezerConcurrency' units) held during each step's read, write and
-- verify but not its trim; when there is nothing to do, it polls the tip
-- every '_fePollMicros'.
--
-- A transient SQLite error (BUSY or LOCKED, 'isTransientSqliteError') is
-- retried up to 'transientRetryLimit' times in a row, with a backoff from
-- 'transientBackoffMicros' doubling each time, and counted in
-- '_fsTransientRetries'; 'freezeOnce' is crash safe, so a retry resumes the
-- step. A verify failure, a changed block hash at the frontier, any other
-- exception (a failed cold write, a full disk) or a transient error past the
-- limit logs @FreezerHalted@ at 'Error', sets '_fsHaltedReason' and returns:
-- the chain keeps running on its stores, and nothing is trimmed below an
-- unverified step. Asynchronous exceptions (cancellation at
-- shutdown) propagate; 'freezeOnce' is crash safe. The stats are logged every
-- 10 minutes and when the freezer stops.
--
runFreezer :: FreezerEnv -> QSem -> IO ()
runFreezer env sem = do
    t0 <- getMonotonicTime
    loop t0 `finally` logStats env
  where
    slot :: IO a -> IO a
    slot = bracket_ (waitQSem sem) (signalQSem sem)

    statsDue lastLog = do
        now <- getMonotonicTime
        if now - lastLog >= statsIntervalSeconds
        then now <$ logStats env
        else return lastLog

    readTip = withFreezerReadConnection env sqliteTip

    loop lastLog = retryTransient env (freezeOnceWith slot env) >>= \case
        Left e -> logHalted env ("exception: " <> T.pack (displayException e))
        Right o -> do
            lastLog' <- statsDue lastLog
            case o of
                Froze _ -> loop lastLog'
                StepDropped -> threadDelay (_fePollMicros env) >> loop lastLog'
                NothingToDo -> retryTransient env readTip >>= \case
                    Left e -> logHalted env ("exception: " <> T.pack (displayException e))
                    Right tip0 -> idle tip0 lastLog'
                HaltedVerify e -> logHalted env ("verification failed: " <> T.pack (show e))
                HaltedHash f -> logHalted env ("another block hash at the frontier " <> T.pack (show f))

    idle tip0 lastLog = do
        threadDelay (_fePollMicros env)
        lastLog' <- statsDue lastLog
        retryTransient env readTip >>= \case
            Left e -> logHalted env ("exception: " <> T.pack (displayException e))
            Right tip
                | tip == tip0 -> idle tip0 lastLog'
                | otherwise -> loop lastLog'

-- | How many transient SQLite errors in a row 'runFreezer' retries.
transientRetryLimit :: Int
transientRetryLimit = 5

-- | The first backoff after a transient SQLite error; it doubles with each
-- retry in a row (50 ms to 800 ms, 1.55 s in all).
transientBackoffMicros :: Int
transientBackoffMicros = 50_000

-- | Whether an exception is SQLite's BUSY or LOCKED: the freezer's own
-- statements raise the 'SQ3.Error' constructor's name, the Pact 4 helpers
-- @Database error: <constructor>@.
isTransientSqliteError :: SomeException -> Bool
isTransientSqliteError e = any (`T.isInfixOf` msg) ["ErrorBusy", "ErrorLocked"]
  where
    msg = T.pack (displayException e)

-- | 'trySync' that retries transient SQLite errors ('runFreezer').
retryTransient :: FreezerEnv -> IO a -> IO (Either SomeException a)
retryTransient env act = go 0
  where
    go n = trySync act >>= \case
        Left e | isTransientSqliteError e, n < transientRetryLimit -> do
            atomicModifyIORef' (_feStats env) $ \st ->
                (st { _fsTransientRetries = _fsTransientRetries st + 1 }, ())
            logg env Warn $ "transient SQLite error, retry " <> T.pack (show (n + 1)) <> " of "
                <> T.pack (show transientRetryLimit) <> ": " <> T.pack (displayException e)
            threadDelay (transientBackoffMicros * 2 ^ n)
            go (n + 1)
        r -> return r

-- | 'try' for synchronous exceptions only.
trySync :: IO a -> IO (Either SomeException a)
trySync act = try act >>= \case
    Left e | Just (_ :: SomeAsyncException) <- fromException e -> throwIO e
    r -> return r

-- | Runs 'freezeOnce' until there is nothing to do, with a gate that always
-- lets a chunk run (@--only-sync-pact@, after the replay). Logs
-- @FreezerDrained { chain, seconds }@, and @FreezerHalted@ if it halts.
--
-- A synchronous exception halts the chain as in 'runFreezer' (logged as
-- @FreezerHalted@, '_fsHaltedReason' set) and the drain returns: it does not
-- reach the node, and 'freezeOnce' is crash safe. Transient SQLite errors
-- are not retried here.
--
drainFreezer :: FreezerEnv -> IO ()
drainFreezer env = do
    t0 <- getMonotonicTime
    o <- trySync go
    t1 <- getMonotonicTime
    case o of
        Left e -> logHalted env ("exception: " <> T.pack (displayException e))
        Right (HaltedVerify e) -> logHalted env ("verification failed: " <> T.pack (show e))
        Right (HaltedHash f) -> logHalted env ("another block hash at the frontier " <> T.pack (show f))
        Right _ -> return ()
    logJson env Info $ object
        [ "FreezerDrained" .= object
            [ "chain" .= chainIdToText (_feChain env)
            , "seconds" .= (t1 - t0)
            ]
        ]
    logStats env
  where
    env' = env { _feBeforeTrimChunk = return () }
    go = freezeOnce env' >>= \case
        Froze _ -> go
        StepDropped -> go
        o -> return o
