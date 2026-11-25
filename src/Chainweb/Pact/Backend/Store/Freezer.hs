{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
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
    }

noHooks :: FreezerHooks
noHooks = FreezerHooks (return ()) (return ()) (return ()) (return ()) (return ())

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
    }
    deriving (Eq, Show)

emptyFreezerStats :: FreezerStats
emptyFreezerStats = FreezerStats
    0 0 0 0 0 0 0 0 0 0 Seq.empty 0 Nothing Nothing Nothing Nothing Nothing Nothing 0 0 0 0 Nothing

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
        ]
    ]
  where
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
-- step, and @w@ adapts to the 20 ms target (spec 6.2 step 5): halved when a
-- chunk takes over 20 ms, doubled under 5 ms, within '_twMin' and '_twMax'.
-- A chunk's cost follows the rows of its window. Tests fix the window with
-- equal bounds.
--
data TrimWindow = TrimWindow
    { _twStart :: !Word64
    , _twMin :: !Word64
    , _twMax :: !Word64
    }
    deriving (Eq, Show)

defaultTrimWindow :: TrimWindow
defaultTrimWindow = TrimWindow 1000 1 1048576

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
    , _feTrimWindow :: !TrimWindow
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
                blocks <- sqliteBlocks db (_frHeight <$> ver) (fromIntegral to)
                when (null blocks) $ logg env Warn $
                    "planned a step to height " <> T.pack (show to) <> " but BlockHistory has no block in it, although the tip "
                    <> T.pack (show tip) <> " is more than the freeze depth above the frontier "
                    <> T.pack (show (_frHeight <$> ver)) <> "; the freezer makes no progress"
                return $ if null blocks then Nothing else Just (StepPlan blocks)

-- | Reads a step (spec 6.2 step 1) in one read transaction, block by block:
-- for each block, the rows of the tables of @VersionedTableMutation@ at its
-- height that 'frozenTable' admits, in rowid order. The step ends after the
-- block at which it reaches '_phFreezeStepBytes'; a single block larger than
-- that is a step of its own.
--
readStep :: FreezerEnv -> SQLiteEnv -> StepPlan -> IO ColdStep
readStep env db (StepPlan blocks) = do
    start <- frontierTxId <$> _ccWritten (_feCold env)
    readBlocks db start (Just (_phFreezeStepBytes (_feConfig env))) blocks

-- | The tables the first step (no frontier yet, txids from 0) reads and
-- trims besides those of @VersionedTableMutation@: every Pact table of the
-- SQLite schema at that moment ('sqliteSchemaTables'). A history that does
-- not start at genesis (a compacted database) holds rows below its first
-- block that no @VersionedTableMutation@ row of a planned height names, and
-- the compaction may keep no @VersionedTableCreation@ row for their table.
-- The first step's txid range starts at 0, so it covers those rows; a table
-- first changed after the first step would otherwise join the cold catalog
-- in a later step whose range misses them, and reads below F would go to
-- cold and miss them. On a history from genesis this adds nothing: every
-- genesis row is named.
firstStepTables :: SQLiteEnv -> IO [Text]
firstStepTables = sqliteSchemaTables

-- | The bytes a row counts for in the step cap.
rowBytes :: StepRow -> Int
rowBytes r = B.length (_srRowKey r) + B.length (_srRowData r) + 8

readBlocks :: SQLiteEnv -> TxId -> Maybe Int -> [(BlockHeight, BlockHash, TxId)] -> IO ColdStep
readBlocks _ _ _ [] = throwIO $ ColdStoreException "Chainweb.Pact.Backend.Store.Freezer: a step without blocks"
readBlocks db start0 cap blocks0 = sqliteReadTransaction db $ go start0 0 HM.empty [] blocks0
  where
    -- acc: folded name to (name as first seen, rows in reverse); order: the
    -- folded names in first-seen order, reversed
    go _ _ _ _ [] = error "readBlocks: impossible"
    go start !bytes acc order ((h, hash, end) : rest) = do
        mutated <- sqliteMutatedTables db h
        base <- if start == 0 then firstStepTables db else return []
        let tables = dedup (filter frozenTable (mutated ++ base))
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

    dedup = go' S.empty
      where
        go' _ [] = []
        go' seen (t : ts)
            | S.member (foldTableName t) seen = go' seen ts
            | otherwise = t : go' (S.insert (foldTableName t) seen) ts

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
    let hi = _frEndTxId to
        fv = _ccFreezerView (_feCold env)
        ps = sqlitePactStore db
    runExceptT $ forM_ tables $ \(t, rows) -> do
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
    showKey = T.pack . show

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
-- written in @[from, F)@: for the tables mutated in those blocks, every row
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
        base <- if prev == Nothing then firstStepTables db else return []
        return (mutated ++ base)
    let work = distinctOn foldTableName
            [ (t, if keepsNoAnchors t then DropAll else KeepAnchors)
            | t <- tables, frozenTable t
            ]
    go work from 0
  where
    fe = _frEndTxId f
    tw = _feTrimWindow env

    distinctOn g = go' S.empty
      where
        go' _ [] = []
        go' seen (x@(t, _) : xs)
            | S.member (g t) seen = go' seen xs
            | otherwise = x : go' (S.insert (g t) seen) xs

    go work lo !total
        | lo >= fe = return (Trimmed total)
        | otherwise = do
            _feBeforeTrimChunk env
            w <- maybe (_twStart tw) id . _fsTrimWindow <$> readIORef (_feStats env)
            let hi = min fe (lo + Pact4.TxId w)
            r <- _feWithPactDb env $ \db -> do
                t0 <- getMonotonicTime
                r <- sqliteWithTrimSavepoint db $ sqliteBlockHashAt db (_frHeight f) >>= \case
                    Just h | h == _frHash f -> Just . sum <$>
                        forM work (\(t, rule) -> sqliteTrimChunk db t rule lo hi fe)
                    _ -> return Nothing
                t1 <- getMonotonicTime
                return ((, t1 - t0) <$> r)
            case r of
                Nothing -> return TrimHashMismatch
                Just (n, dt) -> do
                    let w'  | dt > 0.020 = max (_twMin tw) (w `div` 2)
                            | dt < 0.005 = min (_twMax tw) (w * 2)
                            | otherwise = max (_twMin tw) (min (_twMax tw) w)
                    p0 <- getMonotonicTime
                    _ccTrimProgress (_feCold env) hi
                    p1 <- getMonotonicTime
                    let ms = dt * 1000
                    modifyIORef' (_feStats env) $ \s -> s
                        { _fsTrimWindow = Just w'
                        , _fsRowsTrimmed = _fsRowsTrimmed s + n
                        , _fsTrimChunks = _fsTrimChunks s + 1
                        , _fsTrimMs = _fsTrimMs s + ms
                        , _fsTrimMaxMs = max (_fsTrimMaxMs s) ms
                        , _fsTrimSamples = Seq.drop (Seq.length (_fsTrimSamples s) + 1 - trimSampleCap)
                            (_fsTrimSamples s Seq.|> ms)
                        , _fsTrimPositionMs = _fsTrimPositionMs s + (p1 - p0) * 1000
                        }
                    _feAfterTrimChunk env (dt + (p1 - p0))
                    _fhAfterTrimChunk (_feHooks env)
                    go work hi (total + n)

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

    -- (1) a written but unverified step: verify it again and publish it. A
    -- rewind below the written frontier is refused, so the guard is not
    -- needed here.
    recover meta = case _cmWritten meta of
        Nothing -> throwIO $ ColdStoreException
            "Chainweb.Pact.Backend.Store.Freezer: a verified frontier without a written one"
        Just w -> do
            let v = _cmVerified meta
            logg env Info $ "verifying the written but unpublished step to " <> T.pack (show w)
            r <- slot $ withFreezerReadConnection env $ \db ->
                sqliteBlockHashAt db (_frHeight w) >>= \case
                    Just h | h == _frHash w -> do
                        blocks <- sqliteBlocks db (_frHeight <$> v) (_frHeight w)
                        s <- readBlocks db (frontierTxId v) Nothing blocks
                        _fhAfterRead hooks
                        if _stpTo s /= w
                        then return $ Left $ HaltedVerify $ VerifyFailure "" Nothing $
                            "SQLite's blocks end at " <> T.pack (show (_stpTo s))
                                <> ", not at the written frontier"
                        else timed (verifyStep env db s) >>= \case
                            (Left e, _) -> return (Left (HaltedVerify e))
                            (Right (), vt) -> do
                                addStats $ \st -> st { _fsVerifyMs = _fsVerifyMs st + vt }
                                return (Right s)
                    _ -> return (Left (HaltedHash w))
            case r of
                Left o -> halted o
                Right s -> do
                    _fhAfterVerify hooks
                    _ccPublish cold
                    _fhAfterPublish hooks
                    trimAndReport w (frontierTxId v) (Just s)

    -- (3) a new step
    step = do
        r <- slot $ do
            gen <- guardGeneration rg
            withFreezerReadConnection env $ \db -> planStep env db >>= \case
                Nothing -> return (Left NothingToDo)
                Just plan -> do
                    s <- readStep env db plan
                    _fhAfterRead hooks
                    claimed <- guardClaimStep rg gen (_frEndTxId (_stpTo s))
                    if not claimed
                    then do
                        logg env Info $ "a rewind of the hot store overlapped the read of the step to "
                            <> T.pack (show (_stpTo s)) <> "; the step is dropped and read again"
                        return (Left StepDropped)
                    else flip finally (guardReleaseStep rg) $ do
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
                                return (Right (s, lo))
        case r of
            Left NothingToDo -> return NothingToDo
            Left StepDropped -> return StepDropped
            Left o -> halted o
            Right (s, lo) -> trimAndReport (_stpTo s) lo (Just s)

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
            forM_ ms $ \s -> do
                let count p = sum [ length rs | (t, rs) <- _stpTables s, p (isSysTable t) ]
                    rows = count (const True)
                    bytes = sum [ rowBytes r | (_, rs) <- _stpTables s, r <- rs ]
                addStats $ \st -> st
                    { _fsSteps = _fsSteps st + 1
                    , _fsRowsUser = _fsRowsUser st + count not
                    , _fsRowsSys = _fsRowsSys st + count id
                    , _fsBytes = _fsBytes st + bytes
                    }
                logg env Info $ "froze " <> T.pack (show rows) <> " rows to " <> T.pack (show f)
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
    st <- atomicModifyIORef' (_feStats env) $ \st ->
        let st' = st
                { _fsShadowDifferences = diffs
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
drainFreezer :: FreezerEnv -> IO ()
drainFreezer env = do
    t0 <- getMonotonicTime
    o <- go
    t1 <- getMonotonicTime
    case o of
        HaltedVerify e -> logHalted env ("verification failed: " <> T.pack (show e))
        HaltedHash f -> logHalted env ("another block hash at the frontier " <> T.pack (show f))
        _ -> return ()
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
