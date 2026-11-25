{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
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
-- and the trim of the last published step runs again (deletes are
-- idempotent). A verification failure or a changed block hash at the
-- frontier halts the chain's freezer: SQLite is not trimmed and the node
-- keeps running on SQLite.
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
) where

import Control.Exception
import Control.Monad
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except

import qualified Data.ByteString as B
import qualified Data.HashMap.Strict as HM
import Data.IORef
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T

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
import Chainweb.Pact.Backend.Store.Types (PactStore(..))
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Pact.Backend.Utils (chainDbFileName)

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

-- | What the freezer of a chain has done since it started.
--
data FreezerStats = FreezerStats
    { _fsSteps :: !Int
        -- ^ steps published
    , _fsRowsFrozen :: !Int
    , _fsRowsTrimmed :: !Int
    , _fsTrimChunks :: !Int
    , _fsChunkLimit :: !Int
        -- ^ the current trim chunk size, adapted to the 20 ms target
    , _fsStartTrimDone :: !Bool
        -- ^ whether the trim of the last published step ran after the start
    , _fsHaltedHash :: !(Maybe Frontier)
        -- ^ the trim found another block hash at this frontier; the freezer
        -- of the chain is halted
    }
    deriving (Eq, Show)

emptyFreezerStats :: FreezerStats
emptyFreezerStats = FreezerStats 0 0 0 0 1000 False Nothing

data FreezerEnv = FreezerEnv
    { _feChain :: !ChainId
    , _feCold :: !ColdChain
    , _feLayout :: !PactLayout
    , _feConfig :: !PactHistoryConfig
    , _feWithPactDb :: forall a. (SQLiteEnv -> IO a) -> IO a
        -- ^ runs on the Pact connection, under @cpModuleCacheVar@
    , _feBeforeTrimChunk :: !(IO ())
        -- ^ runs before every trim chunk (Task 25's gate)
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
        let cfg = _feConfig env
            depth = toInteger (_phFreezeDepth cfg)
            stepHeights = max 1 (toInteger (_phFreezeStepHeights cfg))
            vh = maybe (-1) (toInteger . _frHeight) ver
            t = toInteger tip
        if t - vh <= depth + stepHeights
        then return Nothing
        else do
            let to = min (vh + stepHeights) (t - depth)
            blocks <- sqliteBlocks db (_frHeight <$> ver) (fromIntegral to)
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
        tables <- dedup . filter frozenTable <$> sqliteMutatedTables db h
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
-- every row below @F@ of @SYS:Pacts@ ('DropAll').
--
-- 'ensureSplitDir' runs first. Each chunk runs '_feBeforeTrimChunk', then,
-- on the Pact connection in the savepoint @trim@, checks that @BlockHistory@
-- still has the frontier's hash at its height ('TrimHashMismatch' and nothing
-- deleted otherwise) and deletes at most the current chunk size. The chunk
-- size adapts to the 20 ms target: halved above 20 ms, doubled below 5 ms,
-- within 16 and 65,536.
--
trimStep :: FreezerEnv -> Frontier -> TxId -> IO TrimOutcome
trimStep env f from = do
    ensureSplitDir (_feLogger env) (_feLayout env) (_feChain env) (_frHeight f)
    tables <- _feWithPactDb env $ \db -> do
        prev <- sqliteBlockAtOrBelow db (_frHeight f) from
        sqliteMutatedTablesBetween db prev (_frHeight f)
    let work = distinctOn foldTableName
            [ (t, if foldTableName t == "sys:pacts" then DropAll else KeepAnchors)
            | t <- tables, frozenTable t
            ]
    go work 0
  where
    distinctOn g = go' S.empty
      where
        go' _ [] = []
        go' seen (x@(t, _) : xs)
            | S.member (g t) seen = go' seen xs
            | otherwise = x : go' (S.insert (g t) seen) xs

    go [] !total = return (Trimmed total)
    go ((t, rule) : rest) !total = do
        _feBeforeTrimChunk env
        limit <- _fsChunkLimit <$> readIORef (_feStats env)
        r <- _feWithPactDb env $ \db -> do
            t0 <- getMonotonicTime
            r <- sqliteWithTrimSavepoint db $ sqliteBlockHashAt db (_frHeight f) >>= \case
                Just h | h == _frHash f -> Just <$> sqliteTrimChunk db t rule from (_frEndTxId f) limit
                _ -> return Nothing
            t1 <- getMonotonicTime
            return ((, t1 - t0) <$> r)
        case r of
            Nothing -> return TrimHashMismatch
            Just (n, dt) -> do
                let limit'
                        | dt > 0.020 = max 16 (limit `div` 2)
                        | dt < 0.005 = min 65536 (limit * 2)
                        | otherwise = limit
                modifyIORef' (_feStats env) $ \s -> s
                    { _fsChunkLimit = limit'
                    , _fsRowsTrimmed = _fsRowsTrimmed s + n
                    , _fsTrimChunks = _fsTrimChunks s + 1
                    }
                _fhAfterTrimChunk (_feHooks env)
                -- fewer than the limit: the table is done
                if n < limit
                then go rest (total + n)
                else go ((t, rule) : rest) (total + n)

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
    deriving (Eq, Show)

-- | One freezer step (spec 6.2, 6.3):
--
-- 1. if the written frontier is ahead of the verified one, re-read that range
--    on a fresh read connection, verify it, publish it and trim it;
-- 2. on the first call after a start, trim @[trimFrom, F)@ once, which
--    finishes a trim that a crash cut short;
-- 3. otherwise plan, read, write, verify, publish and trim one step.
--
-- A write failure (an exception of the cold store) propagates; nothing is
-- trimmed then.
--
freezeOnce :: FreezerEnv -> IO StepOutcome
freezeOnce env = do
    st <- readIORef (_feStats env)
    meta <- _ccMeta cold
    case _fsHaltedHash st of
        Just f -> return (HaltedHash f)
        Nothing
            | _cmWritten meta /= _cmVerified meta -> recover meta
            | not (_fsStartTrimDone st) -> case _cmVerified meta of
                Just f | _cmTrimFrom meta < _frEndTxId f -> do
                    logg env Info $ "finishing the trim of the last step below " <> T.pack (show f)
                    trimAndReport f (_cmTrimFrom meta) Nothing >>= \case
                        o@(HaltedHash _) -> return o
                        _ -> markStartTrim >> step
                _ -> markStartTrim >> step
            | otherwise -> step
  where
    cold = _feCold env
    hooks = _feHooks env

    markStartTrim = modifyIORef' (_feStats env) $ \s -> s { _fsStartTrimDone = True }

    -- (1) a written but unverified step: verify it again and publish it
    recover meta = case _cmWritten meta of
        Nothing -> throwIO $ ColdStoreException
            "Chainweb.Pact.Backend.Store.Freezer: a verified frontier without a written one"
        Just w -> do
            let v = _cmVerified meta
            logg env Info $ "verifying the written but unpublished step to " <> T.pack (show w)
            r <- withFreezerReadConnection env $ \db ->
                sqliteBlockHashAt db (_frHeight w) >>= \case
                    Just h | h == _frHash w -> do
                        blocks <- sqliteBlocks db (_frHeight <$> v) (_frHeight w)
                        s <- readBlocks db (frontierTxId v) Nothing blocks
                        _fhAfterRead hooks
                        if _stpTo s /= w
                        then return $ Left $ HaltedVerify $ VerifyFailure "" Nothing $
                            "SQLite's blocks end at " <> T.pack (show (_stpTo s))
                                <> ", not at the written frontier"
                        else verifyStep env db s >>= \case
                            Left e -> return (Left (HaltedVerify e))
                            Right () -> return (Right s)
                    _ -> return (Left (HaltedHash w))
            case r of
                Left o -> halted o
                Right s -> do
                    _fhAfterVerify hooks
                    _ccPublish cold
                    _fhAfterPublish hooks
                    o <- trimAndReport w (frontierTxId v) (Just s)
                    case o of
                        Froze _ -> markStartTrim
                        _ -> return ()
                    return o

    -- (3) a new step
    step = do
        r <- withFreezerReadConnection env $ \db -> planStep env db >>= \case
            Nothing -> return (Left NothingToDo)
            Just plan -> do
                s <- readStep env db plan
                _fhAfterRead hooks
                lo <- frontierTxId <$> _ccVerified cold
                _ccWriteStep cold s
                _fhAfterWrite hooks
                verifyStep env db s >>= \case
                    Left e -> return (Left (HaltedVerify e))
                    Right () -> do
                        _fhAfterVerify hooks
                        _ccPublish cold
                        _fhAfterPublish hooks
                        return (Right (s, lo))
        case r of
            Left NothingToDo -> return NothingToDo
            Left o -> halted o
            Right (s, lo) -> trimAndReport (_stpTo s) lo (Just s)

    trimAndReport f lo ms = trimStep env f lo >>= \case
        TrimHashMismatch -> halted (HaltedHash f)
        Trimmed n -> do
            forM_ ms $ \s -> do
                let rows = sum (length . snd <$> _stpTables s)
                modifyIORef' (_feStats env) $ \st -> st
                    { _fsSteps = _fsSteps st + 1
                    , _fsRowsFrozen = _fsRowsFrozen st + rows
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
