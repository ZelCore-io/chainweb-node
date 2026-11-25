{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.Split
--
-- Split routing (storage redesign P3, spec 4.3 "Reads" and 5): a 'PactStore'
-- that answers each read from the hot SQLite store and, below the verified
-- frontier, from the cold store.
--
-- Rules, for every routed operation:
--
-- * The hot statement runs first, and a @Left@ is returned unchanged, so a
--   missing table fails exactly as the legacy store fails, and the cold store
--   is never asked about it.
-- * @F@ is the end txid of the verified frontier ('_ccVerified'), read once
--   per operation, before the hot statement. @F = 0@ (nothing frozen) never
--   asks the cold store.
-- * Tables that 'frozenTable' rejects are answered by SQLite alone. The cold
--   store is only asked about a table in its published catalog.
-- * Below @F@ the hot store keeps the newest version of each key (anchors),
--   except @SYS:Pacts@ ('keepsNoAnchors'), which keeps nothing below @F@.
--
-- The freezer's view of the cold store is never used here: it is not clamped
-- to the verified frontier.
--
module Chainweb.Pact.Backend.Store.Split
( SplitEnv(..)
, SplitCounters(..)
, emptySplitCounters
, SplitStoreError(..)
, splitPactStore
) where

import Control.Exception
import Control.Monad

import Data.ByteString (ByteString)
import Data.IORef
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word

import GHC.Clock (getMonotonicTimeNSec)

import qualified Database.SQLite3.Direct as SQ3

import qualified Pact.Types.Persistence as Pact4

-- internal modules

import Chainweb.BlockHeader.Internal (ParentHeader)
import Chainweb.ChainId
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Config (PactHistoryStore(..))
import Chainweb.Pact.Backend.Store.Layout (PactLayoutError(..))
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Utils (sshow)

type TxId = Pact4.TxId

-- -------------------------------------------------------------------------- --
-- Environment and counters

data SplitEnv = SplitEnv
    { _seChain :: !ChainId
    , _seHot :: !PactStore
        -- ^ 'sqlitePactStore' of the hot connection
    , _seCold :: !ColdChain
    , _seCounters :: !(IORef SplitCounters)
    }

-- | Read counters of a chain.
--
-- Every @SYS:Pacts@ read counts as a hot hit, or as a cold read (a hit or a
-- miss in cold); a hot miss while nothing is frozen counts nowhere. Every call
-- to the cold store counts in '_scColdReads'. @SYS:Pacts@ cold reads are
-- timed (ruling 5): total and maximum, in microseconds.
data SplitCounters = SplitCounters
    { _scColdReads
    , _scSysPactsHotHits
    , _scSysPactsColdReads
    , _scSysPactsColdHits
    , _scSysPactsColdMicros
    , _scSysPactsColdMaxMicros :: !Word64
    }
    deriving (Eq, Show)

emptySplitCounters :: SplitCounters
emptySplitCounters = SplitCounters 0 0 0 0 0 0

-- | A rewind that would cross the written frontier (spec 5, "Rewind below
-- the frontier"; owner decision 2).
data SplitStoreError = RewindBelowFrontier
    { _rbfChain :: !ChainId
    , _rbfTargetEndTxId :: !TxId
    , _rbfFrontier :: !Frontier
        -- ^ the written frontier
    }
    deriving (Eq)

instance Show SplitStoreError where
    show = T.unpack . splitStoreErrorMessage

instance Exception SplitStoreError where
    displayException = T.unpack . splitStoreErrorMessage

splitStoreErrorMessage :: SplitStoreError -> Text
splitStoreErrorMessage (RewindBelowFrontier c target f) = T.unwords
    [ "Refusing to rewind the Pact state of chain", chainIdToText c
    , "to end txid", sshow (txIdWord target) <> ":"
    , "the cold store holds the frozen history up to the frontier at height"
    , sshow (_frHeight f) <> ", block", blockHashText
    , "(end txid", sshow (txIdWord (_frEndTxId f)) <> "),"
    , "and history below the frontier cannot be rewound."
    , "This happens when the block database is older than the frontier, or with"
    , "--initial-block-height-limit or an initial cut file below it."
    , "Ways out: restore a whole snapshot (block database, SQLite and cold store together),"
    , "replay Pact from the chain database, or rebuild legacy files with storage-export."
    ]
  where
    blockHashText = sshow (_frHash f)

txIdWord :: TxId -> Word64
txIdWord (Pact4.TxId w) = w

-- -------------------------------------------------------------------------- --
-- Store

splitPactStore :: SplitEnv -> PactStore
splitPactStore env = hot
    { _psReadNewest = \t k mb -> readNewest env t k mb
    , _psReadPrev = \t k b -> readNewest env t k (Just b)
    , _psReadHistorical = readHistorical env
    , _psDistinctKeys = distinctKeys env
    , _psTxIdsAfter = txIdsAfter env
    , _psRowsAtTxId = rowsAtTxId env
    , _psRowsInTxIdRange = rowsInTxIdRange env
    , _psRewindDbTo = rewindDbTo env
    , _psBackupTo = \_ -> throwIO (BackupRefused HistorySplit)
    }
  where
    hot = _seHot env

-- -------------------------------------------------------------------------- --
-- Routing helpers

-- | The verified frontier's end txid; 0 when nothing is verified.
verifiedTxId :: SplitEnv -> IO TxId
verifiedTxId env = frontierTxId <$> _ccVerified (_seCold env)

isPacts :: Text -> Bool
isPacts = keepsNoAnchors

-- | System tables, under SQLite's case folding of the prefix (as the cold
-- store's 'rowsInTxIdRange' decides it).
isSys :: Text -> Bool
isSys t = isSysTable (T.toUpper (T.take 4 t) <> T.drop 4 t)

bump :: SplitEnv -> (SplitCounters -> SplitCounters) -> IO ()
bump env f = atomicModifyIORef' (_seCounters env) (\c -> (f c, ()))

-- | Runs a cold read if the table is in the published catalog, counting it;
-- the default otherwise.
cold :: SplitEnv -> Text -> a -> IO a -> IO a
cold env t def act = _ccTableId (_seCold env) t >>= \case
    Nothing -> return def
    Just _ -> do
        bump env $ \c -> c { _scColdReads = _scColdReads c + 1 }
        act

-- | 'cold' for a @SYS:Pacts@ read: also counted, timed and its hit counted.
coldPacts :: SplitEnv -> Text -> IO (Maybe a) -> IO (Maybe a)
coldPacts env t act = _ccTableId (_seCold env) t >>= \case
    Nothing -> return Nothing
    Just _ -> do
        t0 <- getMonotonicTimeNSec
        r <- act
        t1 <- getMonotonicTimeNSec
        let us = (t1 - t0) `div` 1000
            hit = maybe 0 (const 1) r
        bump env $ \c -> c
            { _scColdReads = _scColdReads c + 1
            , _scSysPactsColdReads = _scSysPactsColdReads c + 1
            , _scSysPactsColdHits = _scSysPactsColdHits c + hit
            , _scSysPactsColdMicros = _scSysPactsColdMicros c + us
            , _scSysPactsColdMaxMicros = max (_scSysPactsColdMaxMicros c) us
            }
        return r

-- -------------------------------------------------------------------------- --
-- Reads of one key

-- | The cold bound of a key read after a hot miss, if the cold store must be
-- asked: for @SYS:Pacts@ @min b F@ (unbounded: @F@), for other frozen
-- tables @b@ when @b < F@.
keyReadBound :: Text -> TxId -> Maybe TxId -> Maybe TxId
keyReadBound t f mb
    | f == 0 || not (frozenTable t) = Nothing
    | isPacts t = case maybe f (min f) mb of
        0 -> Nothing
        b -> Just b
    | otherwise = case mb of
        Just b | b < f -> Just b
        _ -> Nothing

-- | The routed key read; the hot read and the cold read return the stored
-- row key and the row data.
keyRead
    :: SplitEnv
    -> Text
    -> Maybe TxId
    -> IO (Either SQ3.Error (Maybe a))
    -> (TxId -> IO (Maybe a))
    -> IO (Either SQ3.Error (Maybe a))
keyRead env t mb hotRead coldRead = do
    f <- verifiedTxId env
    hotRead >>= \case
        Left e -> return (Left e)
        Right (Just v) -> do
            when (isPacts t) $
                bump env $ \c -> c { _scSysPactsHotHits = _scSysPactsHotHits c + 1 }
            return (Right (Just v))
        Right Nothing -> case keyReadBound t f mb of
            Nothing -> return (Right Nothing)
            Just b
                | isPacts t -> Right <$> coldPacts env t (coldRead b)
                | otherwise -> Right <$> cold env t Nothing (coldRead b)

readNewest :: SplitEnv -> Text -> ByteString -> Maybe TxId -> IO (Either SQ3.Error (Maybe ByteString))
readNewest env t k mb = keyRead env t mb
    (_psReadNewest (_seHot env) t k mb)
    (\b -> fmap snd <$> _ccReadNewest (_seCold env) t k b)

readHistorical :: SplitEnv -> Text -> ByteString -> TxId -> IO (Either SQ3.Error (Maybe (ByteString, ByteString)))
readHistorical env t k b = keyRead env t (Just b)
    (_psReadHistorical (_seHot env) t k b)
    (\b' -> _ccReadNewest (_seCold env) t k b')

-- -------------------------------------------------------------------------- --
-- Reads of many rows

-- | Both legacy @keys@ statements return ascending byte order (the @UNIQUE@
-- autoindex, pinned by "legacy query plans"), which is the cold order.
distinctKeys :: SplitEnv -> KeysQuery -> Text -> IO (Either SQ3.Error [ByteString])
distinctKeys env q t = do
    f <- verifiedTxId env
    _psDistinctKeys (_seHot env) q t >>= \case
        Left e -> return (Left e)
        Right hot
            | f == 0 || not (frozenTable t) -> return (Right hot)
            | isPacts t -> do
                let b = maybe f (min f) bound
                cs <- if b == 0 then return [] else cold env t [] (_ccDistinctKeys (_seCold env) t b)
                return $ Right $ S.toAscList (S.fromList cs <> S.fromList hot)
            | otherwise -> case bound of
                Just b | b < f -> Right <$> cold env t [] (_ccDistinctKeys (_seCold env) t b)
                _ -> return (Right hot)
  where
    bound = case q of
        KeysAsStored mb -> mb
        KeysOrderedByKey b -> Just b

-- | The legacy statement returns txids in descending order (the @txid DESC@
-- index, pinned by "legacy query plans"); the split answer is sorted the
-- same way.
txIdsAfter :: SplitEnv -> Text -> TxId -> IO (Either SQ3.Error [TxId])
txIdsAfter env t x = do
    f <- verifiedTxId env
    _psTxIdsAfter (_seHot env) t x >>= \case
        Left e -> return (Left e)
        Right hot
            | f == 0 || not (frozenTable t) || isSys t -> return (Right hot)
            | txIdWord x + 1 < txIdWord f -> do
                cs <- cold env t [] (_ccTxIdsBetween (_seCold env) t x f)
                return $ Right $ S.toDescList (S.fromList cs <> S.fromList hot)
            | otherwise -> return (Right hot)

rowsAtTxId :: SplitEnv -> Text -> TxId -> IO (Either SQ3.Error [(ByteString, ByteString)])
rowsAtTxId env t x = do
    f <- verifiedTxId env
    _psRowsAtTxId (_seHot env) t x >>= \case
        Left e -> return (Left e)
        Right hot
            | f == 0 || not (frozenTable t) || isSys t -> return (Right hot)
            | x < f -> Right . maybe [] id <$> cold env t Nothing (_ccRowsAtTxId (_seCold env) t x)
            | otherwise -> return (Right hot)

-- | Hot rows at or above @F@, then cold rows in @[from, min to F)@: both
-- parts txid descending, so the whole is.
--
-- Within one txid, cold returns rowid order for user tables and key order
-- for @SYS:*@ tables, which have no by-txid lists; the legacy statement
-- returns rowid order for both. The only caller, @getBlockHistory@, is
-- reached from tests alone. The cold read of a @SYS:*@ table scans the whole
-- table's @R@ range (parked pointer M2); this is bounded by that caller.
rowsInTxIdRange :: SplitEnv -> Text -> TxId -> TxId -> IO (Either SQ3.Error [(TxId, ByteString, ByteString)])
rowsInTxIdRange env t from to = do
    f <- verifiedTxId env
    _psRowsInTxIdRange (_seHot env) t from to >>= \case
        Left e -> return (Left e)
        Right hot
            | f == 0 || not (frozenTable t) -> return (Right hot)
            | otherwise -> do
                let hot' = [ r | r@(x, _, _) <- hot, x >= f ]
                cs <- if from < min to f
                    then cold env t [] (_ccRowsInTxIdRange (_seCold env) t from to)
                    else return []
                return (Right (hot' ++ cs))

-- -------------------------------------------------------------------------- --
-- Rewind

-- | Refuses a rewind whose target ends below the __written__ frontier, before
-- any SQL write; otherwise the hot rewind. A target the hot store does not
-- know is left to the hot rewind, which fails as the legacy store fails.
rewindDbTo :: SplitEnv -> Maybe ParentHeader -> IO TxId
rewindDbTo env p = do
    target <- _psGetEndTxId (_seHot env) "rewindDbToBlock" p
    written <- _ccWritten (_seCold env)
    case (target, written) of
        (Historical e, Just w) | e < _frEndTxId w ->
            throwIO (RewindBelowFrontier (_seChain env) e w)
        _ -> _psRewindDbTo (_seHot env) p
