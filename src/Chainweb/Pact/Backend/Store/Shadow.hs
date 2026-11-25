{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.Shadow
--
-- Shadow mode (storage redesign P3, spec 7.5): "Every 'PactStore' read goes
-- to both and any difference is logged with the query. The legacy answer is
-- returned."
--
-- Rules:
--
-- * Reads and block lookups run on the legacy store, then on the split store
--   (inside 'try'). The two outcomes are compared exactly: results by 'Eq',
--   exceptions by their text ('displayException'). A difference is logged as
--   a @ShadowDifference@ JSON record at 'Warn'. The legacy result is
--   returned, and a legacy exception is rethrown.
-- * Writes ('_psCommitBlock', '_psRewindDbTo', '_psInitSchema') run on the
--   legacy store first; a legacy exception is rethrown without running the
--   split side. Then the split side runs; an outcome that differs from the
--   legacy one (a @Left@, another result or an exception) is one difference
--   and marks the split side broken. From then on the chain runs on the
--   legacy store alone, reads included; that is logged once
--   (@ShadowBroken@, at 'Error').
-- * '_psWithBatch' and '_psWithReadSnapshot' nest: legacy around split. A
--   failure of the split savepoint itself (not of the action) breaks the
--   split side instead of failing the legacy one.
-- * '_psBackupTo' is refused ('BackupRefused').
--
-- One accepted difference is normalised before the comparison: within one
-- txid, the cold part of a @SYS:*@ table's '_psRowsInTxIdRange' comes in key
-- order, not rowid order ("Chainweb.Pact.Backend.Store.Split"). Its only
-- caller, @getBlockHistory@, is reached from tests alone.
--
-- Values in the log are rendered as the bytes of 'show' of the result (or
-- @exception: @ and the exception text), in hex, truncated to 512 bytes, each
-- followed by the SHA-256 of the full value ('renderShadowValue'). Row keys
-- and the hash list of @doLookupSuccessful@ are bounded the same way.
--
-- The split side never changes, or throws into, what the caller gets from
-- the legacy store:
--
-- * A split result is forced to normal form inside the 'try' that guards
--   the split call, so a bottom in it is a split exception.
-- * The comparison, the rendering and the log record are built inside a
--   guard that catches every synchronous exception; a failure there is
--   logged as a difference itself. The record's text is forced before it
--   reaches the logger.
-- * The legacy result is returned as the legacy store built it: shadow does
--   not force it beyond what the comparison needs, and a bottom that the
--   comparison hits is a difference, not an exception for the caller.
-- * In a nest, an exception of the action reaches the caller even when the
--   split savepoint fails afterwards; that failure breaks the split side.
--
-- The difference log is bounded: per op, the first
-- 'shadowFullDifferences' differences are logged in full. After that one
-- @ShadowDifferencesSuppressed@ note is logged, and the next differences of
-- the op are only counted, with one @ShadowDifferenceSummary@ record (the
-- count since the last one) at most every 'shadowSummarySeconds', logged
-- with the next difference once that time has passed.
--
module Chainweb.Pact.Backend.Store.Shadow
( ShadowEnv(..)
, ShadowLogState
, newShadowLogState
, shadowPactStore
, shadowFullDifferences
, shadowSummarySeconds
, renderShadowValue
, normaliseSysRange
) where

import Control.DeepSeq (NFData, force)
import Control.Exception
import Control.Monad

import Crypto.Hash (SHA256(..), hashWith)

import Data.Aeson (Value, object, (.=))
import qualified Data.Aeson as A
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as SB
import Data.Function (on)
import qualified Data.HashMap.Strict as HM
import Data.IORef
import Data.List (groupBy, sortOn)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Data.Word

import System.LogLevel

import qualified Pact.Types.Persistence as Pact4

-- internal modules

import Chainweb.BlockHeader.Internal (ParentHeader(..), blockHash, blockHeight)
import Chainweb.BlockHash (BlockHash)
import Chainweb.BlockHeight (BlockHeight)
import Chainweb.ChainId
import Chainweb.Logger (GenericLogger, logFunctionText)
import Chainweb.Pact.Backend.Store.Cold (isSysTable)
import Chainweb.Pact.Backend.Store.Config (PactHistoryStore(..))
import Chainweb.Pact.Backend.Store.Layout (PactLayoutError(..))
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Utils (sshow)

import Control.Lens (view)

type TxId = Pact4.TxId

-- -------------------------------------------------------------------------- --
-- Environment

data ShadowEnv = ShadowEnv
    { _shChain :: !ChainId
    , _shLegacy :: !PactStore
        -- ^ 'sqlitePactStore' of the untouched legacy file
    , _shSplit :: !PactStore
        -- ^ 'splitPactStore' of the chain
    , _shLogger :: !GenericLogger
    , _shDifferences :: !(IORef Word64)
        -- ^ the differences found so far, logged or suppressed
    , _shBroken :: !(IORef Bool)
        -- ^ set by the first failed write on the split side; the chain then
        -- runs on the legacy store alone
    , _shClock :: !(IO Double)
        -- ^ monotonic seconds, for the summary interval ('getMonotonicTime';
        -- tests use a virtual clock)
    , _shLogState :: !ShadowLogState
        -- ^ per-op counts of the rate limit ('newShadowLogState')
    }

-- | Per op: the differences logged in full, the ones suppressed in all and
-- since the last summary, and the time of the last summary.
data OpLog = OpLog
    { _olLogged :: !Word64
    , _olSuppressed :: !Word64
    , _olPending :: !Word64
    , _olLastSummary :: !Double
    }

newtype ShadowLogState = ShadowLogState (IORef (HM.HashMap Text OpLog))

newShadowLogState :: IO ShadowLogState
newShadowLogState = ShadowLogState <$> newIORef mempty

-- | Differences logged in full per op before the log switches to summaries.
shadowFullDifferences :: Word64
shadowFullDifferences = 100

-- | The least time between two summary records of one op: 10 minutes.
shadowSummarySeconds :: Double
shadowSummarySeconds = 600

-- -------------------------------------------------------------------------- --
-- Store

shadowPactStore :: ShadowEnv -> PactStore
shadowPactStore env = PactStore
    { _psGetLatestBlock = rd "getLatestBlock" [] id _psGetLatestBlock
    , _psGetEarliestBlock = rd "getEarliestBlock" [] id _psGetEarliestBlock
    , _psLookupBlock = \hh ->
        rd "lookupBlock" (blockArgs hh) id (\p -> _psLookupBlock p hh)
    , _psGetBlockParent = \v c hh ->
        rd "getBlockParent" (chainIdToText c : blockArgs hh) id (\p -> _psGetBlockParent p v c hh)
    , _psGetEndTxId = \msg ph ->
        rd "getEndTxId" [msg, parentArg ph] id (\p -> _psGetEndTxId p msg ph)
    , _psGetEndTxId' = \msg h hash ->
        rd "getEndTxId'" (msg : blockArgs (h, hash)) id (\p -> _psGetEndTxId' p msg h hash)
    , _psDoLookupSuccessful = \h hs ->
        rd "doLookupSuccessful" [sshow h, bytesArg (B.concat (SB.fromShort <$> V.toList hs))] id
            (\p -> _psDoLookupSuccessful p h hs)
    , _psRewindDbTo = \ph ->
        wr "rewindDbTo" [parentArg ph] (\p -> _psRewindDbTo p ph)
    , _psBackupTo = \_ -> throwIO (BackupRefused HistoryShadow)
    , _psInitSchema = wr "initSchema" [] _psInitSchema

    , _psReadNewest = \t k mb ->
        rd "readNewest" [t, bytesArg k, maybe "none" txArg mb] id (\p -> _psReadNewest p t k mb)
    , _psRowsInTxIdRange = \t from to ->
        rd "rowsInTxIdRange" [t, txArg from, txArg to] (normaliseSysRange t)
            (\p -> _psRowsInTxIdRange p t from to)
    , _psReadPrev = \t k b ->
        rd "readPrev" [t, bytesArg k, txArg b] id (\p -> _psReadPrev p t k b)
    , _psReadHistorical = \t k b ->
        rd "readHistorical" [t, bytesArg k, txArg b] id (\p -> _psReadHistorical p t k b)
    , _psDistinctKeys = \q t ->
        rd "distinctKeys" [t, keysArg q] id (\p -> _psDistinctKeys p q t)
    , _psTxIdsAfter = \t x ->
        rd "txIdsAfter" [t, txArg x] id (\p -> _psTxIdsAfter p t x)
    , _psRowsAtTxId = \t x ->
        rd "rowsAtTxId" [t, txArg x] id (\p -> _psRowsAtTxId p t x)
    , _psTableExistsAt = \t h ->
        rd "tableExistsAt" [t, sshow h] id (\p -> _psTableExistsAt p t h)
    , _psTableInCatalog = \lower t ->
        rd "tableInCatalog" [sshow lower, t] id (\p -> _psTableInCatalog p lower t)

    , _psCommitBlock = \bc ->
        wr "commitBlock" (blockArgs (_bcHeight bc, _bcHash bc) ++ [txArg (_bcEndTxId bc)])
            (\p -> _psCommitBlock p bc)
    , _psWithBatch = nest "withBatch" _psWithBatch
    , _psWithReadSnapshot = nest "withReadSnapshot" _psWithReadSnapshot
    }
  where
    rd :: (Eq a, Show a, NFData a) => Text -> [Text] -> (a -> a) -> (PactStore -> IO a) -> IO a
    rd = shadowRead env

    wr :: (Eq a, Show a, NFData a) => Text -> [Text] -> (PactStore -> IO a) -> IO a
    wr = shadowWrite env

    nest :: Text -> (PactStore -> (forall b. IO b -> IO b)) -> (forall a. IO a -> IO a)
    nest = shadowNest env

-- -------------------------------------------------------------------------- --
-- Running both sides

-- | 'try' that does not catch asynchronous exceptions.
trySync :: IO a -> IO (Either SomeException a)
trySync act = try act >>= \case
    Left e | isAsync e -> throwIO e
    r -> return r

isAsync :: SomeException -> Bool
isAsync e = case fromException e of
    Just (_ :: SomeAsyncException) -> True
    Nothing -> False

-- | The split call, its result forced inside the 'try'.
runSplit :: NFData a => ShadowEnv -> (PactStore -> IO a) -> IO (Either SomeException a)
runSplit env f = trySync (f (_shSplit env) >>= evaluate . force)

-- | Whether the outcomes differ. A comparison that throws counts as a
-- difference.
differ :: Eq a => Either SomeException a -> Either SomeException a -> IO Bool
differ a b = either (const True) not <$> trySync (evaluate (sameOutcome a b))

sameOutcome :: Eq a => Either SomeException a -> Either SomeException a -> Bool
sameOutcome (Right a) (Right b) = a == b
sameOutcome (Left a) (Left b) = displayException a == displayException b
sameOutcome _ _ = False

shadowRead
    :: (Eq a, Show a, NFData a)
    => ShadowEnv -> Text -> [Text] -> (a -> a) -> (PactStore -> IO a) -> IO a
shadowRead env op args norm f = do
    legacy <- trySync (f (_shLegacy env))
    broken <- readIORef (_shBroken env)
    unless broken $ do
        split <- runSplit env f
        d <- differ (norm <$> legacy) (norm <$> split)
        when d $ logDifference env op args legacy split
    either throwIO return legacy

shadowWrite
    :: (Eq a, Show a, NFData a)
    => ShadowEnv -> Text -> [Text] -> (PactStore -> IO a) -> IO a
shadowWrite env op args f = do
    legacy <- f (_shLegacy env)
    broken <- readIORef (_shBroken env)
    unless broken $ do
        split <- runSplit env f
        d <- differ (Right legacy) split
        when d $ do
            logDifference env op args (Right legacy) split
            markBroken env op "a write failed on the split side"
    return legacy

-- | The action's own exception inside the split savepoint, so that it can be
-- told apart from a failure of the savepoint.
newtype ActionThrew = ActionThrew SomeException
    deriving Show

instance Exception ActionThrew

-- | Legacy around split. The action runs once, inside both savepoints. If
-- the split savepoint fails before the action starts, or after it returned
-- or threw, the split side is broken and the legacy batch goes on with the
-- action's own outcome: its result, or its exception.
shadowNest :: ShadowEnv -> Text -> (PactStore -> (forall b. IO b -> IO b)) -> (forall a. IO a -> IO a)
shadowNest env op with act = with (_shLegacy env) $ do
    broken <- readIORef (_shBroken env)
    if broken
    then act
    else do
        state <- newIORef NotStarted
        r <- trySync $ with (_shSplit env) $ do
            writeIORef state Started
            try act >>= \case
                Right a -> do
                    writeIORef state (Finished a)
                    return a
                Left x
                    | isAsync x -> throwIO x
                    | otherwise -> do
                        writeIORef state (Threw x)
                        throwIO (ActionThrew x)
        case r of
            Right a -> return a
            Left e -> readIORef state >>= \case
                Threw x -> do
                    case fromException e of
                        Just (ActionThrew _) -> return ()
                        Nothing -> savepointFailed e
                    throwIO x
                Started -> throwIO e
                NotStarted -> do
                    savepointFailed e
                    act
                Finished a -> do
                    savepointFailed e
                    return a
  where
    savepointFailed e = do
        logDifference env op [] (Right ()) (Left e)
        markBroken env op "the split savepoint failed"

data NestState a = NotStarted | Started | Finished a | Threw SomeException

-- -------------------------------------------------------------------------- --
-- Logging

-- | Logs one difference. Rendering and logging run in a guard: when they
-- fail, a record naming the failure is logged instead, and when that fails
-- too, nothing is logged. Only asynchronous exceptions leave.
logDifference :: Show a => ShadowEnv -> Text -> [Text] -> Either SomeException a -> Either SomeException a -> IO ()
logDifference env op args legacy split = trySync full >>= \case
    Right () -> return ()
    Left e -> void $ trySync $ do
        msg <- safeText (displayException e)
        emitDifference env op $ object
            [ "ShadowDifference" .= object
                [ "chain" .= chainIdToText (_shChain env)
                , "op" .= op
                , "args" .= ([] :: [Text])
                , "legacy" .= ("not rendered" :: Text)
                , "split" .= ("not rendered" :: Text)
                , "renderFailure" .= msg
                ]
            ]
  where
    full = do
        as <- mapM (evaluate . force) args
        l <- renderOutcome legacy
        s <- renderOutcome split
        emitDifference env op $ object
            [ "ShadowDifference" .= object
                [ "chain" .= chainIdToText (_shChain env)
                , "op" .= op
                , "args" .= as
                , "legacy" .= l
                , "split" .= s
                ]
            ]

-- | Counts a difference and logs its record, or the rate limit's note or
-- summary instead. The record's text is forced first: if that throws,
-- nothing is counted.
emitDifference :: ShadowEnv -> Text -> Value -> IO ()
emitDifference env op record = do
    t <- jsonText record
    n <- atomicModifyIORef' (_shDifferences env) (\x -> (x + 1, x + 1))
    now <- _shClock env
    let ShadowLogState ref = _shLogState env
    decision <- atomicModifyIORef' ref $ \m ->
        let o = HM.lookupDefault (OpLog 0 0 0 now) op m
            (o', d) = rateLimit now o
        in (HM.insert op o' m, d)
    case decision of
        LogFull -> logFunctionText (_shLogger env) Warn t
        StartSuppressing -> logJson env Warn $ object
            [ "ShadowDifferencesSuppressed" .= object
                [ "chain" .= chainIdToText (_shChain env)
                , "op" .= op
                , "logged" .= shadowFullDifferences
                , "summarySeconds" .= shadowSummarySeconds
                , "message" .= ("further differences of this op are counted and summarised" :: Text)
                ]
            ]
        Summary suppressed total -> logJson env Warn $ object
            [ "ShadowDifferenceSummary" .= object
                [ "chain" .= chainIdToText (_shChain env)
                , "op" .= op
                , "suppressed" .= suppressed
                , "suppressedTotal" .= total
                , "differences" .= n
                ]
            ]
        Suppress -> return ()

data RateDecision = LogFull | StartSuppressing | Summary !Word64 !Word64 | Suppress

rateLimit :: Double -> OpLog -> (OpLog, RateDecision)
rateLimit now o
    | _olLogged o < shadowFullDifferences = (o { _olLogged = _olLogged o + 1 }, LogFull)
    | _olSuppressed o == 0 = (OpLog (_olLogged o) 1 1 now, StartSuppressing)
    | now - _olLastSummary o >= shadowSummarySeconds =
        (OpLog (_olLogged o) total 0 now, Summary pending total)
    | otherwise = (OpLog (_olLogged o) total pending (_olLastSummary o), Suppress)
  where
    total = _olSuppressed o + 1
    pending = _olPending o + 1

markBroken :: ShadowEnv -> Text -> Text -> IO ()
markBroken env op cause = do
    was <- atomicModifyIORef' (_shBroken env) (\b -> (True, b))
    unless was $ void $ trySync $ logJson env Error $ object
        [ "ShadowBroken" .= object
            [ "chain" .= chainIdToText (_shChain env)
            , "op" .= op
            , "message" .= (cause <> "; the chain runs on the legacy store alone from now on")
            ]
        ]

-- | The record as strict 'Text', forced, so that no thunk reaches the
-- logger.
jsonText :: Value -> IO Text
jsonText v = evaluate (force (T.decodeUtf8 (BL.toStrict (A.encode v))))

logJson :: ShadowEnv -> LogLevel -> Value -> IO ()
logJson env level v = jsonText v >>= logFunctionText (_shLogger env) level

-- | The text of a string, or a placeholder when rendering it throws.
safeText :: String -> IO Text
safeText s = trySync (evaluate (force (T.pack s))) >>= \case
    Right t -> return t
    Left _ -> return "<unrenderable>"

-- | 'renderShadowValue' of an outcome; a placeholder when rendering throws
-- (a partial 'show', or an exception whose text throws).
renderOutcome :: Show a => Either SomeException a -> IO Text
renderOutcome o = trySync (evaluate (force (renderShadowValue (outcomeBytes o)))) >>= \case
    Right t -> return t
    Left _ -> return "<unrenderable>"

outcomeBytes :: Show a => Either SomeException a -> ByteString
outcomeBytes = T.encodeUtf8 . T.pack . \case
    Left e -> "exception: " <> displayException e
    Right a -> show a

-- | A value for the log: hex of at most its first 512 bytes (@...@ when
-- truncated), then @ sha256:@ and the SHA-256 of the whole value in hex.
renderShadowValue :: ByteString -> Text
renderShadowValue b =
    hex (B.take 512 b)
    <> (if B.length b > 512 then "..." else "")
    <> " sha256:" <> T.pack (show (hashWith SHA256 b))

-- -------------------------------------------------------------------------- --
-- Arguments

hex :: ByteString -> Text
hex = T.decodeUtf8 . B16.encode

-- | Bytes of unbounded length (row keys, hash lists): plain hex up to 512
-- bytes, 'renderShadowValue' beyond.
bytesArg :: ByteString -> Text
bytesArg b
    | B.length b <= 512 = hex b
    | otherwise = renderShadowValue b

txArg :: TxId -> Text
txArg (Pact4.TxId w) = sshow w

blockArgs :: (BlockHeight, BlockHash) -> [Text]
blockArgs (h, hash) = [sshow h, sshow hash]

parentArg :: Maybe ParentHeader -> Text
parentArg Nothing = "genesis"
parentArg (Just (ParentHeader h)) = T.unwords
    [sshow (view blockHeight h), sshow (view blockHash h)]

keysArg :: KeysQuery -> Text
keysArg (KeysAsStored Nothing) = "asStored none"
keysArg (KeysAsStored (Just b)) = "asStored " <> txArg b
keysArg (KeysOrderedByKey b) = "orderedByKey " <> txArg b

-- | Within one txid, rows of a @SYS:*@ table in key order (see the module
-- header). The order of the txids is kept, so a split result with txids
-- out of order still differs.
normaliseSysRange
    :: Text
    -> Either e [(TxId, ByteString, ByteString)]
    -> Either e [(TxId, ByteString, ByteString)]
normaliseSysRange t
    | isSysTable (T.toUpper (T.take 4 t) <> T.drop 4 t) =
        fmap (concatMap (sortOn (\(_, k, _) -> k)) . groupBy ((==) `on` (\(x, _, _) -> x)))
    | otherwise = id
