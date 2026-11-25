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
-- followed by the SHA-256 of the full value ('renderShadowValue').
--
module Chainweb.Pact.Backend.Store.Shadow
( ShadowEnv(..)
, shadowPactStore
, renderShadowValue
) where

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
import Data.IORef
import Data.List (sortOn)
import Data.Ord (Down(..))
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
        -- ^ the differences logged so far
    , _shBroken :: !(IORef Bool)
        -- ^ set by the first failed write on the split side; the chain then
        -- runs on the legacy store alone
    }

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
        rd "doLookupSuccessful" (sshow h : (hex . SB.fromShort <$> V.toList hs)) id
            (\p -> _psDoLookupSuccessful p h hs)
    , _psRewindDbTo = \ph ->
        wr "rewindDbTo" [parentArg ph] (\p -> _psRewindDbTo p ph)
    , _psBackupTo = \_ -> throwIO (BackupRefused HistoryShadow)
    , _psInitSchema = wr "initSchema" [] _psInitSchema

    , _psReadNewest = \t k mb ->
        rd "readNewest" [t, hex k, maybe "none" txArg mb] id (\p -> _psReadNewest p t k mb)
    , _psRowsInTxIdRange = \t from to ->
        rd "rowsInTxIdRange" [t, txArg from, txArg to] (normaliseSysRange t)
            (\p -> _psRowsInTxIdRange p t from to)
    , _psReadPrev = \t k b ->
        rd "readPrev" [t, hex k, txArg b] id (\p -> _psReadPrev p t k b)
    , _psReadHistorical = \t k b ->
        rd "readHistorical" [t, hex k, txArg b] id (\p -> _psReadHistorical p t k b)
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
    rd :: (Eq a, Show a) => Text -> [Text] -> (a -> a) -> (PactStore -> IO a) -> IO a
    rd = shadowRead env

    wr :: (Eq a, Show a) => Text -> [Text] -> (PactStore -> IO a) -> IO a
    wr = shadowWrite env

    nest :: Text -> (PactStore -> (forall b. IO b -> IO b)) -> (forall a. IO a -> IO a)
    nest = shadowNest env

-- -------------------------------------------------------------------------- --
-- Running both sides

-- | 'try' that does not catch asynchronous exceptions.
trySync :: IO a -> IO (Either SomeException a)
trySync act = try act >>= \case
    Left e | Just (_ :: SomeAsyncException) <- fromException e -> throwIO e
    r -> return r

sameOutcome :: Eq a => Either SomeException a -> Either SomeException a -> Bool
sameOutcome (Right a) (Right b) = a == b
sameOutcome (Left a) (Left b) = displayException a == displayException b
sameOutcome _ _ = False

shadowRead
    :: (Eq a, Show a)
    => ShadowEnv -> Text -> [Text] -> (a -> a) -> (PactStore -> IO a) -> IO a
shadowRead env op args norm f = do
    legacy <- trySync (f (_shLegacy env))
    broken <- readIORef (_shBroken env)
    unless broken $ do
        split <- trySync (f (_shSplit env))
        unless (sameOutcome (norm <$> legacy) (norm <$> split)) $
            logDifference env op args legacy split
    either throwIO return legacy

shadowWrite :: (Eq a, Show a) => ShadowEnv -> Text -> [Text] -> (PactStore -> IO a) -> IO a
shadowWrite env op args f = do
    legacy <- f (_shLegacy env)
    broken <- readIORef (_shBroken env)
    unless broken $ do
        split <- trySync (f (_shSplit env))
        unless (sameOutcome (Right legacy) split) $ do
            logDifference env op args (Right legacy) split
            markBroken env op
    return legacy

-- | Legacy around split. The action runs once, inside both savepoints. If
-- the split savepoint fails before the action starts, or after it returned,
-- the split side is broken and the legacy batch goes on; an exception of the
-- action itself propagates through both.
shadowNest :: ShadowEnv -> Text -> (PactStore -> (forall b. IO b -> IO b)) -> (forall a. IO a -> IO a)
shadowNest env op with act = with (_shLegacy env) $ do
    broken <- readIORef (_shBroken env)
    if broken
    then act
    else do
        state <- newIORef NotStarted
        r <- trySync $ with (_shSplit env) $ do
            writeIORef state Started
            a <- act
            writeIORef state (Finished a)
            return a
        case r of
            Right a -> return a
            Left e -> readIORef state >>= \case
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
        markBroken env op

data NestState a = NotStarted | Started | Finished a

-- -------------------------------------------------------------------------- --
-- Logging

logDifference :: Show a => ShadowEnv -> Text -> [Text] -> Either SomeException a -> Either SomeException a -> IO ()
logDifference env op args legacy split = do
    atomicModifyIORef' (_shDifferences env) (\n -> (n + 1, ()))
    logJson env Warn $ object
        [ "ShadowDifference" .= object
            [ "chain" .= chainIdToText (_shChain env)
            , "op" .= op
            , "args" .= args
            , "legacy" .= renderShadowValue (outcomeBytes legacy)
            , "split" .= renderShadowValue (outcomeBytes split)
            ]
        ]

markBroken :: ShadowEnv -> Text -> IO ()
markBroken env op = do
    was <- atomicModifyIORef' (_shBroken env) (\b -> (True, b))
    unless was $ logJson env Error $ object
        [ "ShadowBroken" .= object
            [ "chain" .= chainIdToText (_shChain env)
            , "op" .= op
            , "message" .= ("a write failed on the split side; the chain runs on the legacy store alone from now on" :: Text)
            ]
        ]

logJson :: ShadowEnv -> LogLevel -> Value -> IO ()
logJson env level v = logFunctionText (_shLogger env) level
    (T.decodeUtf8 (BL.toStrict (A.encode v)))

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
-- header).
normaliseSysRange
    :: Text
    -> Either e [(TxId, ByteString, ByteString)]
    -> Either e [(TxId, ByteString, ByteString)]
normaliseSysRange t
    | isSysTable (T.toUpper (T.take 4 t) <> T.drop 4 t) = fmap (sortOn (\(x, k, _) -> (Down x, k)))
    | otherwise = id
