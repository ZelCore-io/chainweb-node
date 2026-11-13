{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.StorageTools.RangeDb
--
-- Range databases: small RocksDB databases with a slice of a node's block
-- headers, payloads and cuts, copied from the read-only service API of a
-- node. Everything is written through the code paths of a node (the header
-- DB insert, 'addNewPayload', and the cut hashes table), so the values in
-- the range database are byte for byte what a node would have written.
--
-- Also the tools to compare, inspect and corrupt such databases.
--
module Chainweb.StorageTools.RangeDb
( -- * Writing options
  RangeDbWriteOpts(..)
, defaultRangeDbWriteOpts
, rangeDbWriteOptsParser
, rangeDbPayloadDbConfig
, withRangeDbWriting

  -- * Building
, RangeSpec(..)
, RangeSummary(..)
, buildRangeDb
, buildRangeDbWith
, canonicalTips
, sampleRangeDb
, sampleTargets
, contiguousRangeDb
, probeMaxHeight

  -- * Inspecting
, rangeDbInfo

  -- * Comparing
, DiffSummary(..)
, NamespaceDiff(..)
, diffRangeDbs
, diffIsEmpty
, renderDiff
, corruptEntry
, readNamespace

  -- * Namespaces
, payloadNamespaces
, legacyPayloadNamespaces
, metadataNamespaces
, headerNamespaces
, headerNamespacesOf
, allNamespaces
, rawTable
) where

import Control.Exception (throwIO)
import Control.Lens (view)
import Control.Monad

import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as B8
import Data.Bits (xor)
import Data.Containers.ListUtils (nubOrd)
import Data.Foldable (toList)
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import Data.IORef
import Data.List (sortOn)
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Word

import Options.Applicative

import System.Directory (doesFileExist)
import System.FilePath ((</>))
import System.LogLevel
import System.Random

-- internal modules

import Chainweb.BlockHeader
import Chainweb.BlockHeaderDB.Internal (unsafeInsertBlockHeaderDb)
import Chainweb.BlockHeaderDB.RestAPI.Client (branchHeadersClient)
import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Chainweb.Configuration
    (PayloadCompression(..), boolValueReader, payloadCompressionReader,
    payloadCompressionToText, payloadCompressionTuning)
import Chainweb.Cut (unsafeMkCut)
import Chainweb.Cut.CutHashes
import Chainweb.CutDB (cutHashesTable)
import Chainweb.CutDB.RestAPI.Client (cutGetClient)
import Chainweb.Logger
import Chainweb.Payload
import Chainweb.Payload.PayloadStore
import Chainweb.Payload.PayloadStore.RocksDB
import Chainweb.Storage.Table
import Chainweb.Storage.Table.RocksDB
import Chainweb.StorageTools.Http
import Chainweb.TreeDB (BranchBounds(..), UpperBound(..))
import Chainweb.Utils (int, sshow)
import Chainweb.Utils.Paging (Page(..))
import Chainweb.Version
import Chainweb.WebBlockHeaderDB (getWebBlockHeaderDb, initWebBlockHeaderDb)

-- -------------------------------------------------------------------------- --
-- Namespaces

-- | The tables of the payload database that a node writes.
--
payloadNamespaces :: [B.ByteString]
payloadNamespaces =
    [ "BlockPayloadIndex"
    , "BlockPayload2"
    , "BlockTransactions2"
    , "BlockOutputs2"
    , "TransactionTree2"
    , "OutputTree2"
    ]

-- | Tables of the payload database that a node does not write any more but
-- that may still hold data.
--
legacyPayloadNamespaces :: [B.ByteString]
legacyPayloadNamespaces =
    [ "BlockPayload"
    , "BlockTransactions"
    , "BlockOutputs"
    , "TransactionTree"
    , "OutputTree"
    ]

-- | Tables with other data than headers and payloads.
--
metadataNamespaces :: [B.ByteString]
metadataNamespaces = ["CutHashes"]

-- | The header and rank table of each chain of a version.
--
headerNamespaces :: ChainwebVersion -> [B.ByteString]
headerNamespaces = headerNamespacesOf . sortedChains

-- | The header and rank table of each of the chains.
--
headerNamespacesOf :: [ChainId] -> [B.ByteString]
headerNamespacesOf cs = concat
    [ ["BlockHeader/" <> cidBytes c <> "/header", "BlockHeader/" <> cidBytes c <> "/rank"]
    | c <- cs
    ]
  where
    cidBytes = T.encodeUtf8 . chainIdToText

-- | The header and payload namespaces of a version, as compared by
-- 'diffRangeDbs' for a copy of a node's data.
--
allNamespaces :: ChainwebVersion -> [B.ByteString]
allNamespaces v = payloadNamespaces <> headerNamespaces v

sortedChains :: ChainwebVersion -> [ChainId]
sortedChains = sortOn (chainIdInt @Int) . toList . chainIds

-- | A table without codecs, for the table with the given namespace, for
-- instance @"BlockHeader/3/header"@. Keys and values are the stored bytes.
--
rawTable :: RocksDb -> B.ByteString -> RocksDbTable B.ByteString B.ByteString
rawTable db ns = newTable db (Codec id return) (Codec id return) (B.split 0x2f ns)

-- | All entries of a table, in key order. Keys are without the namespace.
--
readNamespace :: RocksDb -> B.ByteString -> IO [(B.ByteString, B.ByteString)]
readNamespace db ns = withTableIterator (rawTable db ns) $ \it ->
    let go acc = iterEntry it >>= \case
            Nothing -> return $ reverse acc
            Just (Entry k v) -> iterNext it >> go ((k, v) : acc)
    in go []

-- -------------------------------------------------------------------------- --
-- Writing options

-- | How a range database is written: the payload store setting and the
-- RocksDB compression of the node options @--store-merkle-trees@ and
-- @--payload-compression@.
--
data RangeDbWriteOpts = RangeDbWriteOpts
    { _rwoStoreMerkleTrees :: !Bool
    , _rwoPayloadCompression :: !PayloadCompression
    }
    deriving (Show, Eq)

-- | The defaults of the node: trees are written, Snappy.
--
defaultRangeDbWriteOpts :: RangeDbWriteOpts
defaultRangeDbWriteOpts = RangeDbWriteOpts True PayloadSnappy

-- | @--store-merkle-trees true|false@ and @--payload-compression
-- snappy|zstd-dict@, with the readers and defaults of the node options.
--
rangeDbWriteOptsParser :: Parser RangeDbWriteOpts
rangeDbWriteOptsParser = RangeDbWriteOpts
    <$> option boolValueReader
        ( long "store-merkle-trees"
        <> metavar "true|false"
        <> value (_rwoStoreMerkleTrees defaultRangeDbWriteOpts)
        <> showDefaultWith (\b -> if b then "true" else "false")
        <> help "write the Merkle trees of the payloads, as a node with this setting does"
        )
    <*> option payloadCompressionReader
        ( long "payload-compression"
        <> metavar "snappy|zstd-dict"
        <> value (_rwoPayloadCompression defaultRangeDbWriteOpts)
        <> showDefaultWith (T.unpack . payloadCompressionToText)
        <> help "compression of the files that are written, as a node with this setting does"
        )

rangeDbPayloadDbConfig :: RangeDbWriteOpts -> PayloadDbConfig
rangeDbPayloadDbConfig o = PayloadDbConfig
    { _payloadDbConfigStoreTrees = if _rwoStoreMerkleTrees o
        then StoreMerkleTrees
        else DoNotStoreMerkleTrees
    }

-- | Opens (or creates) a range database with the compression of the options,
-- and flushes the memtables to SST files before it is closed.
--
withRangeDbWriting :: RangeDbWriteOpts -> FilePath -> (RocksDb -> IO a) -> IO a
withRangeDbWriting o path act =
    withRocksDbTuned path modernDefaultOptions (payloadCompressionTuning $ _rwoPayloadCompression o) $ \db -> do
        r <- act db
        flushRocksDb db
        return r

-- -------------------------------------------------------------------------- --
-- Building

-- | A range database with all headers and payloads of all chains up to the
-- height and one cut with every chain at that height. Payloads of all chains
-- are included, so that a node that syncs only the Pact state never meets a
-- missing payload.
--
newtype RangeSpec = RangeSpec { _rsMaxHeight :: BlockHeight }
    deriving (Show, Eq)

data RangeSummary = RangeSummary
    { _rsHeaders :: !Int
    , _rsPayloads :: !Int
        -- ^ payloads requested, fetched, written, and read back
    , _rsCuts :: !Int
    , _rsSkipped :: !Int
        -- ^ sampled (chain, height) targets for which the service has no
        -- header
    , _rsRequests :: !Word64
        -- ^ requests that the limiter granted in the meantime. Includes the
        -- requests of other callers that share the limiter.
    }
    deriving (Show, Eq)

-- | Copies the headers and the payloads (with outputs) of all chains up to a
-- height, and one cut.
--
-- Headers are fetched per chain in pages and are written with the header DB
-- insert; payloads are fetched in batches of 'payloadBatchSize' and are
-- written with 'addNewPayload' through a payload store with the given
-- configuration. Each payload is read back, which fails if the
-- payload that the service returned does not hash to the key that was
-- requested.
--
-- The cut holds the canonical header at the height of each chain, that is the
-- ancestor at that height of the tip of the cut that the service reports (see
-- 'canonicalTips'). Orphan headers and their payloads, at that height and
-- below, are copied like all others.
--
buildRangeDb :: Logger l => l -> PayloadDbConfig -> ServiceApi -> RangeSpec -> RocksDb -> IO RangeSummary
buildRangeDb l cfg api = buildRangeDbWith l cfg api (runServiceApi api $ cutGetClient $ _saVersion api)

-- | 'buildRangeDb' with the action that gets the current cut of the source.
-- The cut is read once, after the headers and payloads were copied.
--
buildRangeDbWith :: Logger l => l -> PayloadDbConfig -> ServiceApi -> IO CutHashes -> RangeSpec -> RocksDb -> IO RangeSummary
buildRangeDbWith l cfg api getSrcCut (RangeSpec maxHeight) rdb = do
    p0 <- limiterPermits (_saLimiter api)
    wdb <- initWebBlockHeaderDb rdb v
    let pdb = newPayloadDbWith cfg rdb
    nHeaders <- newIORef (0 :: Int)
    nPayloads <- newIORef (0 :: Int)
    forM_ (sortedChains v) $ \cid -> do
        logg Info $ "chain " <> sshow cid <> ": copying headers and payloads up to height " <> sshow maxHeight
        bdb <- getWebBlockHeaderDb wdb cid
        pending <- newIORef []
        let flush = do
                reqs <- reverse <$> readIORef pending
                writeIORef pending []
                n <- storePayloads api pdb cid reqs
                modifyIORef' nPayloads (+ n)
        forHeaders api cid 0 maxHeight $ \hdrs -> do
            forM_ hdrs $ unsafeInsertBlockHeaderDb bdb
            modifyIORef' nHeaders (+ length hdrs)
            forM_ hdrs $ \hdr ->
                modifyIORef' pending ((view blockHeight hdr, view blockPayloadHash hdr) :)
            len <- length <$> readIORef pending
            when (len >= payloadBatchSize) flush
        flush
    srcCut <- getSrcCut
    tips <- canonicalTips api srcCut maxHeight
    forM_ tips $ \(cid, hdr) ->
        logg Info $ "chain " <> sshow cid <> ": cut at " <> sshow (view blockHash hdr)
    let cut = unsafeMkCut v (HM.fromList tips)
    casInsert (cutHashesTable rdb) (cutToCutHashes Nothing cut)
    p1 <- limiterPermits (_saLimiter api)
    hs <- readIORef nHeaders
    ps <- readIORef nPayloads
    logg Info $ "done: " <> sshow hs <> " headers, " <> sshow ps <> " payloads, 1 cut"
    return $ RangeSummary hs ps 1 0 (p1 - p0)
  where
    v = _saVersion api
    logg = logFunctionText l

-- | The canonical header at the given height of each chain: the ancestor at
-- that height of the tip of the chain in the cut. The service is asked for
-- the branch of the tip, bounded to the height, so that a fork at that height
-- cannot lead to a header that is not an ancestor of the cut. (The height
-- limit of the cut endpoint is a cut height, which is only approximately a
-- block height, and is not used.)
--
-- Fails if a chain is not in the cut, if the tip of a chain is below the
-- height, or if the service has no ancestor of the tip at the height.
--
canonicalTips :: ServiceApi -> CutHashes -> BlockHeight -> IO [(ChainId, BlockHeader)]
canonicalTips api srcCut h = forM (sortedChains v) $ \cid -> do
    let failWith :: String -> IO a
        failWith msg = throwIO $ userError $ "chain " <> show cid <> " " <> msg
    BlockHashWithHeight tipHeight tipHash <- case HM.lookup cid (_cutHashes srcCut) of
        Nothing -> failWith "is not in the cut of the service"
        Just t -> return t
    when (tipHeight < h) $
        failWith $ "has a tip at height " <> show tipHeight <> ", below " <> show h
    page <- runServiceApi api $ branchHeadersClient v cid (Just 1) Nothing
        (Just $ int h) (Just $ int h)
        (BranchBounds mempty (HS.singleton $ UpperBound tipHash))
    case _pageItems page of
        [] -> failWith $ "has no ancestor of its tip " <> show tipHash <> " at height " <> show h
        hdr : _
            | view blockHeight hdr == h -> return (cid, hdr)
            | otherwise -> failWith $ "returned a header at height " <> show (view blockHeight hdr)
                <> " for height " <> show h
  where
    v = _saVersion api

-- | The (chain, height) targets of a sample: up to @count@ distinct targets
-- drawn uniformly from all chains and from the heights between the genesis
-- height of the chain and the given height. Deterministic in the seed. If
-- there are not more targets than @count@, all of them are returned.
--
sampleTargets :: ChainwebVersion -> Word64 -> Int -> BlockHeight -> [(ChainId, BlockHeight)]
sampleTargets v seed count maxHeight
    | count <= 0 = []
    | toInteger count >= total = allSlots
    | otherwise = draw S.empty [] (mkStdGen (fromIntegral seed)) count
  where
    chains = [c | c <- sortedChains v, genesisBlockHeight v c <= maxHeight]
    nChains = length chains
    slotsOf c = int maxHeight - int (genesisBlockHeight v c) + 1 :: Integer
    total = sum $ slotsOf <$> chains
    allSlots = [(c, h) | c <- chains, h <- [genesisBlockHeight v c .. maxHeight]]

    draw :: S.Set (ChainId, BlockHeight) -> [(ChainId, BlockHeight)] -> StdGen -> Int -> [(ChainId, BlockHeight)]
    draw _ acc _ 0 = reverse acc
    draw seen acc g n =
        let (ci, g1) = uniformR (0, nChains - 1) g
            c = chains !! ci
            lo = int (genesisBlockHeight v c) :: Word64
            (h, g2) = uniformR (lo, int maxHeight) g1
            target = (c, fromIntegral h)
        in if target `S.member` seen
            then draw seen acc g2 n
            else draw (S.insert target seen) (target : acc) g2 (n - 1)

-- | The highest height with a header on the chain, found with a galloping and
-- a binary search over requests for headers at or above a height.
--
probeMaxHeight :: ServiceApi -> ChainId -> IO BlockHeight
probeMaxHeight api cid = do
    let lo0 = genesisBlockHeight (_saVersion api) cid
    ok <- hasHeaderAtOrAbove api cid lo0
    unless ok $ throwIO $ userError $ "chain " <> show cid <> " has no headers"
    (lo, hi) <- gallop lo0 1
    bisect lo hi
  where
    -- invariant: a header at or above lo, none at or above hi
    gallop lo step = do
        let h = lo + step
        ok <- hasHeaderAtOrAbove api cid h
        if ok then gallop h (step * 2) else return (lo, h)
    bisect lo hi
        | hi - lo <= 1 = return lo
        | otherwise = do
            let mid = lo + (hi - lo) `div` 2
            ok <- hasHeaderAtOrAbove api cid mid
            if ok then bisect mid hi else bisect lo mid

-- | A database with the payloads (with outputs) of a random sample of
-- (chain, height) pairs across the whole history, see 'sampleTargets'. The
-- highest height is the highest header of the chain with the lowest id. If a
-- target has several headers (a fork), the payloads of all of them are
-- included. Only the payload tables are written, no headers and no cut.
--
-- A target that has no header at the service is skipped and counted in
-- '_rsSkipped'.
--
sampleRangeDb :: Logger l => l -> PayloadDbConfig -> ServiceApi -> Word64 -> Int -> RocksDb -> IO RangeSummary
sampleRangeDb l cfg api seed count rdb = do
    p0 <- limiterPermits (_saLimiter api)
    maxHeight <- probeMaxHeight api (minimum $ sortedChains v)
    let targets = sampleTargets v seed count maxHeight
    logg Info $ "sampling " <> sshow (length targets) <> " (chain, height) targets up to height " <> sshow maxHeight
    reqs <- forM targets $ \(cid, h) -> do
        hdrs <- collectHeaders api cid h h
        when (null hdrs) $
            logg Warn $ "no header at height " <> sshow h <> " of chain " <> sshow cid
        return (cid, [(h, view blockPayloadHash hdr) | hdr <- hdrs])
    let skipped = length $ filter (null . snd) reqs
        byChain = HM.toList $ HM.fromListWith (flip (<>)) reqs
    ps <- fmap sum $ forM byChain $ \(cid, rs) -> storePayloads api pdb cid rs
    p1 <- limiterPermits (_saLimiter api)
    logg Info $ "done: " <> sshow ps <> " payloads"
    return $ RangeSummary 0 ps 0 skipped (p1 - p0)
  where
    v = _saVersion api
    pdb = newPayloadDbWith cfg rdb
    logg = logFunctionText l

-- | A database with the payloads (with outputs) of all blocks of one chain in
-- a range of heights, both ends included. Only the payload tables are
-- written, no headers and no cut.
--
contiguousRangeDb
    :: Logger l
    => l
    -> PayloadDbConfig
    -> ServiceApi
    -> ChainId
    -> BlockHeight
    -> BlockHeight
    -> RocksDb
    -> IO RangeSummary
contiguousRangeDb l cfg api cid lo hi rdb = do
    when (lo > hi) $ throwIO $ userError $ "empty range: " <> show lo <> " > " <> show hi
    when (cid `notElem` sortedChains v) $ throwIO $ userError $ "unknown chain: " <> show cid
    p0 <- limiterPermits (_saLimiter api)
    hdrs <- collectHeaders api cid lo hi
    logg Info $ "chain " <> sshow cid <> ": " <> sshow (length hdrs) <> " headers in heights "
        <> sshow lo <> " to " <> sshow hi
    ps <- storePayloads api (newPayloadDbWith cfg rdb) cid
        [(view blockHeight hdr, view blockPayloadHash hdr) | hdr <- hdrs]
    p1 <- limiterPermits (_saLimiter api)
    logg Info $ "done: " <> sshow ps <> " payloads"
    return $ RangeSummary 0 ps 0 0 (p1 - p0)
  where
    v = _saVersion api
    logg = logFunctionText l

collectHeaders :: ServiceApi -> ChainId -> BlockHeight -> BlockHeight -> IO [BlockHeader]
collectHeaders api cid lo hi = do
    ref <- newIORef []
    forHeaders api cid lo hi $ \hs -> modifyIORef' ref (reverse hs <>)
    reverse <$> readIORef ref

-- | Fetches the payloads, writes them with 'addNewPayload' and reads each of
-- them back. The read-back fails if the recomputed payload hash is not the
-- requested one. Returns the number of payloads.
--
storePayloads
    :: ServiceApi
    -> PayloadDb RocksDbTable
    -> ChainId
    -> [(BlockHeight, BlockPayloadHash)]
    -> IO Int
storePayloads api pdb cid reqs = do
    ps <- fetchPayloadsWithOutputs api cid (nubOrd reqs)
    forM_ ps $ \(h, p) -> do
        addNewPayload pdb h p
        lookupPayloadWithHeight pdb (Just h) (_payloadWithOutputsPayloadHash p) >>= \case
            Just p' | p' == p -> return ()
            _ -> throwIO $ userError $ "payload at height " <> show h <> " of chain " <> show cid
                <> " does not read back: " <> show (_payloadWithOutputsPayloadHash p)
    return $ length ps

-- -------------------------------------------------------------------------- --
-- Info

-- | A description of a range database: the highest cut, and the number of
-- entries of each header and payload table.
--
-- Opens the database read-write, for once, which applies a write ahead log
-- that a previous process left. The database must exist.
--
rangeDbInfo :: FilePath -> IO T.Text
rangeDbInfo path = do
    exists <- doesFileExist (path </> "CURRENT")
    unless exists $ ioError $ userError $ "not a RocksDB database: " <> path
    withRocksDb path modernDefaultOptions $ \db -> do
        cut <- withTableIterator (unCasify $ cutHashesTable db) $ \it ->
            iterLast it >> iterValue it
        cuts <- length <$> readNamespace db "CutHashes"
        let cutLines = case cut of
                Nothing -> ["highest cut: none"]
                Just c ->
                    [ "highest cut: height " <> sshow (_cutHashesHeight c)
                        <> ", weight " <> sshow (_cutHashesWeight c)
                        <> ", id " <> sshow (_cutHashesId c)
                    ] <>
                    [ "  chain " <> chainIdToText cid <> ": height " <> sshow (_bhwhHeight bh)
                        <> ", hash " <> sshow (_bhwhHash bh)
                    | (cid, bh) <- sortOn (chainIdInt @Int . fst) $ HM.toList $ _cutHashes c
                    ]
            nss = payloadNamespaces <> legacyPayloadNamespaces <> case cut of
                Nothing -> []
                -- the version of a decoded cut is not usable, use its chains
                Just c -> headerNamespacesOf $ sortOn (chainIdInt @Int) $ HM.keys $ _cutHashes c
        counts <- forM nss $ \ns -> (,) ns . length <$> readNamespace db ns
        return $ T.unlines $
            cutLines <>
            [ "number of cuts: " <> sshow cuts
            , "entries:"
            ] <>
            [ "  " <> T.decodeUtf8 ns <> ": " <> sshow n | (ns, n) <- counts, n > 0 || ns `elem` payloadNamespaces ]

-- -------------------------------------------------------------------------- --
-- Diff

data NamespaceDiff = NamespaceDiff
    { _ndNamespace :: !B.ByteString
    , _ndLeft :: !Int
        -- ^ number of entries on the left
    , _ndRight :: !Int
    , _ndOnlyLeft :: !Int
        -- ^ keys that only the left has
    , _ndOnlyRight :: !Int
    , _ndDiffering :: !Int
        -- ^ keys in both with different values
    , _ndKeys :: ![B.ByteString]
        -- ^ the first 20 keys with a difference, in key order
    }
    deriving (Show, Eq)

newtype DiffSummary = DiffSummary { _dsNamespaces :: [NamespaceDiff] }
    deriving (Show, Eq)

ndDifferences :: NamespaceDiff -> Int
ndDifferences n = _ndOnlyLeft n + _ndOnlyRight n + _ndDiffering n

diffIsEmpty :: DiffSummary -> Bool
diffIsEmpty = all ((== 0) . ndDifferences) . _dsNamespaces

-- | Compares the given namespaces of two databases, key by key and byte by
-- byte. The namespace prefixes of the two 'RocksDb' handles are not part of
-- the keys that are compared.
--
diffRangeDbs :: RocksDb -> RocksDb -> [B.ByteString] -> IO DiffSummary
diffRangeDbs a b nss = DiffSummary <$> mapM diffNamespace nss
  where
    diffNamespace ns =
        withTableIterator (rawTable a ns) $ \ia ->
        withTableIterator (rawTable b ns) $ \ib ->
            go ia ib (NamespaceDiff ns 0 0 0 0 0 [])
      where
        note k n
            | ndDifferences n < 20 = n { _ndKeys = _ndKeys n <> [k] }
            | otherwise = n
        go ia ib !acc = do
            ea <- iterEntry ia
            eb <- iterEntry ib
            case (ea, eb) of
                (Nothing, Nothing) -> return acc
                (Just (Entry k _), Nothing) -> do
                    iterNext ia
                    go ia ib $ (note k acc) { _ndLeft = _ndLeft acc + 1, _ndOnlyLeft = _ndOnlyLeft acc + 1 }
                (Nothing, Just (Entry k _)) -> do
                    iterNext ib
                    go ia ib $ (note k acc) { _ndRight = _ndRight acc + 1, _ndOnlyRight = _ndOnlyRight acc + 1 }
                (Just (Entry ka va), Just (Entry kb vb)) -> case compare ka kb of
                    LT -> do
                        iterNext ia
                        go ia ib $ (note ka acc) { _ndLeft = _ndLeft acc + 1, _ndOnlyLeft = _ndOnlyLeft acc + 1 }
                    GT -> do
                        iterNext ib
                        go ia ib $ (note kb acc) { _ndRight = _ndRight acc + 1, _ndOnlyRight = _ndOnlyRight acc + 1 }
                    EQ -> do
                        iterNext ia
                        iterNext ib
                        let acc' = acc { _ndLeft = _ndLeft acc + 1, _ndRight = _ndRight acc + 1 }
                        go ia ib $ if va == vb
                            then acc'
                            else (note ka acc') { _ndDiffering = _ndDiffering acc' + 1 }

-- | A text report of a diff, with keys in hex.
--
renderDiff :: DiffSummary -> T.Text
renderDiff (DiffSummary ns) = T.unlines $ concatMap render ns <>
    [ if all ((== 0) . ndDifferences) ns then "no differences" else "DIFFERENCES FOUND" ]
  where
    render n =
        [ T.decodeUtf8 (_ndNamespace n) <> ": left " <> sshow (_ndLeft n) <> ", right " <> sshow (_ndRight n)
            <> ", only left " <> sshow (_ndOnlyLeft n) <> ", only right " <> sshow (_ndOnlyRight n)
            <> ", differing " <> sshow (_ndDiffering n)
        ] <>
        [ "    key " <> T.decodeUtf8 (B16.encode k) | k <- _ndKeys n ]

-- | Flips the last byte of the value of the n-th entry (counting from 0, in
-- key order) of a namespace. For checking that a comparison detects a change.
--
corruptEntry :: RocksDb -> B.ByteString -> Int -> IO ()
corruptEntry db ns n = do
    entry <- withTableIterator tbl $ \it -> do
        replicateM_ n (iterNext it)
        iterEntry it
    case entry of
        Nothing -> ioError $ userError $
            "corruptEntry: namespace " <> B8.unpack ns <> " has no entry number " <> show n
        Just (Entry k v)
            | B.null v -> ioError $ userError "corruptEntry: empty value"
            | otherwise -> tableInsert tbl k (B.snoc (B.init v) (B.last v `xor` 0xff))
  where
    tbl = rawTable db ns
