{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- |
-- Module: Chainweb.Test.Pact.Store.Shadow
--
-- Tests for shadow mode (storage redesign P3, spec 7.5): every read runs on
-- the legacy store and on the split store, differences are logged, and the
-- legacy answer is returned.
--
-- The fixture is the freezer's: the split store over the frozen and trimmed
-- SQLite file (verified frontier at height 8, end txid 90), the legacy store
-- over the untouched twin.
--
module Chainweb.Test.Pact.Store.Shadow
( tests
) where

import Control.Exception
import Control.Lens ((&), (.~))
import Control.Monad

import Data.Aeson (Value(..), decodeStrict)
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString as B
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Short as SB
import Data.IORef
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V

import qualified Database.RocksDB.Base as R
import qualified Database.SQLite3.Direct as SQ3

import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import System.LogLevel

import Test.Tasty
import Test.Tasty.HUnit

import qualified Pact.Types.Persistence as Pact4

-- internal modules

import Chainweb.BlockHeader.Internal
import Chainweb.Logger (genericLogger)
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Cold.Key (rowKey)
import Chainweb.Pact.Backend.Store.Config (PactHistoryStore(..))
import Chainweb.Pact.Backend.Store.Freezer (StepOutcome(..))
import Chainweb.Pact.Backend.Store.Layout (PactLayoutError(..))
import Chainweb.Pact.Backend.Store.Shadow
import Chainweb.Pact.Backend.Store.SQLite
import Chainweb.Pact.Backend.Store.Split
import Chainweb.Pact.Backend.Store.Types
import Chainweb.Pact.Backend.Types (SQLiteEnv)
import Chainweb.Storage.Table.RocksDB (_rocksDbHandle)
import Chainweb.Test.Pact.Store.Freezer
    ( cid, tipHeight, mkHash, block, dataTables, withFixture, Node(..)
    , withPlainNode, runToEnd, finalFrontier
    )
import Chainweb.Test.TestVersions (slowForkingCpmTestVersion)
import Chainweb.Test.Utils (petersen)
import Chainweb.Version (ChainwebVersion)

type TxId = Pact4.TxId

tests :: TestTree
tests = testGroup "Shadow store"
    [ testCase "same history, freezer drained, reads at all bounds: zero differences" sameHistoryTest
    , testCase "one corrupted cold R value: exactly one ShadowDifference naming op and args; legacy answer returned" corruptTest
    , testCase "split commit failure: one difference, broken, legacy keeps committing and answering" commitFailureTest
    , testCase "rewind refused on the split side only: one difference, broken, legacy rewound" rewindTest
    , testCase "legacy exceptions are rethrown; equal exceptions are no difference" exceptionTest
    , testCase "split savepoint failure: one difference, broken, the legacy batch commits" savepointTest
    , testCase "backup is refused" backupTest
    ]

-- -------------------------------------------------------------------------- --
-- Fixture

data Shadow = Shadow
    { _shStore :: PactStore
    , _shEnv :: ShadowEnv
    , _shLogs :: IO [Text]
    , _shNode :: Node
    , _shTwin :: SQLiteEnv
    , _shSplitCalls :: IORef Int
        -- ^ calls of the split store's reads and writes
    }

-- | The drained fixture with a shadow store over the twin (legacy) and the
-- split store; @f@ may replace operations of the split store.
withShadowWith :: (PactStore -> PactStore) -> (Shadow -> IO a) -> IO a
withShadowWith f k = withFixture $ \root twin -> withPlainNode root $ \n -> do
    outcomes <- runToEnd (_nEnv n)
    last outcomes @?= NothingToDo
    _ccVerified (_nCold n) >>= (@?= Just finalFrontier)
    counters <- newIORef emptySplitCounters
    calls <- newIORef 0
    logs <- newIORef []
    diffs <- newIORef 0
    broken <- newIORef False
    let sp = splitPactStore SplitEnv
            { _seChain = cid
            , _seHot = sqlitePactStore (_nPact n)
            , _seCold = _nCold n
            , _seCounters = counters
            }
        env = ShadowEnv
            { _shChain = cid
            , _shLegacy = sqlitePactStore twin
            , _shSplit = counting calls (f sp)
            , _shLogger = genericLogger Info (\t -> modifyIORef' logs (t :))
            , _shDifferences = diffs
            , _shBroken = broken
            }
    k Shadow
        { _shStore = shadowPactStore env
        , _shEnv = env
        , _shLogs = reverse <$> readIORef logs
        , _shNode = n
        , _shTwin = twin
        , _shSplitCalls = calls
        }

withShadow :: (Shadow -> IO a) -> IO a
withShadow = withShadowWith id

-- | Counts the calls of the reads and the writes of a store.
counting :: IORef Int -> PactStore -> PactStore
counting ref p = p
    { _psReadNewest = \t k b -> c >> _psReadNewest p t k b
    , _psReadPrev = \t k b -> c >> _psReadPrev p t k b
    , _psReadHistorical = \t k b -> c >> _psReadHistorical p t k b
    , _psDistinctKeys = \q t -> c >> _psDistinctKeys p q t
    , _psTxIdsAfter = \t x -> c >> _psTxIdsAfter p t x
    , _psRowsAtTxId = \t x -> c >> _psRowsAtTxId p t x
    , _psRowsInTxIdRange = \t a b -> c >> _psRowsInTxIdRange p t a b
    , _psGetLatestBlock = c >> _psGetLatestBlock p
    , _psCommitBlock = \b -> c >> _psCommitBlock p b
    , _psRewindDbTo = \h -> c >> _psRewindDbTo p h
    }
  where
    c = modifyIORef' ref (+ 1)

differences :: Shadow -> IO Word
differences s = fromIntegral <$> readIORef (_shDifferences (_shEnv s))

isBroken :: Shadow -> IO Bool
isBroken s = readIORef (_shBroken (_shEnv s))

-- | The logged JSON records with the given top-level name, and their bodies.
records :: Text -> Shadow -> IO [KM.KeyMap Value]
records name s = do
    ls <- _shLogs s
    return
        [ o
        | l <- ls
        , let j = T.dropWhile (/= '{') l
        , Just (Object top) <- [decodeStrict (T.encodeUtf8 j)]
        , Just (Object o) <- [KM.lookup (K.fromText name) top]
        ]

field :: Text -> KM.KeyMap Value -> Maybe Value
field k = KM.lookup (K.fromText k)

hex :: B.ByteString -> Text
hex = T.decodeUtf8 . B16.encode

-- | A header with the fixture's height and hash, for '_psRewindDbTo'.
parentAt :: Int -> ParentHeader
parentAt h = ParentHeader $ genesisBlockHeader v cid
    & blockHash .~ mkHash h 0
    & blockHeight .~ fromIntegral h

v :: ChainwebVersion
v = slowForkingCpmTestVersion petersen

missingTable :: Text
missingTable = "no_such_table"

-- | Every read of the interface, through the shadow store.
allReads :: PactStore -> SQLiteEnv -> IO ()
allReads sh twin = do
    let lg = sqlitePactStore twin
        bounds = [0 .. 131] :: [TxId]
    void $ _psGetLatestBlock sh
    void $ _psGetEarliestBlock sh
    forM_ [0 .. tipHeight + 1] $ \h -> do
        void $ _psLookupBlock sh (fromIntegral h, mkHash h 0)
        void $ _psGetBlockParent sh v cid (fromIntegral h, mkHash h 0)
        void $ _psGetEndTxId' sh "shadow" (fromIntegral h) (mkHash h 0)
        void $ _psGetEndTxId sh "shadow" (Just (parentAt h))
        void $ _psDoLookupSuccessful sh (fromIntegral h)
            (V.fromList [SB.toShort ("txhash-" <> T.encodeUtf8 (T.pack (show i))) | i <- [0 .. tipHeight]])
    void $ _psGetEndTxId sh "shadow" Nothing
    forM_ (dataTables ++ [missingTable]) $ \t -> do
        ks <- either (const []) id <$> _psDistinctKeys lg (KeysAsStored Nothing) t
        forM_ ("absent" : ks) $ \k -> do
            void $ _psReadNewest sh t k Nothing
            forM_ bounds $ \b -> do
                void $ _psReadNewest sh t k (Just b)
                void $ _psReadPrev sh t k b
                void $ _psReadHistorical sh t k b
        void $ _psDistinctKeys sh (KeysAsStored Nothing) t
        forM_ bounds $ \b -> do
            void $ _psDistinctKeys sh (KeysAsStored (Just b)) t
            void $ _psDistinctKeys sh (KeysOrderedByKey b) t
            -- Pact 4 calls these for user tables only; for SYS tables split
            -- answers from the hot store alone (as the "Split store" tests
            -- pin), and shadow would report that
            unless (isSysTable t) $ do
                void $ _psTxIdsAfter sh t b
                void $ _psRowsAtTxId sh t b
            forM_ [0, 5, 45, 89, 90, 91, 131 :: TxId] $ \to ->
                void $ _psRowsInTxIdRange sh t b to
        forM_ [0 .. tipHeight + 1] $ \h ->
            void $ _psTableExistsAt sh t (fromIntegral h)
        forM_ [False, True] $ \lower -> do
            void $ _psTableInCatalog sh lower t
            void $ _psTableInCatalog sh lower (T.toUpper t)

-- -------------------------------------------------------------------------- --
-- Tests

sameHistoryTest :: Assertion
sameHistoryTest = withShadow $ \s -> do
    allReads (_shStore s) (_shTwin s)
    ds <- records "ShadowDifference" s
    assertEqual ("differences: " <> show ds) 0 (length ds)
    differences s >>= (@?= 0)
    isBroken s >>= (@?= False)
    -- the split store was asked, and it read the cold store
    readIORef (_shSplitCalls s) >>= assertBool "split store not called" . (> 0)

corruptTest :: Assertion
corruptTest = withShadow $ \s -> do
    -- alice@1 (height 0) is below F and not the anchor: only cold has it
    let cc = _nCold (_shNode s)
        db = _rocksDbHandle (coldStoreRocksDb (_nStore (_shNode s)))
    Just tid <- _ccTableId cc "coin_table"
    R.put db R.defaultWriteOptions (rowKey cid tid "alice" 1) "{\"corrupt\":1}"

    r <- _psReadNewest (_shStore s) "coin_table" "alice" (Just 2)
    r @?= Right (Just "{\"a1\":0}")
    differences s >>= (@?= 1)
    isBroken s >>= (@?= False)
    [d] <- records "ShadowDifference" s
    field "chain" d @?= Just (String "0")
    field "op" d @?= Just (String "readNewest")
    field "args" d @?= Just (Array (V.fromList [String "coin_table", String (hex "alice"), String "2"]))
    let valueOf k = case field k d of
            Just (String t) -> t
            other -> error ("no " <> T.unpack k <> ": " <> show other)
        shown = T.encodeUtf8 . T.pack . show
        legacyBytes = shown (Right (Just ("{\"a1\":0}" :: B.ByteString)) :: Either SQ3.Error (Maybe B.ByteString))
        splitBytes = shown (Right (Just ("{\"corrupt\":1}" :: B.ByteString)) :: Either SQ3.Error (Maybe B.ByteString))
    valueOf "legacy" @?= renderShadowValue legacyBytes
    valueOf "split" @?= renderShadowValue splitBytes
    assertBool "hex then the SHA-256" $ (hex legacyBytes <> " sha256:") `T.isPrefixOf` valueOf "legacy"

    -- the other reads of the history still agree
    allReads (_shStore s) (_shTwin s)
    n <- differences s
    assertBool ("only reads of alice@1 differ, got " <> show n) (n >= 1)
    ds <- records "ShadowDifference" s
    forM_ ds $ \x -> case field "args" x of
        Just (Array as) -> assertBool ("a difference not about alice: " <> show x)
            (String (hex "alice") `elem` V.toList as || field "op" x `elem` (Just . String <$> ["rowsInTxIdRange", "rowsAtTxId"]))
        _ -> assertFailure (show x)

commitFailureTest :: Assertion
commitFailureTest = withShadowWith failCommit $ \s -> do
    let sh = _shStore s
        lg = sqlitePactStore (_shTwin s)
        b13 = block 13
        b14 = block 14
    _psWithBatch sh (_psCommitBlock sh b13) >>= (@?= Right ())
    differences s >>= (@?= 1)
    isBroken s >>= (@?= True)
    [d] <- records "ShadowDifference" s
    field "op" d @?= Just (String "commitBlock")
    length <$> records "ShadowBroken" s >>= (@?= 1)
    calls0 <- readIORef (_shSplitCalls s)

    -- legacy only from now on: no split call, no new difference, one log
    _psWithBatch sh (_psCommitBlock sh b14) >>= (@?= Right ())
    _psGetLatestBlock lg >>= (@?= Just (14, mkHash 14 0))
    _psGetLatestBlock sh >>= (@?= Just (14, mkHash 14 0))
    got <- _psReadNewest sh "coin_table" "alice" Nothing
    want <- _psReadNewest lg "coin_table" "alice" Nothing
    got @?= want
    got @?= Right (Just "{\"a3\":14}")
    allReads sh (_shTwin s)
    readIORef (_shSplitCalls s) >>= (@?= calls0)
    differences s >>= (@?= 1)
    length <$> records "ShadowBroken" s >>= (@?= 1)
  where
    failCommit p = p
        { _psCommitBlock = \_ -> return (Left (CommitError CommitBlockHistory CommitStep SQ3.ErrorIO)) }

rewindTest :: Assertion
rewindTest = withShadow $ \s -> do
    let sh = _shStore s
        lg = sqlitePactStore (_shTwin s)
        hot = sqlitePactStore (_nPact (_shNode s))
    -- height 5 ends at txid 60, below F = 90
    r <- _psWithBatch sh (_psRewindDbTo sh (Just (parentAt 5)))
    r @?= 60
    differences s >>= (@?= 1)
    isBroken s >>= (@?= True)
    [d] <- records "ShadowDifference" s
    field "op" d @?= Just (String "rewindDbTo")
    case field "split" d of
        Just (String t) -> assertBool (T.unpack t) (hex "exception: Refusing to rewind" `T.isPrefixOf` t)
        other -> assertFailure (show other)
    _psGetLatestBlock lg >>= (@?= Just (5, mkHash 5 0))
    _psGetLatestBlock sh >>= (@?= Just (5, mkHash 5 0))
    -- the split side was not rewound
    _psGetLatestBlock hot >>= (@?= Just (fromIntegral tipHeight, mkHash tipHeight 0))
    length <$> records "ShadowBroken" s >>= (@?= 1)
    differences s >>= (@?= 1)

exceptionTest :: Assertion
exceptionTest = withShadow $ \s -> do
    -- an unknown parent: both stores throw the same exception, and the
    -- legacy one reaches the caller
    let unknown = parentAt 40
    try @SomeException (_psRewindDbTo (_shStore s) (Just unknown)) >>= \case
        Left e -> assertBool (displayException e) (not (null (displayException e)))
        Right x -> assertFailure ("expected an exception, got " <> show x)
    _psGetLatestBlock (sqlitePactStore (_shTwin s)) >>= (@?= Just (fromIntegral tipHeight, mkHash tipHeight 0))
    differences s >>= (@?= 0)
    isBroken s >>= (@?= False)

savepointTest :: Assertion
savepointTest = withShadowWith failBatch $ \s -> do
    let sh = _shStore s
    _psWithBatch sh (_psCommitBlock sh (block 13)) >>= (@?= Right ())
    differences s >>= (@?= 1)
    isBroken s >>= (@?= True)
    [d] <- records "ShadowDifference" s
    field "op" d @?= Just (String "withBatch")
    _psGetLatestBlock (sqlitePactStore (_shTwin s)) >>= (@?= Just (13, mkHash 13 0))
    _psGetLatestBlock (sqlitePactStore (_nPact (_shNode s))) >>= (@?= Just (fromIntegral tipHeight, mkHash tipHeight 0))
  where
    failBatch p = p { _psWithBatch = \_ -> throwIO (userError "savepoint failed") }

backupTest :: Assertion
backupTest = withShadow $ \s -> withSystemTempDirectory "shadow-backup" $ \tmp ->
    try (_psBackupTo (_shStore s) (tmp </> "backup.sqlite")) >>= \case
        Left e -> e @?= BackupRefused HistoryShadow
        Right () -> assertFailure "expected BackupRefused"
