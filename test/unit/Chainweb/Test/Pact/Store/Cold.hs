{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.Test.Pact.Store.Cold
--
-- Tests for the cold store core: meta encoding, catalog, key ids and the
-- one-batch step write.
--
module Chainweb.Test.Pact.Store.Cold
( tests
) where

import Control.Exception
import Control.Monad

import Data.Bits (shiftL, (.|.))
import qualified Data.ByteString as B
import Data.IORef
import Data.Text (Text)
import qualified Data.Text as T

import System.IO.Temp (withSystemTempDirectory)

import Test.QuickCheck
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck (testProperty)

import qualified Pact.Types.Persistence as Pact4

-- internal modules

import Chainweb.BlockHash
import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Pact.Backend.Store.Cold.Key
import Chainweb.Storage.Table.RocksDB (writeBatchSync)
import Chainweb.Test.Orphans.Internal ()

-- -------------------------------------------------------------------------- --
-- Generators

instance Arbitrary Frontier where
    arbitrary = Frontier <$> arbitrary <*> arbitrary <*> (Pact4.TxId <$> arbitrary)

instance Arbitrary ColdMeta where
    arbitrary = ColdMeta <$> arbitrary <*> arbitrary <*> (Pact4.TxId <$> arbitrary)

prop_metaRoundTrip :: ColdMeta -> Property
prop_metaRoundTrip m = decodeColdMeta (encodeColdMeta m) === Right m

-- -------------------------------------------------------------------------- --
-- Helpers

cid :: ChainId
cid = unsafeChainId 0

fr :: Word -> Word -> Frontier
fr h e = Frontier (BlockHeight (fromIntegral h)) nullBlockHash (Pact4.TxId (fromIntegral e))

row :: B.ByteString -> Word -> B.ByteString -> StepRow
row k t d = StepRow k (Pact4.TxId (fromIntegral t)) d

stp :: Frontier -> [(Text, [StepRow])] -> ColdStep
stp = ColdStep

-- | A fresh cold store directory, opened read-write with an 8 MiB cache.
withDir :: (FilePath -> IO a) -> IO a
withDir k = withSystemTempDirectory "cold-store" $ \tmp -> k (tmp <> "/pactCold")

withStore :: FilePath -> (ColdStore -> IO a) -> IO a
withStore dir = withColdStore dir ColdReadWrite 8388608

be32 :: B.ByteString -> Maybe Word
be32 b
    | B.length b == 4 = Just $ B.foldl' (\a x -> a `shiftL` 8 .|. fromIntegral x) 0 b
    | otherwise = Nothing

getTableId :: ColdStore -> Text -> IO (Maybe TableId)
getTableId cs name = fmap (TableId . fromIntegral) . (be32 =<<)
    <$> coldRawGet cs (tableIdKey cid name)

getKeyId :: ColdStore -> TableId -> B.ByteString -> IO (Maybe KeyId)
getKeyId cs t k = fmap (KeyId . fromIntegral) . (be32 =<<)
    <$> coldRawGet cs (keyIdKey cid t k)

mustTableId :: ColdStore -> Text -> IO TableId
mustTableId cs name = getTableId cs name >>= \case
    Nothing -> assertFailure ("no table id for " <> show name)
    Just t -> return t

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = testGroup "Cold store"
    [ testProperty "meta round trip" prop_metaRoundTrip
    , testCase "decodeColdMeta rejects another version byte and a truncated value" metaRejectTest
    , testCase "writeStep sets written only; publish sets verified and trimFrom; both survive a reopen" frontierTest
    , testCase "table ids are stable across reopen; Coin_Table and coin_table share an id; a non-ASCII name is not folded" tableIdTest
    , testCase "key ids: first-seen order within a step, continued in the next step and after a reopen" keyIdTest
    , testCase "T list of each txid equals its rows' key ids in rowid order; SYS tables get no T, K or I" txidListTest
    , testCase "a store written by writeStep holds no key that starts with X" noTxIndexTest
    , testCase "a write that throws leaves table ids, key ids and meta unchanged" failedWriteTest
    , testCase "table name helpers" tableNameTest
    , testCase "a step that is not after the written frontier is refused" stepOrderTest
    , testCase "publish makes a step's new tables visible before it moves the frontier" publishOrderTest
    ]

-- | Review finding M1 of Task 22: a reader that sees the new frontier must
-- also see the step's tables. The hook runs between the two updates.
publishOrderTest :: IO ()
publishOrderTest = withDir $ \dir -> withStore dir $ \cs -> do
    ref <- newIORef Nothing
    seen <- newIORef []
    let mid = readIORef ref >>= \case
            Nothing -> assertFailure "no chain"
            Just cc -> do
                v <- _ccVerified cc
                t <- _ccTableId cc "New_Table"
                rows <- _ccRowsAtTxId cc "New_Table" 1
                writeIORef seen [show v, show (t /= Nothing), show rows]
    cc <- openColdChainHooked writeBatchSync mid cs cid
    writeIORef ref (Just cc)
    _ccWriteStep cc $ stp (fr 1 2) [("New_Table", [row "k" 1 "v"])]
    _ccPublish cc
    -- between the updates: the catalog already has the table, the frontier
    -- is still the old one, so the step's rows stay invisible
    readIORef seen >>= (@?= [show (Nothing :: Maybe Frontier), show True, show (Nothing :: Maybe [(B.ByteString, B.ByteString)])])
    _ccVerified cc >>= (@?= Just (fr 1 2))
    _ccRowsAtTxId cc "New_Table" 1 >>= (@?= Just [("k", "v")])

metaRejectTest :: IO ()
metaRejectTest = do
    let m = ColdMeta (Just (fr 5 50)) (Just (fr 4 40)) (Pact4.TxId 30)
        bytes = encodeColdMeta m
    assertEqual "version byte" (Just 1) (fst <$> B.uncons bytes)
    assertBool "another version byte" $ isLeft' $ decodeColdMeta (B.cons 2 (B.drop 1 bytes))
    assertBool "truncated" $ isLeft' $ decodeColdMeta (B.take (B.length bytes - 1) bytes)
    assertBool "empty" $ isLeft' $ decodeColdMeta B.empty
    assertBool "trailing bytes" $ isLeft' $ decodeColdMeta (bytes <> "x")
    assertBool "bad frontier tag" $ isLeft' $ decodeColdMeta (B.pack [1, 7] <> B.drop 2 bytes)
  where
    isLeft' = either (const True) (const False)

frontierTest :: IO ()
frontierTest = withDir $ \dir -> do
    let rows1 = [("coin_coin-table", [row "a" 0 "a0", row "b" 9 "b9"])]
        rows2 = [("coin_coin-table", [row "a" 10 "a10"])]
        rows3 = [("coin_coin-table", [row "c" 25 "c25"])]
    withStore dir $ \cs -> do
        cc <- openColdChain cs cid
        assertEqual "empty meta" (ColdMeta Nothing Nothing 0) =<< _ccMeta cc
        _ccWriteStep cc (stp (fr 1 10) rows1)
        assertEqual "written only" (ColdMeta (Just (fr 1 10)) Nothing 0) =<< _ccMeta cc
        assertEqual "verified" Nothing =<< _ccVerified cc
        assertEqual "written" (Just (fr 1 10)) =<< _ccWritten cc
        _ccPublish cc
        assertEqual "published" (ColdMeta (Just (fr 1 10)) (Just (fr 1 10)) 0) =<< _ccMeta cc
        assertEqual "verified after publish" (Just (fr 1 10)) =<< _ccVerified cc
        _ccWriteStep cc (stp (fr 2 20) rows2)
        _ccPublish cc
        -- the first step was never trimmed: the publish keeps the trim
        -- position at 0, so no trim range is skipped
        assertEqual "second publish" (ColdMeta (Just (fr 2 20)) (Just (fr 2 20)) 0) =<< _ccMeta cc
        -- publishing without a new step changes nothing
        _ccPublish cc
        assertEqual "idempotent publish" (ColdMeta (Just (fr 2 20)) (Just (fr 2 20)) 0) =<< _ccMeta cc
        _ccTrimProgress cc 20
        assertEqual "trimmed to the verified frontier" (ColdMeta (Just (fr 2 20)) (Just (fr 2 20)) 20) =<< _ccMeta cc
        _ccWriteStep cc (stp (fr 3 30) rows3)
        stats <- coldCacheStats cs
        assertBool "cache statistics" (stats /= Nothing)
    withStore dir $ \cs -> do
        cc <- openColdChain cs cid
        assertEqual "after reopen" (ColdMeta (Just (fr 3 30)) (Just (fr 2 20)) 20) =<< _ccMeta cc
        assertEqual "verified after reopen" (Just (fr 2 20)) =<< _ccVerified cc
        assertEqual "written after reopen" (Just (fr 3 30)) =<< _ccWritten cc
        _ccPublish cc
        assertEqual "publish after reopen" (ColdMeta (Just (fr 3 30)) (Just (fr 3 30)) 20) =<< _ccMeta cc
        other <- openColdChain cs (unsafeChainId 1)
        assertEqual "another chain is empty" (ColdMeta Nothing Nothing 0) =<< _ccMeta other
    withColdStore dir ColdReadOnly 8388608 $ \cs -> do
        cc <- openColdChain cs cid
        assertEqual "read-only open" (ColdMeta (Just (fr 3 30)) (Just (fr 3 30)) 20) =<< _ccMeta cc
        r <- try @SomeException $ _ccWriteStep cc (stp (fr 4 40) [])
        assertBool "read-only write is refused" (either (const True) (const False) r)

tableIdTest :: IO ()
tableIdTest = withDir $ \dir -> do
    withStore dir $ \cs -> do
        cc <- openColdChain cs cid
        _ccWriteStep cc $ stp (fr 1 10)
            [ ("Coin_Table", [row "a" 0 "x"])
            , ("free.foo", [row "a" 1 "y"])
            ]
        assertEqual "not published yet" Nothing =<< _ccTableId cc "coin_table"
        _ccPublish cc
        assertEqual "folded N key" (Just (TableId 0)) =<< getTableId cs "coin_table"
        assertEqual "no unfolded N key" Nothing =<< getTableId cs "Coin_Table"
        assertEqual "second table" (Just (TableId 1)) =<< getTableId cs "free.foo"
        assertEqual "catalog, upper case" (Just (TableId 0)) =<< _ccTableId cc "COIN_TABLE"
        assertEqual "catalog, lower case" (Just (TableId 0)) =<< _ccTableId cc "coin_table"
    withStore dir $ \cs -> do
        cc <- openColdChain cs cid
        assertEqual "catalog after reopen" (Just (TableId 0)) =<< _ccTableId cc "Coin_Table"
        _ccWriteStep cc $ stp (fr 2 20)
            [ ("coin_table", [row "b" 10 "z"])
            , ("Ñame", [row "a" 11 "n1"])
            , ("ñame", [row "a" 12 "n2"])
            ]
        _ccPublish cc
        assertEqual "same id in another case" (Just (TableId 0)) =<< getTableId cs "coin_table"
        assertEqual "continued id" (Just (TableId 2)) =<< getTableId cs "Ñame"
        assertEqual "non-ASCII not folded" (Just (TableId 3)) =<< getTableId cs "ñame"
        assertEqual "rows of the same table" (Just "z") =<< coldRawGet cs (rowKey cid (TableId 0) "b" 10)
        assertEqual "catalog of a non-ASCII name" (Just (TableId 2)) =<< _ccTableId cc "Ñame"
        assertEqual "catalog of an unknown name" Nothing =<< _ccTableId cc "nope"

keyIdTest :: IO ()
keyIdTest = withDir $ \dir -> do
    withStore dir $ \cs -> do
        cc <- openColdChain cs cid
        _ccWriteStep cc $ stp (fr 1 10) [("t", [row "b" 0 "", row "a" 1 "", row "b" 2 ""])]
        _ccPublish cc
        t <- mustTableId cs "t"
        assertEqual "b first" (Just (KeyId 0)) =<< getKeyId cs t "b"
        assertEqual "a second" (Just (KeyId 1)) =<< getKeyId cs t "a"
        _ccWriteStep cc $ stp (fr 2 20) [("t", [row "c" 10 "", row "a" 11 ""])]
        assertEqual "continued in the next step" (Just (KeyId 2)) =<< getKeyId cs t "c"
        assertEqual "I entry" (Just "c") =<< coldRawGet cs (idKey cid t (KeyId 2))
    withStore dir $ \cs -> do
        cc <- openColdChain cs cid
        t <- mustTableId cs "t"
        _ccWriteStep cc $ stp (fr 3 30)
            [ ("u", [row "x" 20 ""])
            , ("t", [row "d" 21 "", row "b" 22 "", row "\0e" 23 ""])
            ]
        assertEqual "continued after a reopen" (Just (KeyId 3)) =<< getKeyId cs t "d"
        assertEqual "key with a zero byte" (Just (KeyId 4)) =<< getKeyId cs t "\0e"
        assertEqual "old key keeps its id" (Just (KeyId 0)) =<< getKeyId cs t "b"
        u <- mustTableId cs "u"
        assertEqual "a new table starts at 0" (Just (KeyId 0)) =<< getKeyId cs u "x"
        assertEqual "I entries" [Just "b", Just "a", Just "c", Just "d", Just "\0e"]
            =<< mapM (coldRawGet cs . idKey cid t . KeyId) [0 .. 4]

txidListTest :: IO ()
txidListTest = withDir $ \dir -> withStore dir $ \cs -> do
    cc <- openColdChain cs cid
    _ccWriteStep cc $ stp (fr 1 10)
        [ ("t", [row "z" 5 "z5", row "a" 5 "a5", row "m" 5 "m5", row "a" 6 "a6"])
        , ("SYS:Pacts", [row "p" 5 "p5", row "q" 6 "q6"])
        ]
    t <- mustTableId cs "t"
    s <- mustTableId cs "sys:pacts"
    ids <- forM ["z", "a", "m"] $ \k -> getKeyId cs t k >>= maybe (assertFailure "no key id") return
    assertEqual "T at 5" (Just (encodeKeyIdList ids)) =<< coldRawGet cs (txidKey cid t 5)
    assertEqual "T at 6" (Just (encodeKeyIdList [ids !! 1])) =<< coldRawGet cs (txidKey cid t 6)
    assertEqual "R entry" (Just "a6") =<< coldRawGet cs (rowKey cid t "a" 6)
    assertEqual "R entry of a SYS table" (Just "q6") =<< coldRawGet cs (rowKey cid s "q" 6)
    keys <- coldRawKeys cs
    let header tag (TableId i) = B.pack [tag, 0] <> B.pack [fromIntegral (i `div` 2^(24::Int)), fromIntegral (i `div` 2^(16::Int)), fromIntegral (i `div` 256), fromIntegral i]
    forM_ [0x54, 0x4B, 0x49] $ \tag ->
        assertEqual ("no " <> show tag <> " keys for SYS:Pacts") []
            (filter (header tag s `B.isPrefixOf`) keys)
    assertEqual "R keys of SYS:Pacts" 2 (length (filter (header 0x52 s `B.isPrefixOf`) keys))

noTxIndexTest :: IO ()
noTxIndexTest = withDir $ \dir -> withStore dir $ \cs -> do
    cc <- openColdChain cs cid
    forM_ [1 .. 5] $ \i -> do
        let base = (i - 1) * 10
        _ccWriteStep cc $ stp (fr i (i * 10))
            [ ("t" <> T.pack (show (i `mod` 2)), [row "k" base "v", row "X" (base + 1) "w"])
            , ("SYS:Pacts", [row "X" base "p"])
            ]
        _ccPublish cc
    keys <- coldRawKeys cs
    assertBool "store holds keys" (not (null keys))
    assertEqual "no X keys" [] (filter ("X" `B.isPrefixOf`) keys)

failedWriteTest :: IO ()
failedWriteTest = withDir $ \dir -> withStore dir $ \cs -> do
    failNext <- newIORef False
    let writer db ops = do
            f <- atomicModifyIORef' failNext (\x -> (False, x))
            when f $ throwIO (userError "injected write failure")
            writeBatchSync db ops
    cc <- openColdChainWith writer cs cid
    _ccWriteStep cc $ stp (fr 1 10) [("t", [row "a" 0 ""])]
    _ccPublish cc
    before <- _ccMeta cc
    writeIORef failNext True
    r <- try @IOException $ _ccWriteStep cc $ stp (fr 2 20)
        [ ("t", [row "b" 10 "", row "c" 11 ""])
        , ("new", [row "x" 12 ""])
        ]
    assertBool "the write throws" (either (const True) (const False) r)
    assertEqual "meta unchanged" before =<< _ccMeta cc
    assertEqual "nothing written" Nothing =<< getTableId cs "new"
    -- the same step again: ids continue as if the failed write never happened
    _ccWriteStep cc $ stp (fr 2 20)
        [ ("other", [row "y" 10 ""])
        , ("t", [row "c" 11 ""])
        ]
    t <- mustTableId cs "t"
    assertEqual "table id continues" (Just (TableId 1)) =<< getTableId cs "other"
    assertEqual "key id continues" (Just (KeyId 1)) =<< getKeyId cs t "c"
    assertEqual "written moved" (Just (fr 2 20)) =<< _ccWritten cc

tableNameTest :: IO ()
tableNameTest = do
    assertEqual "fold" "coin_coin-table.x1" (foldTableName "Coin_COIN-Table.X1")
    assertEqual "non-ASCII untouched" "Ñame" (foldTableName "Ñame")
    assertBool "SYS" (isSysTable "SYS:Pacts")
    assertBool "not SYS" (not (isSysTable "sys:pacts"))
    assertBool "modules stay in SQLite" (not (frozenTable "SYS:Modules"))
    assertBool "module sources stay in SQLite" (not (frozenTable "SYS:ModuleSources"))
    assertBool "pacts are frozen" (frozenTable "SYS:Pacts")
    assertBool "user tables are frozen" (frozenTable "coin_coin-table")
    assertEqual "frontierTxId Nothing" 0 (frontierTxId Nothing)
    assertEqual "frontierTxId" 30 (frontierTxId (Just (fr 3 30)))

stepOrderTest :: IO ()
stepOrderTest = withDir $ \dir -> withStore dir $ \cs -> do
    cc <- openColdChain cs cid
    _ccWriteStep cc $ stp (fr 2 20) [("t", [row "a" 0 ""])]
    let refused msg s = do
            r <- try @SomeException (_ccWriteStep cc s)
            assertBool msg (either (const True) (const False) r)
    refused "same height" (stp (fr 2 30) [])
    refused "row below the written frontier" (stp (fr 3 30) [("t", [row "a" 19 ""])])
    refused "row at the step's end txid" (stp (fr 3 30) [("t", [row "a" 30 ""])])
    refused "a table that stays in SQLite" (stp (fr 3 30) [("SYS:Modules", [row "a" 20 ""])])
    assertEqual "meta unchanged" (ColdMeta (Just (fr 2 20)) Nothing 0) =<< _ccMeta cc
