{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.Test.Pact.Store.ColdRead
--
-- Tests for the cold reads of 'ColdChain' against a pure model: generated
-- histories are written through '_ccWriteStep' and '_ccPublish', and every
-- read is compared with the model's answer for every key (also absent ones)
-- and every bound near a version, near the verified frontier and past the
-- end.
--
module Chainweb.Test.Pact.Store.ColdRead
( tests
) where

import Control.Monad

import qualified Data.ByteString as B
import Data.Char (toUpper)
import Data.List (nub, sort, sortOn)
import Data.Maybe
import Data.Ord (Down(..))
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word

import System.IO.Temp (withSystemTempDirectory)

import Test.QuickCheck
import Test.QuickCheck.Monadic
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck (testProperty)

import qualified Pact.Types.Persistence as Pact4

-- internal modules

import Chainweb.BlockHash
import Chainweb.BlockHeight
import Chainweb.ChainId
import Chainweb.Pact.Backend.Store.Cold
import Chainweb.Storage.Table.RocksDB (_rocksDbHandle)

-- -------------------------------------------------------------------------- --
-- Histories

cid :: ChainId
cid = unsafeChainId 0

-- | Two user tables and one system table, so a seek can run into the next
-- table's range.
tableNames :: [Text]
tableNames = ["Coin_Table", "other", "SYS:Pacts"]

-- | Row keys: the empty key, @0x00@, @0xFF@ and keys that are prefixes of
-- each other.
keyPool :: [B.ByteString]
keyPool = ["", "\0", "\xFF", "\xFF\xFF", "a", "a\0", "a\0\0", "a\xFF", "b"]

-- | The keys that are never written (besides the pool keys a history leaves
-- out).
absentKeys :: [B.ByteString]
absentKeys = ["\0\0", "a\0\xFF", "c", "\xFF\xFF\xFF"]

-- | One write: table index, row key, txid, row data. A history lists its
-- writes in rowid order.
data Write = Write
    { _wTable :: !Int
    , _wKey :: !B.ByteString
    , _wTxId :: !Word64
    , _wData :: !B.ByteString
    }
    deriving (Eq, Show)

-- | Steps (each with its end txid and its writes in rowid order), and the
-- number of steps that are published; the rest are written only.
data History = History
    { _hSteps :: ![(Word64, [Write])]
    , _hPublished :: !Int
    }
    deriving (Eq, Show)

instance Arbitrary History where
    arbitrary = do
        n <- choose (1, 14)
        gaps <- vectorOf n (choose (1, 3 :: Word64))
        let txids = drop 1 $ scanl (+) 0 gaps
            txids' = subtract 1 <$> txids
        txs <- forM txids' $ \x -> do
            ws <- forM [0 .. length tableNames - 1] $ \t -> do
                ks <- shuffle =<< sublistOf keyPool
                ks' <- take <$> choose (0, 3) <*> pure ks
                forM ks' $ \k -> Write t k x . B.pack <$> listOf arbitrary
            -- rowid order within a txid interleaves the tables
            shuffle (concat ws)
        groups <- splitGroups (zip txids' txs)
        ends <- forM (zip groups (drop 1 (fmap Just groups) ++ [Nothing])) $ \(g, next) -> do
            let lastTx = fst (last g)
            case next of
                Just ng -> choose (lastTx + 1, fst (head ng))
                Nothing -> (lastTx + 1 +) <$> choose (0, 2)
        let steps = zip ends (concatMap snd <$> groups)
        pub <- frequency [(3, pure (length steps)), (1, choose (0, length steps))]
        return $ History steps pub
      where
        splitGroups [] = return []
        splitGroups xs = do
            k <- choose (1, length xs)
            (take k xs :) <$> splitGroups (drop k xs)

    shrink _ = []

-- | The verified frontier's end txid.
frontier :: History -> Word64
frontier h = case take (_hPublished h) (_hSteps h) of
    [] -> 0
    ps -> fst (last ps)

-- | The writes below the verified frontier, in rowid order.
visible :: History -> [Write]
visible h = concatMap snd (take (_hPublished h) (_hSteps h))

allWrites :: History -> [Write]
allWrites h = concatMap snd (_hSteps h)

txidsOf :: History -> [Word64]
txidsOf h = nub (sort (_wTxId <$> allWrites h))

-- | Every bound near a version, near the frontier and past the end.
boundsOf :: History -> [Word64]
boundsOf h = nub $ sort $
    [0, 1, f, f + 1, maxBound, maxBound - 1]
    ++ [ e | (e, _) <- _hSteps h ]
    ++ (if f > 0 then [f - 1] else [])
    ++ concat [ [x, x + 1] | x <- txidsOf h ]
  where
    f = frontier h

-- -------------------------------------------------------------------------- --
-- Writing a history

frontierAt :: Int -> Word64 -> Frontier
frontierAt i e = Frontier (BlockHeight (fromIntegral i)) nullBlockHash (Pact4.TxId e)

toStep :: Int -> (Word64, [Write]) -> ColdStep
toStep i (end, ws) = ColdStep (frontierAt i end)
    [ (name, [ StepRow (_wKey w) (Pact4.TxId (_wTxId w)) (_wData w) | w <- ws, _wTable w == t ])
    | (t, name) <- zip [0 ..] tableNames
    , any ((== t) . _wTable) ws
    ]

writeHistory :: ColdChain -> History -> IO ()
writeHistory cc h = forM_ (zip [1 ..] (_hSteps h)) $ \(i, s) -> do
    _ccWriteStep cc (toStep i s)
    when (i <= _hPublished h) $ _ccPublish cc

withHistory :: History -> (ColdStore -> ColdChain -> IO a) -> IO a
withHistory h act = withSystemTempDirectory "cold-read" $ \tmp ->
    withColdStore (tmp <> "/pactCold") ColdReadWrite 8388608 $ \cs -> do
        cc <- openColdChain cs cid
        writeHistory cc h
        act cs cc

-- | Each table under its own name and under another ASCII case (SQLite
-- folds ASCII identifiers), plus a table that does not exist.
queryNames :: [(Maybe Int, Text)]
queryNames =
    [ (Just t, n') | (t, n) <- zip [0 ..] tableNames, n' <- [n, T.map toUpper n] ]
    ++ [(Nothing, "no_such_table")]

isSys :: Maybe Int -> Bool
isSys = (== Just 2)

-- -------------------------------------------------------------------------- --
-- Model

clamp :: History -> Word64 -> Word64
clamp h b = min b (frontier h)

modelReadNewest :: History -> Maybe Int -> B.ByteString -> Word64 -> Maybe (B.ByteString, B.ByteString)
modelReadNewest h t k b = case vs of
    [] -> Nothing
    _ -> Just (k, _wData (last (sortOn _wTxId vs)))
  where
    vs = [ w | w <- visible h, Just (_wTable w) == t, _wKey w == k, _wTxId w < clamp h b ]

modelDistinctKeys :: History -> Maybe Int -> Word64 -> [B.ByteString]
modelDistinctKeys h t b = nub $ sort
    [ _wKey w | w <- visible h, Just (_wTable w) == t, _wTxId w < clamp h b ]

modelTxIdsBetween :: History -> Maybe Int -> Word64 -> Word64 -> [Word64]
modelTxIdsBetween h t lo hi
    | isSys t = []
    | otherwise = nub $ sort
        [ _wTxId w | w <- visible h, Just (_wTable w) == t, _wTxId w > lo, _wTxId w < clamp h hi ]

modelRowsAtTxId :: History -> Maybe Int -> Word64 -> Maybe [(B.ByteString, B.ByteString)]
modelRowsAtTxId h t x
    | isSys t = Nothing
    | otherwise = case [ (_wKey w, _wData w) | w <- visible h, Just (_wTable w) == t, _wTxId w == x ] of
        [] -> Nothing
        rs -> Just rs

modelRowsInTxIdRange :: History -> Maybe Int -> Word64 -> Word64 -> [(Word64, B.ByteString, B.ByteString)]
modelRowsInTxIdRange h t from to = concatMap perTx (sortOn Down (nub (fst3 <$> rows)))
  where
    rows = [ (_wTxId w, _wKey w, _wData w)
           | w <- visible h, Just (_wTable w) == t, _wTxId w >= from, _wTxId w < clamp h to ]
    perTx x
        | isSys t = sortOn snd3 [ r | r <- rows, fst3 r == x ]
        | otherwise = [ r | r <- rows, fst3 r == x ]
    fst3 (a, _, _) = a
    snd3 (_, b, _) = b

-- -------------------------------------------------------------------------- --
-- Properties

checkAll :: (Eq a, Show a) => String -> [(String, a, IO a)] -> IO Property
checkAll name cases = do
    bad <- fmap catMaybes $ forM cases $ \(lbl, expected, act) -> do
        got <- act
        return $ if got == expected
            then Nothing
            else Just (lbl <> ": expected " <> show expected <> ", got " <> show got)
    return $ counterexample (name <> ":\n" <> unlines (take 5 bad)) (null bad)

historyProp :: (History -> ColdStore -> ColdChain -> IO Property) -> History -> Property
historyProp p h = monadicIO $ run (withHistory h (p h)) >>= \r -> return r

tx :: Word64 -> Pact4.TxId
tx = Pact4.TxId

prop_readNewestModel :: History -> Property
prop_readNewestModel = historyProp $ \h _ cc -> checkAll "readNewest"
    [ (show (n, k, b), modelReadNewest h t k b, _ccReadNewest cc n k (tx b))
    | (t, n) <- queryNames, k <- keyPool ++ absentKeys, b <- boundsOf h
    ]

prop_distinctKeysModel :: History -> Property
prop_distinctKeysModel = historyProp $ \h _ cc -> checkAll "distinctKeys"
    [ (show (n, b), modelDistinctKeys h t b, _ccDistinctKeys cc n (tx b))
    | (t, n) <- queryNames, b <- boundsOf h
    ]

prop_txIdsBetweenModel :: History -> Property
prop_txIdsBetweenModel = historyProp $ \h _ cc -> checkAll "txIdsBetween"
    [ (show (n, lo, hi), tx <$> modelTxIdsBetween h t lo hi, _ccTxIdsBetween cc n (tx lo) (tx hi))
    | (t, n) <- queryNames, lo <- boundsOf h, hi <- boundsOf h
    ]

prop_rowsAtTxIdModel :: History -> Property
prop_rowsAtTxIdModel = historyProp $ \h _ cc -> checkAll "rowsAtTxId"
    [ (show (n, x), modelRowsAtTxId h t x, _ccRowsAtTxId cc n (tx x))
    | (t, n) <- queryNames, x <- boundsOf h
    ]

prop_rowsInTxIdRangeModel :: History -> Property
prop_rowsInTxIdRangeModel = historyProp $ \h _ cc -> checkAll "rowsInTxIdRange"
    [ ( show (n, from, to)
      , [ (tx x, k, v) | (x, k, v) <- modelRowsInTxIdRange h t from to ]
      , _ccRowsInTxIdRange cc n (tx from) (tx to)
      )
    | (t, n) <- queryNames, from <- boundsOf h, to <- boundsOf h
    ]

-- | The read of the node equals the measuring tool's read (seek, then one
-- step back) with the bound clamped to the frontier.
prop_readNewestEqualsToolRead :: History -> Property
prop_readNewestEqualsToolRead = historyProp $ \h cs cc -> do
    let db = _rocksDbHandle (coldStoreRocksDb cs)
    cases <- fmap concat $ forM (zip [0 :: Int ..] tableNames) $ \(_, n) ->
        _ccTableId cc n >>= \case
            Nothing -> return []
            Just tid -> return
                [ ( show (n, k, b)
                  , coldReadNewest db cid tid k (clamp h b)
                  , fmap snd <$> _ccReadNewest cc n k (tx b)
                  )
                | k <- keyPool ++ absentKeys, b <- boundsOf h
                ]
    expected <- forM cases $ \(l, tool, node) -> (\v -> (l, v, node)) <$> tool
    checkAll "readNewest vs coldReadNewest" expected

-- -------------------------------------------------------------------------- --
-- Unit tests

fr :: Int -> Word64 -> Frontier
fr = frontierAt

row :: B.ByteString -> Word64 -> B.ByteString -> StepRow
row k t = StepRow k (tx t)

withChain :: (ColdStore -> ColdChain -> IO a) -> IO a
withChain act = withSystemTempDirectory "cold-read" $ \tmp ->
    withColdStore (tmp <> "/pactCold") ColdReadWrite 8388608 $ \cs ->
        openColdChain cs cid >>= act cs

-- | A store that holds only system table rows has no key after the @R@
-- keyspace, so a seek past the last row runs past the end of the database.
-- With two tables, a seek past the last key of the first table runs into the
-- second table's range.
pastEndTest :: IO ()
pastEndTest = do
    withChain $ \cs cc -> do
        _ccWriteStep cc $ ColdStep (fr 1 10)
            [ ("SYS:Pacts", [row "\xFF\xFF" 3 "p3", row "\xFF\xFF" 5 "p5", row "a" 4 "a4"]) ]
        _ccPublish cc
        keys <- coldRawKeys cs
        assertBool "the last key of the store is an R key" $ (B.take 1 <$> lastMay keys) == Just "R"
        _ccReadNewest cc "SYS:Pacts" "\xFF\xFF" (tx maxBound) >>= (@?= Just ("\xFF\xFF", "p5"))
        _ccReadNewest cc "SYS:Pacts" "\xFF\xFF\xFF" (tx maxBound) >>= (@?= Nothing)
        _ccReadNewest cc "SYS:Pacts" "\xFF\xFF" (tx 5) >>= (@?= Just ("\xFF\xFF", "p3"))
        _ccDistinctKeys cc "SYS:Pacts" (tx maxBound) >>= (@?= ["a", "\xFF\xFF"])
        _ccRowsInTxIdRange cc "SYS:Pacts" 0 (tx maxBound)
            >>= (@?= [(5, "\xFF\xFF", "p5"), (4, "a", "a4"), (3, "\xFF\xFF", "p3")])
        let db = _rocksDbHandle (coldStoreRocksDb cs)
        Just t <- _ccTableId cc "SYS:Pacts"
        coldReadNewest db cid t "\xFF\xFF" 10 >>= (@?= Just "p5")
    withChain $ \_ cc -> do
        _ccWriteStep cc $ ColdStep (fr 1 10)
            [ ("first", [row "\xFF\xFF" 3 "f3"])
            , ("second", [row "" 2 "s2", row "\0" 4 "s4"])
            ]
        _ccPublish cc
        _ccReadNewest cc "first" "\xFF\xFF\xFF" (tx maxBound) >>= (@?= Nothing)
        _ccReadNewest cc "first" "\xFF\xFF" (tx maxBound) >>= (@?= Just ("\xFF\xFF", "f3"))
        _ccReadNewest cc "second" "" (tx maxBound) >>= (@?= Just ("", "s2"))
        _ccDistinctKeys cc "first" (tx maxBound) >>= (@?= ["\xFF\xFF"])
        _ccTxIdsBetween cc "first" 0 (tx maxBound) >>= (@?= [3])
        _ccRowsInTxIdRange cc "first" 0 (tx maxBound) >>= (@?= [(3, "\xFF\xFF", "f3")])
  where
    lastMay [] = Nothing
    lastMay xs = Just (last xs)

-- | Review Focus 1: a bound equal to a version's txid excludes it, a bound one
-- above includes it; at the frontier @F@, @F - 1@ and @F + 1@ the answer is
-- clamped to @F@, and rows of a written but unpublished step are invisible.
boundTest :: IO ()
boundTest = withChain $ \_ cc -> do
    _ccWriteStep cc $ ColdStep (fr 1 10) [("Coin_Table", [row "k" 3 "v3", row "j" 9 "j9", row "k" 9 "v9"])]
    _ccPublish cc
    _ccWriteStep cc $ ColdStep (fr 2 20) [("Coin_Table", [row "k" 12 "v12", row "n" 15 "n15"])]
    let rn b = _ccReadNewest cc "Coin_Table" "k" (tx b)
    rn 3 >>= (@?= Nothing)
    rn 4 >>= (@?= Just ("k", "v3"))
    rn 9 >>= (@?= Just ("k", "v3"))
    rn 10 >>= (@?= Just ("k", "v9"))
    rn 11 >>= (@?= Just ("k", "v9"))
    rn 13 >>= (@?= Just ("k", "v9"))
    rn 0 >>= (@?= Nothing)
    _ccReadNewest cc "Coin_Table" "n" (tx maxBound) >>= (@?= Nothing)
    _ccDistinctKeys cc "Coin_Table" 3 >>= (@?= [])
    _ccDistinctKeys cc "Coin_Table" 4 >>= (@?= ["k"])
    _ccDistinctKeys cc "Coin_Table" 9 >>= (@?= ["k"])
    _ccDistinctKeys cc "Coin_Table" 10 >>= (@?= ["j", "k"])
    _ccDistinctKeys cc "Coin_Table" 16 >>= (@?= ["j", "k"])
    _ccTxIdsBetween cc "Coin_Table" 3 9 >>= (@?= [])
    _ccTxIdsBetween cc "Coin_Table" 2 10 >>= (@?= [3, 9])
    _ccTxIdsBetween cc "Coin_Table" 0 (tx maxBound) >>= (@?= [3, 9])
    _ccRowsAtTxId cc "Coin_Table" 9 >>= (@?= Just [("j", "j9"), ("k", "v9")])
    _ccRowsAtTxId cc "Coin_Table" 10 >>= (@?= Nothing)
    _ccRowsAtTxId cc "Coin_Table" 12 >>= (@?= Nothing)
    _ccRowsInTxIdRange cc "Coin_Table" 3 9 >>= (@?= [(3, "k", "v3")])
    _ccRowsInTxIdRange cc "Coin_Table" 4 10 >>= (@?= [(9, "j", "j9"), (9, "k", "v9")])
    _ccRowsInTxIdRange cc "Coin_Table" 9 11 >>= (@?= [(9, "j", "j9"), (9, "k", "v9")])
    _ccRowsInTxIdRange cc "Coin_Table" 10 (tx maxBound) >>= (@?= [])
    -- Review Focus 2: another ASCII case finds the same table
    _ccReadNewest cc "COIN_TABLE" "k" 10 >>= (@?= Just ("k", "v9"))
    _ccReadNewest cc "coin_table" "k" 10 >>= (@?= Just ("k", "v9"))
    _ccReadNewest cc "no_such_table" "k" 10 >>= (@?= Nothing)
    _ccRowsAtTxId cc "no_such_table" 9 >>= (@?= Nothing)
    -- publishing makes the second step visible
    _ccPublish cc
    rn 13 >>= (@?= Just ("k", "v12"))
    _ccReadNewest cc "Coin_Table" "n" (tx maxBound) >>= (@?= Just ("n", "n15"))

tests :: TestTree
tests = testGroup "Cold reads"
    [ testProperty "readNewest equals the model" prop_readNewestModel
    , testProperty "distinctKeys equals the model" prop_distinctKeysModel
    , testProperty "txIdsBetween equals the model" prop_txIdsBetweenModel
    , testProperty "rowsAtTxId equals the model" prop_rowsAtTxIdModel
    , testProperty "rowsInTxIdRange equals the model" prop_rowsInTxIdRangeModel
    , testCase "seek past the end of the database and past the end of a table's range" pastEndTest
    , testProperty "readNewest equals coldReadNewest (seek and step back)" prop_readNewestEqualsToolRead
    , testCase "bound equal to a version's txid excludes it; bound one above includes it" boundTest
    ]
