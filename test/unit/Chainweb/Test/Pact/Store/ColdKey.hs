{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module: Chainweb.Test.Pact.Store.ColdKey
--
-- Tests for the cold-store key encoding.
--
module Chainweb.Test.Pact.Store.ColdKey
( tests
) where

import Control.Exception

import qualified Data.ByteString as B
import Data.Word

import Test.QuickCheck
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck (testProperty)

-- internal modules

import Chainweb.ChainId
import Chainweb.Pact.Backend.Store.Cold.Key

-- -------------------------------------------------------------------------- --
-- Generators

-- | Keys with a heavy share of 0x00 and 0xFF bytes, including the empty key and
-- long keys.
newtype Key = Key B.ByteString
    deriving (Show, Eq, Ord)

instance Arbitrary Key where
    arbitrary = Key . B.pack <$> oneof
        [ pure []
        , listOf byte
        , resize 600 (listOf byte)
        , listOf (elements [0x00, 0xFF])
        ]
      where
        byte = frequency [(3, pure 0x00), (3, pure 0xFF), (4, arbitrary)]
    shrink (Key k) = Key . B.pack <$> shrink (B.unpack k)

-- | Two keys that are often related: equal, one a prefix of the other, or
-- differing in one byte. Independent keys almost never share a prefix.
data KeyPair = KeyPair B.ByteString B.ByteString
    deriving Show

instance Arbitrary KeyPair where
    arbitrary = do
        Key a <- arbitrary
        Key s <- arbitrary
        b <- oneof
            [ pure a
            , pure (a <> s)
            , B.take <$> choose (0, B.length a) <*> pure a
            , (\i w -> B.take i a <> B.singleton w <> B.drop (i + 1) a)
                <$> choose (0, B.length a) <*> elements [0x00, 0x01, 0xFE, 0xFF]
            , (\(Key k) -> k) <$> arbitrary
            ]
        return (KeyPair a b)

newtype Cid = Cid ChainId
    deriving (Show, Eq)

instance Arbitrary Cid where
    arbitrary = Cid . unsafeChainId <$> choose (0, 255)

instance Arbitrary TableId where
    arbitrary = TableId <$> arbitrary

instance Arbitrary KeyId where
    arbitrary = KeyId <$> oneof [arbitrary, choose (0, 300), pure maxBound]

-- -------------------------------------------------------------------------- --
-- Properties

-- | ByteString order is SQLite's BINARY collation.
prop_rowKeyOrder :: Cid -> TableId -> Key -> Key -> Word64 -> Word64 -> Property
prop_rowKeyOrder (Cid c) t (Key a) (Key b) x y =
    compare (rowKey c t a x) (rowKey c t b y) === compare (a, x) (b, y)

prop_prefixOnlyOwnKey :: Cid -> TableId -> Key -> Key -> Word64 -> Property
prop_prefixOnlyOwnKey (Cid c) t (Key a) (Key b) x =
    (rowKeyPrefix c t a `B.isPrefixOf` rowKey c t b x) === (a == b)

prop_relatedKeys :: Cid -> TableId -> KeyPair -> Word64 -> Word64 -> Property
prop_relatedKeys (Cid c) t (KeyPair a b) x y = conjoin
    [ compare (rowKey c t a x) (rowKey c t b y) === compare (a, x) (b, y)
    , (rowKeyPrefix c t a `B.isPrefixOf` rowKey c t b y) === (a == b)
    , compare (escapeKey a) (escapeKey b) === compare a b
    ]

prop_prefixOfRowKey :: Cid -> TableId -> Key -> Word64 -> Property
prop_prefixOfRowKey (Cid c) t (Key k) x =
    rowKey c t k x === rowKeyPrefix c t k <> B.pack (be64 x)
  where
    be64 w = [fromIntegral (w `div` (256 ^ i)) | i <- [7, 6 .. 0 :: Int]]

prop_escapeOrder :: Key -> Key -> Property
prop_escapeOrder (Key a) (Key b) = compare (escapeKey a) (escapeKey b) === compare a b

prop_escapeRoundTrip :: Key -> Key -> Property
prop_escapeRoundTrip (Key k) (Key rest) =
    unescapeKey (escapeKey k <> rest) === Just (k, rest)

prop_rowKeyRoundTrip :: Cid -> TableId -> Key -> Word64 -> Property
prop_rowKeyRoundTrip (Cid c) t (Key k) x =
    decodeRowKey (rowKey c t k x) === Just (chainByte c, t, k, x)

prop_keyIdListRoundTrip :: [KeyId] -> Property
prop_keyIdListRoundTrip ids = decodeKeyIdList (encodeKeyIdList ids) === Just ids

prop_txidKeyOrder :: Cid -> TableId -> Word64 -> Word64 -> Property
prop_txidKeyOrder (Cid c) t x y = compare (txidKey c t x) (txidKey c t y) === compare x y

prop_idKeyOrder :: Cid -> TableId -> KeyId -> KeyId -> Property
prop_idKeyOrder (Cid c) t x y = compare (idKey c t x) (idKey c t y) === compare x y

-- | The keyspaces never overlap: the first byte differs.
prop_keyspacesDisjoint :: Cid -> TableId -> Key -> Word64 -> KeyId -> Property
prop_keyspacesDisjoint (Cid c) t (Key k) x i =
    (B.head <$> keys) === ["RTKINMX" `B.index` n | n <- [0 .. 6]]
  where
    keys =
        [ rowKey c t k x, txidKey c t x, keyIdKey c t k, idKey c t i
        , tableIdKey c "tbl", metaKey c, txIndexKey c k ]

-- -------------------------------------------------------------------------- --
-- Vectors

unit_chainByteRange :: Assertion
unit_chainByteRange = do
    chainByte (unsafeChainId 255) @?= 255
    r <- try (evaluate (chainByte (unsafeChainId 256)))
    case r of
        Left (ErrorCall _) -> return ()
        Right b -> assertFailure ("expected an error, got " <> show b)

unit_escapeVector :: Assertion
unit_escapeVector = escapeKey "a\0b" @?= "a\0\xFF\&b\0\0"

unit_escapeEmpty :: Assertion
unit_escapeEmpty = escapeKey "" @?= "\0\0"

unit_unescapeMalformed :: Assertion
unit_unescapeMalformed = do
    unescapeKey "abc" @?= Nothing
    unescapeKey "ab\0" @?= Nothing
    unescapeKey "ab\0\x01x" @?= Nothing
    unescapeKey "ab\0\xFF" @?= Nothing

unit_rowKeyVector :: Assertion
unit_rowKeyVector =
    rowKey (unsafeChainId 3) (TableId 0x01020304) "a\0b" 0x0102030405060708
        @?= "R\x03\x01\x02\x03\x04\&a\0\xFF\&b\0\0\x01\x02\x03\x04\x05\x06\x07\x08"

unit_decodeRowKeyMalformed :: Assertion
unit_decodeRowKeyMalformed = do
    decodeRowKey "" @?= Nothing
    decodeRowKey "T\x00\x00\x00\x00\x00\0\0\x00\x00\x00\x00\x00\x00\x00\x00" @?= Nothing
    -- missing a byte of txid
    decodeRowKey "R\x00\x00\x00\x00\x01k\0\0\x00\x00\x00\x00\x00\x00\x00" @?= Nothing
    -- trailing byte after txid
    decodeRowKey "R\x00\x00\x00\x00\x01k\0\0\x00\x00\x00\x00\x00\x00\x00\x00\x00" @?= Nothing

unit_otherVectors :: Assertion
unit_otherVectors = do
    txidKey (unsafeChainId 2) (TableId 7) 258 @?= "T\x02\0\0\0\x07\0\0\0\0\0\0\x01\x02"
    keyIdKey (unsafeChainId 2) (TableId 7) "k\0" @?= "K\x02\0\0\0\x07k\0\xFF\0\0"
    idKey (unsafeChainId 2) (TableId 7) (KeyId 0x0A0B0C0D) @?= "I\x02\0\0\0\x07\x0A\x0B\x0C\x0D"
    tableIdKey (unsafeChainId 2) "SYS:Pacts" @?= "N\x02SYS:Pacts"
    tableIdKey (unsafeChainId 2) "caf\233" @?= "N\x02\&caf\xC3\xA9"
    metaKey (unsafeChainId 19) @?= "M\x13"
    txIndexKey (unsafeChainId 0) "\xAB\xCD" @?= "X\x00\xAB\xCD"

unit_varintVectors :: Assertion
unit_varintVectors = do
    encodeKeyIdList [] @?= ""
    encodeKeyIdList [KeyId 0, KeyId 127, KeyId 128, KeyId 300, KeyId maxBound]
        @?= "\x00\x7F\x80\x01\xAC\x02\xFF\xFF\xFF\xFF\x0F"

unit_varintMalformed :: Assertion
unit_varintMalformed = do
    -- truncated continuation
    decodeKeyIdList "\x80" @?= Nothing
    -- value above 2^32 - 1
    decodeKeyIdList "\x80\x80\x80\x80\x10" @?= Nothing
    -- overlong encoding of zero
    decodeKeyIdList "\x80\x00" @?= Nothing

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = testGroup "Cold key encoding"
    [ testProperty "rowKey order is (key, txid) order" prop_rowKeyOrder
    , testProperty "rowKeyPrefix matches only its own key" prop_prefixOnlyOwnKey
    , testProperty "order and prefix hold for related keys" prop_relatedKeys
    , testProperty "rowKey is rowKeyPrefix plus big-endian txid" prop_prefixOfRowKey
    , testProperty "escapeKey preserves order" prop_escapeOrder
    , testProperty "escape round trip with trailing bytes" prop_escapeRoundTrip
    , testProperty "rowKey round trip" prop_rowKeyRoundTrip
    , testProperty "key id list round trip" prop_keyIdListRoundTrip
    , testProperty "txidKey order" prop_txidKeyOrder
    , testProperty "idKey order" prop_idKeyOrder
    , testProperty "keyspaces are disjoint" prop_keyspacesDisjoint
    , testCase "chainByte rejects chain ids above 255" unit_chainByteRange
    , testCase "escape vector" unit_escapeVector
    , testCase "escape empty key" unit_escapeEmpty
    , testCase "unescape rejects malformed input" unit_unescapeMalformed
    , testCase "rowKey vector" unit_rowKeyVector
    , testCase "decodeRowKey rejects malformed input" unit_decodeRowKeyMalformed
    , testCase "other key vectors" unit_otherVectors
    , testCase "varint vectors" unit_varintVectors
    , testCase "varint rejects malformed input" unit_varintMalformed
    ]
