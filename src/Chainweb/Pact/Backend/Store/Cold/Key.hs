{-# LANGUAGE BangPatterns #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.Cold.Key
--
-- Key encoding of the cold store (storage redesign, spec 4.3). A key is a byte
-- string; RocksDB orders keys bytewise, which is also SQLite's BINARY
-- collation, so the order of the Pact row keys survives the move.
--
-- Layouts (@c@ is the chain id in one byte, @t@ a 'TableId' in four bytes,
-- big-endian):
--
-- > R c t esc(rowkey) txid(8)   row bytes
-- > T c t txid(8)               key ids of the rows with that txid
-- > K c t esc(rowkey)           key id of a row key
-- > I c t id(4)                 row key of a key id
-- > N c tablename               table id (UTF-8 table name, unescaped, last)
-- > M c                         frontiers
-- > X c hash                    transaction index
--
-- @esc(k)@ writes every @0x00@ of @k@ as @0x00 0xFF@ and ends with
-- @0x00 0x00@. The terminator sorts below every continuation, so
-- @esc a < esc b@ iff @a < b@ and no escaped key is a prefix of another.
--
-- Once data is written with these layouts they cannot change.
--
module Chainweb.Pact.Backend.Store.Cold.Key
( TableId(..)
, KeyId(..)
, chainByte
, escapeKey
, unescapeKey
, rowKey
, rowKeyPrefix
, decodeRowKey
, txidKey
, keyIdKey
, idKey
, tableIdKey
, metaKey
, txIndexKey
, encodeKeyIdList
, decodeKeyIdList
) where

import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BL
import Data.Text (Text)
import qualified Data.Text.Encoding as T
import Data.Word

-- internal modules

import Chainweb.ChainId

-- -------------------------------------------------------------------------- --
-- Types

-- | A table id, assigned at first freeze.
newtype TableId = TableId Word32
    deriving (Eq, Ord, Show)

-- | A key id: the number of a row key inside its table.
newtype KeyId = KeyId Word32
    deriving (Eq, Ord, Show)

-- -------------------------------------------------------------------------- --
-- Helpers

build :: BB.Builder -> B.ByteString
build = BL.toStrict . BB.toLazyByteString

-- | The chain id as one byte. Fails for chain ids above 255.
chainByte :: ChainId -> Word8
chainByte c
    | i > 255 = error $ "Chainweb.Pact.Backend.Store.Cold.Key.chainByte: chain id above 255: " <> show i
    | otherwise = fromIntegral i
  where
    i = chainIdInt c :: Int

-- | The @tag c t@ header shared by the table keyspaces.
tableHeader :: Char -> ChainId -> TableId -> BB.Builder
tableHeader tag c (TableId t) =
    BB.char7 tag <> BB.word8 (chainByte c) <> BB.word32BE t

-- -------------------------------------------------------------------------- --
-- Escaped row keys

-- | Write @0x00@ as @0x00 0xFF@ and end with @0x00 0x00@.
escapeKey :: B.ByteString -> B.ByteString
escapeKey = build . escapeKeyB

escapeKeyB :: B.ByteString -> BB.Builder
escapeKeyB k = go k <> BB.word8 0x00 <> BB.word8 0x00
  where
    go !s = case B.elemIndex 0x00 s of
        Nothing -> BB.byteString s
        Just i -> BB.byteString (B.take i s)
            <> BB.word8 0x00 <> BB.word8 0xFF
            <> go (B.drop (i + 1) s)

-- | Read an escaped key from the front of the input. Returns the key and the
-- bytes after the terminator. Nothing if the input is not a well-formed
-- escaped key (no terminator, or a @0x00@ followed by neither @0x00@ nor
-- @0xFF@).
unescapeKey :: B.ByteString -> Maybe (B.ByteString, B.ByteString)
unescapeKey = go []
  where
    go acc s = do
        i <- B.elemIndex 0x00 s
        let chunk = B.take i s
        case B.uncons (B.drop (i + 1) s) of
            Just (0x00, rest) -> Just (B.concat (reverse (chunk : acc)), rest)
            Just (0xFF, rest) -> go (B.singleton 0x00 : chunk : acc) rest
            _ -> Nothing

-- -------------------------------------------------------------------------- --
-- Keys

-- | @R c t esc(rowkey) txid(8)@
rowKey :: ChainId -> TableId -> B.ByteString -> Word64 -> B.ByteString
rowKey c t k txid = build $ tableHeader 'R' c t <> escapeKeyB k <> BB.word64BE txid

-- | @R c t esc(rowkey)@: the prefix of all versions of one row key, and of no
-- other row key.
rowKeyPrefix :: ChainId -> TableId -> B.ByteString -> B.ByteString
rowKeyPrefix c t k = build $ tableHeader 'R' c t <> escapeKeyB k

-- | The inverse of 'rowKey'. The chain is returned as its byte.
decodeRowKey :: B.ByteString -> Maybe (Word8, TableId, B.ByteString, Word64)
decodeRowKey s = do
    (0x52, s1) <- B.uncons s
    (c, s2) <- B.uncons s1
    let (tb, s3) = B.splitAt 4 s2
    t <- word32BE tb
    (k, s4) <- unescapeKey s3
    txid <- word64BE s4
    return (c, TableId t, k, txid)

-- | @T c t txid(8)@
txidKey :: ChainId -> TableId -> Word64 -> B.ByteString
txidKey c t txid = build $ tableHeader 'T' c t <> BB.word64BE txid

-- | @K c t esc(rowkey)@
keyIdKey :: ChainId -> TableId -> B.ByteString -> B.ByteString
keyIdKey c t k = build $ tableHeader 'K' c t <> escapeKeyB k

-- | @I c t id(4)@
idKey :: ChainId -> TableId -> KeyId -> B.ByteString
idKey c t (KeyId i) = build $ tableHeader 'I' c t <> BB.word32BE i

-- | @N c tablename@, with the name in UTF-8. The name is the last component
-- and needs no escaping.
tableIdKey :: ChainId -> Text -> B.ByteString
tableIdKey c name = build $
    BB.char7 'N' <> BB.word8 (chainByte c) <> BB.byteString (T.encodeUtf8 name)

-- | @M c@
metaKey :: ChainId -> B.ByteString
metaKey c = build $ BB.char7 'M' <> BB.word8 (chainByte c)

-- | @X c hash@
txIndexKey :: ChainId -> B.ByteString -> B.ByteString
txIndexKey c h = build $ BB.char7 'X' <> BB.word8 (chainByte c) <> BB.byteString h

-- -------------------------------------------------------------------------- --
-- Fixed-width decoding

word32BE :: B.ByteString -> Maybe Word32
word32BE b
    | B.length b == 4 = Just $! B.foldl' (\a x -> a `shiftL` 8 .|. fromIntegral x) 0 b
    | otherwise = Nothing

-- | Exactly 8 bytes.
word64BE :: B.ByteString -> Maybe Word64
word64BE b
    | B.length b == 8 = Just $! B.foldl' (\a x -> a `shiftL` 8 .|. fromIntegral x) 0 b
    | otherwise = Nothing

-- -------------------------------------------------------------------------- --
-- Key id lists (LEB128 varints)

-- | Each id as an unsigned LEB128 varint: 7 bits per byte, least significant
-- group first, high bit set on all bytes but the last.
encodeKeyIdList :: [KeyId] -> B.ByteString
encodeKeyIdList = build . foldMap (\(KeyId i) -> varint i)
  where
    varint :: Word32 -> BB.Builder
    varint w
        | w < 0x80 = BB.word8 (fromIntegral w)
        | otherwise = BB.word8 (fromIntegral (w .&. 0x7F) .|. 0x80) <> varint (w `shiftR` 7)

-- | The inverse of 'encodeKeyIdList'. Nothing on a truncated varint, a value
-- above 2^32 - 1, or a non-minimal encoding (so the encoding is canonical).
decodeKeyIdList :: B.ByteString -> Maybe [KeyId]
decodeKeyIdList s = case B.uncons s of
    Nothing -> Just []
    Just _ -> do
        (i, rest) <- varint 0 0 s
        (KeyId i :) <$> decodeKeyIdList rest
  where
    varint :: Int -> Word64 -> B.ByteString -> Maybe (Word32, B.ByteString)
    varint !n !acc b = do
        (x, rest) <- B.uncons b
        let acc' = acc .|. (fromIntegral (x .&. 0x7F) `shiftL` (7 * n))
        if x .&. 0x80 /= 0
            then if n >= 4 then Nothing else varint (n + 1) acc' rest
            else if (n > 0 && x == 0) || acc' > fromIntegral (maxBound :: Word32)
                then Nothing
                else Just (fromIntegral acc', rest)
