{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

-- |
-- Module: Chainweb.Pact.Backend.Store.Types
-- License: See LICENSE file
--
-- The storage interface of the Pact checkpointer.
--
-- Every SQLite statement that the checkpointer and the Pact 4 and Pact 5
-- databases run is an operation of 'PactStore'. Operations return what the SQL
-- returned, in the same order and with the same errors: row-level operations
-- never catch or wrap SQLite errors, @Left e@ carries the raw 'SQ3.Error', and
-- each caller keeps its own error handling. Fork logic stays above the
-- interface.
--
-- Tables are named by their SQL names (for example @SYS:KeySets@ or
-- @free.coin_coin-table@); row keys and row data are the stored bytes.
--
module Chainweb.Pact.Backend.Store.Types
    ( PactStore(..)
    , KeysQuery(..)
    , BlockCommit(..)
    , Historical(..)
    , _Historical
    , _NoHistory
    ) where

import Control.DeepSeq (NFData)
import Control.Lens (makePrisms)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Short as SB
import Data.HashMap.Strict (HashMap)
import Data.Text (Text)
import Data.Vector (Vector)
import qualified Database.SQLite3.Direct as SQ3
import GHC.Generics (Generic)

import Chainweb.BlockHash
import Chainweb.BlockHeader.Internal (ParentHeader)
import Chainweb.BlockHeight
import Chainweb.Utils (T2)
import Chainweb.Version

import qualified Pact.Types.Persistence as Pact4

-- | The result of a historical lookup which might fail to even find the
-- header the history is being queried for.
data Historical a
    = Historical a
    | NoHistory
    deriving stock (Eq, Foldable, Functor, Generic, Traversable, Show)
    deriving anyclass NFData

makePrisms ''Historical

type TxId = Pact4.TxId

-- | Which @SELECT DISTINCT rowkey@ statement '_psDistinctKeys' runs, and its
-- bound: only rows with @txid < bound@ count.
data KeysQuery
    = KeysAsStored !(Maybe TxId)
    -- ^ Pact 4's statement, without @ORDER BY@: the keys in the order SQLite
    -- returns them. Unbounded at the tip, bounded when rewound.
    | KeysOrderedByKey !TxId
    -- ^ Pact 5's statement, with @ORDER BY rowkey@. Pact 5 always reads
    -- below a bound, so the bound is mandatory.
    deriving stock (Eq, Show)

-- | Everything one block writes, in the order 3.2.1 writes it.
data BlockCommit = BlockCommit
    { _bcHeight :: !BlockHeight
    , _bcHash :: !BlockHash
    , _bcEndTxId :: !TxId
        -- ^ the @endingtxid@ of the block's 'BlockHistory' row
    , _bcNewTables :: ![Text]
        -- ^ created first, each followed by its @VersionedTableCreation@ row
    , _bcRows :: ![(Text, [(ByteString, TxId, ByteString)])]
        -- ^ per table, @(rowkey, txid, rowdata)@ in insertion order; each
        -- table's @VersionedTableMutation@ row is written right after its
        -- rows, so the mutated tables are exactly the tables listed here
    , _bcTxHashes :: ![ByteString]
        -- ^ the block's successful transactions, for @TransactionIndex@
    }
    deriving stock (Eq, Show)

-- | The storage operations of the Pact checkpointer.
--
-- Fields that replace a 3.2.1 function taking an 'SQLiteEnv' have that
-- function's type without the 'SQLiteEnv' argument, and behave exactly like
-- it, exceptions included.
data PactStore = PactStore
    { _psGetLatestBlock :: !(IO (Maybe (BlockHeight, BlockHash)))
    , _psGetEarliestBlock :: !(IO (Maybe (BlockHeight, BlockHash)))
    , _psLookupBlock :: !((BlockHeight, BlockHash) -> IO Bool)
    , _psGetBlockParent :: !(ChainwebVersion -> ChainId -> (BlockHeight, BlockHash) -> IO (Maybe BlockHash))
    , _psGetEndTxId :: !(Text -> Maybe ParentHeader -> IO (Historical TxId))
    , _psGetEndTxId' :: !(Text -> BlockHeight -> BlockHash -> IO (Historical TxId))
    , _psDoLookupSuccessful
        :: !(BlockHeight -> Vector SB.ShortByteString -> IO (HashMap SB.ShortByteString (T2 BlockHeight BlockHash)))
    , _psRewindDbTo :: !(Maybe ParentHeader -> IO TxId)
        -- ^ delete all state newer than the parent; returns the parent's
        -- ending txid
    , _psBackupTo :: !(FilePath -> IO ())
        -- ^ @VACUUM main INTO@ the given file
    , _psInitSchema :: !(IO ())
        -- ^ create the tables that exist before genesis

    , _psReadNewest :: !(Text -> ByteString -> Maybe TxId -> IO (Either SQ3.Error (Maybe ByteString)))
        -- ^ table, rowkey, optional bound: the newest row data of the key,
        -- among rows with @txid < bound@ when a bound is given
    , _psRowsInTxIdRange :: !(Text -> TxId -> TxId -> IO (Either SQ3.Error [(TxId, ByteString, ByteString)]))
        -- ^ table, from, to: @(txid, rowkey, rowdata)@ with
        -- @txid >= from AND txid < to@, in the order SQLite returns them
    , _psReadPrev :: !(Text -> ByteString -> TxId -> IO (Either SQ3.Error (Maybe ByteString)))
        -- ^ table, rowkey, bound: the newest row data with @txid < bound@
    , _psReadHistorical :: !(Text -> ByteString -> TxId -> IO (Either SQ3.Error (Maybe (ByteString, ByteString))))
        -- ^ table, rowkey, bound: the stored rowkey and row data of the newest
        -- row with @txid < bound@
    , _psDistinctKeys :: !(KeysQuery -> Text -> IO (Either SQ3.Error [ByteString]))
        -- ^ the distinct row keys of the table, among the rows the query's
        -- bound admits
    , _psTxIdsAfter :: !(Text -> TxId -> IO (Either SQ3.Error [TxId]))
        -- ^ the distinct txids greater than the given one
    , _psRowsAtTxId :: !(Text -> TxId -> IO (Either SQ3.Error [(ByteString, ByteString)]))
        -- ^ @(rowkey, rowdata)@ of the rows written at the txid, in rowid
        -- order
    , _psTableExistsAt :: !(Text -> BlockHeight -> IO (Either SQ3.Error Bool))
        -- ^ whether @VersionedTableCreation@ has the table (compared lower
        -- case) with a creation height below the given height
    , _psTableInCatalog :: !(Bool -> Text -> IO (Either SQ3.Error Bool))
        -- ^ whether @sqlite_master@ has exactly one table of that name;
        -- compared lower case when the flag is set
    , _psCommitBlock :: !(BlockCommit -> IO (Either SQ3.Error ()))
        -- ^ write a block: new tables, rows, transaction index, block
        -- history, stopping at the first error
    , _psWithBatch :: forall a. IO a -> IO a
        -- ^ run in the @batch@ savepoint: released on success, rolled back
        -- on any exception
    , _psWithReadSnapshot :: forall a. IO a -> IO a
        -- ^ run in the @batch@ savepoint and always roll it back
    }
