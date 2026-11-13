{-# LANGUAGE DataKinds #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}

-- |
-- Module: Chainweb.Test.Pact.Store.Characterization
--
-- Characterization goldens for the SQLite path of the Pact checkpointer.
--
-- These pin what chainweb-node 3.2.1 writes to and reads from SQLite, so that
-- moving the SQL statements behind @PactStore@ can be shown to preserve
-- behaviour byte for byte. The scripts are fixed; nothing is random.
--
-- Regenerate only on purpose:
--
-- > cabal run chainweb-tests -- -p '/pact-store/' --accept
--
module Chainweb.Test.Pact.Store.Characterization
    ( tests
    -- * The script, for differential tests of the store
    , cid
    , extend
    , p4Block
    , p4Header
    , p4Headers
    , p4Read
    , scenario
    , v4
    , withP4
    ) where

import Control.Exception (evaluate)
import Control.Exception.Safe
import Control.Lens
import Control.Monad
import Control.Monad.Trans.Except (runExceptT)
import Control.Monad.Trans.Reader (runReaderT)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as SB
import Data.Foldable
import qualified Data.HashMap.Strict as HM
import Data.IORef (newIORef)
import Data.List (intersperse, sortOn)
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import Data.String (fromString)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Data.Word
import qualified Streaming.Prelude as Stream
import System.LogLevel
import Test.Tasty (TestTree, testGroup)

import Database.SQLite3.Direct (Utf8(..))

-- pact 4
import qualified Pact.Interpreter as P4 (PactDbEnv(..))
import qualified Pact.Types.Command as P4 (RequestKey(..))
import qualified Pact.Types.KeySet as P4
import qualified Pact.Types.RowData as P4
import qualified Pact.Types.Runtime as P4
import qualified Pact.Types.SQLite as P4SQL

-- pact 5
import Pact.Core.Builtin (CoreBuiltin)
import Pact.Core.Command.Types (RequestKey(..))
import Pact.Core.Errors (PactError)
import Pact.Core.Evaluate (Info)
import Pact.Core.Gas (GasEnv(..), MilliGas(..), freeGasModel)
import Pact.Core.Guards (KSPredicate(..), KeySet(..), KeySetName(..), PublicKeyText(..))
import Pact.Core.Hash (Hash(..))
import Pact.Core.Literal
import Pact.Core.Names
import Pact.Core.PactValue
import Pact.Core.Persistence

-- chainweb
import Chainweb.BlockHash
import Chainweb.BlockHeader.Internal
import Chainweb.BlockHeight
import Chainweb.Logger
import Chainweb.Pact.Backend.Types
import Chainweb.Pact.Backend.Store.Types (PactStore(..))
import Chainweb.Pact.Backend.Utils
import qualified Chainweb.Pact.PactService.Checkpointer.Internal as Checkpointer
import Chainweb.Pact.Types
import qualified Chainweb.Pact4.Backend.ChainwebPactDb as Pact4
import Chainweb.Pact5.Backend.ChainwebPactDb (Pact5Db(..))
import Chainweb.Pact5.Types (noInfo)
import Chainweb.Test.TestVersions
import Chainweb.Test.Utils (golden, petersen)
import Chainweb.Utils
import Chainweb.Utils.Serialization (runGetEitherS)
import Chainweb.Version
import Chainweb.Graph (singletonChainGraph)

-- -------------------------------------------------------------------------- --
-- Tests

tests :: TestTree
tests = testGroup "Chainweb.Test.Pact.Store.Characterization"
    [ golden "pact-store-pact4-commit" $ render <$> pact4Commit
    , golden "pact-store-pact5-commit" $ render <$> pact5Commit
    , golden "pact-store-pact4-history" $ render <$> pact4History
    , golden "pact-store-missing-table-errors" $ render <$> missingTableErrors
    , golden "pact-store-reads" $ render <$> readsAtHeights
    , golden "pact-store-rewind" $ render <$> rewind
    , golden "pact-store-pact5-rewind" $ render <$> pact5Rewind
    , golden "pact-store-lookup-txs" $ render <$> lookupTxs
    ]
  where
    render = BL.fromStrict . T.encodeUtf8 . T.unlines

-- | Pact 4 blocks with 'PersistIntraBlockWrites': every write of every
-- transaction is a row, so several transactions writing one key in one block
-- leave one row each.
pact4Commit :: IO [Text]
pact4Commit = withP4 $ \cp sql -> do
    extend cp Nothing (zipWith p4Block p4Headers scenario)
    dumpDb sql

-- | The same script in Pact 5 blocks.
pact5Commit :: IO [Text]
pact5Commit = withP5 $ \cp sql -> do
    extend cp Nothing (zipWith p5Block p5Headers scenario)
    dumpDb sql

-- | @_txids@, @_getTxLog@ and @_keys@ at the tip and rewound. Pact42 (sorted
-- keys) is on from height 5 in 'v4'.
pact4History :: IO [Text]
pact4History = withP4 $ \cp _ -> do
    let (early, late) = splitAt 3 (zip p4Headers scenario)
    extend cp Nothing (uncurry p4Block <$> early)
    tipOff <- p4Read cp (p4Header 2) historyProbe
    extend cp (Just (p4Header 2)) (uncurry p4Block <$> late)
    tipOn <- p4Read cp (p4Header 6) historyProbe
    rewoundOff <- p4Read cp (p4Header 2) historyProbe
    rewoundOn <- p4Read cp (p4Header 5) historyProbe
    return $ concat
        [ section "tip (chain has blocks 0 to 2), parent height 2, block height 3 (Pact42 sort off)" tipOff
        , section "tip (chain has blocks 0 to 6), parent height 6, block height 7 (Pact42 sort on)" tipOn
        , section "rewound (chain has blocks 0 to 6), parent height 2, block height 3 (Pact42 sort off)" rewoundOff
        , section "rewound (chain has blocks 0 to 6), parent height 5, block height 6 (Pact42 sort on)" rewoundOn
        ]
  where
    historyProbe env = do
        let P4.PactDbEnv pdb mv = Pact4._cpPactDbEnv env
        keys1 <- probe "keys t1" $ P4._keys pdb (ut4 "t1") mv
        keys2 <- probe "keys t2" $ P4._keys pdb (ut4 "t2") mv
        txids1 <- P4._txids pdb "t1" (P4.TxId 0) mv
        txids2 <- probe "txids t2 > 0" $ P4._txids pdb "t2" (P4.TxId 0) mv
        txids1' <- probe "txids t1 > 4" $ P4._txids pdb "t1" (P4.TxId 4) mv
        logs <- forM txids1 $ \tid ->
            probe ("getTxLog t1 " <> sshow tid) $ P4._getTxLog pdb (ut4 "t1") tid mv
        _ <- P4._beginTx pdb P4.Transactional mv
        P4._writeRow pdb P4.Write (ut4 "t1") "new" (row4 99) mv
        pendingKeys <- probe "keys t1 after a pending write of new" $ P4._keys pdb (ut4 "t1") mv
        pendingTxids <- probe "txids t1 > 0 after a pending write of new" $ P4._txids pdb "t1" (P4.TxId 0) mv
        P4._rollbackTx pdb mv
        return $
            [ keys1, keys2, "txids t1 > 0: " <> sshow txids1, txids2, txids1' ]
            <> logs <> [pendingKeys, pendingTxids]

-- | Reads and keys of a table before its creation height, of a table that
-- never exists, of an existing table under a different case (@T2@) and of a
-- missing key in an existing table, at the tip and rewound, in Pact 4 and
-- Pact 5.
missingTableErrors :: IO [Text]
missingTableErrors = do
    p4 <- withP4 $ \cp _ -> do
        extend cp Nothing (zipWith p4Block p4Headers scenario)
        tip <- p4Read cp (p4Header 6) $ \env -> concatMapM (p4Probe env)
            [("nope", "a"), ("t2", "x"), ("T2", "x"), ("t1", "nokey")]
        r0 <- p4Read cp (p4Header 0) $ \env -> concatMapM (p4Probe env)
            [("t2", "x"), ("nope", "a")]
        r1 <- p4Read cp (p4Header 1) $ \env -> concatMapM (p4Probe env) [("t2", "x")]
        r2 <- p4Read cp (p4Header 2) $ \env -> concatMapM (p4Probe env) [("t2", "x"), ("T2", "x"), ("t1", "nokey")]
        return $ concat
            [ section "Pact 4 tip, block height 7" tip
            , section "Pact 4 rewound, block height 1 (t2 is created at 2)" r0
            , section "Pact 4 rewound, block height 2 (t2 is created at 2)" r1
            , section "Pact 4 rewound, block height 3" r2
            ]
    p5 <- withP5 $ \cp _ -> do
        extend cp Nothing (zipWith p5Block p5Headers scenario)
        tip <- p5Read cp (p5Header 6) $ \pdb -> concatMapM (p5Probe pdb)
            [("nope", "a"), ("t2", "x"), ("T2", "x"), ("t1", "nokey")]
        r0 <- p5Read cp (p5Header 0) $ \pdb -> concatMapM (p5Probe pdb)
            [("t2", "x"), ("nope", "a")]
        r1 <- p5Read cp (p5Header 1) $ \pdb -> concatMapM (p5Probe pdb) [("t2", "x")]
        r2 <- p5Read cp (p5Header 2) $ \pdb -> concatMapM (p5Probe pdb) [("t2", "x"), ("T2", "x"), ("t1", "nokey")]
        return $ concat
            [ section "Pact 5 tip, block height 7" tip
            , section "Pact 5 rewound, block height 1 (t2 is created at 2)" r0
            , section "Pact 5 rewound, block height 2 (t2 is created at 2)" r1
            , section "Pact 5 rewound, block height 3" r2
            ]
    return (p4 <> p5)
  where
    p4Probe env (t, k) = do
        let P4.PactDbEnv pdb mv = Pact4._cpPactDbEnv env
        r <- probe ("read " <> t <> " " <> k) $ P4._readRow pdb (ut4 t) (P4.RowKey k) mv
        ks <- probe ("keys " <> t) $ P4._keys pdb (ut4 t) mv
        return [r, ks]
    p5Probe pdb (t, k) = do
        r <- probe5 ("read " <> t <> " " <> k) $ _pdbRead pdb (DUserTables (tn5 t)) (RowKey k)
        ks <- probe5 ("keys " <> t) $ _pdbKeys pdb (DUserTables (tn5 t))
        return [r, ks]

-- | Full dumps before and after 'rewindTo', in two fresh databases:
--
-- * parent height 2: the limit is the ending txid of block 2, which is 7.
--   @t1@ has a row at exactly txid 7 (@zz@), and so has @SYS:KeySets@
--   (@ks2@), so @txid >= 7@ and @txid > 7@ give different dumps;
-- * parent height 1: the limit is 4, below the creation of @t2@ and below
--   rows of @t1@;
-- * parent height 5 (limit 12), with extra rows in the system tables.
rewind :: IO [Text]
rewind = rewindScript withP4 (zipWith p4Block p4Headers scenario) p4Header

-- | The same rewinds in Pact 5 blocks.
pact5Rewind :: IO [Text]
pact5Rewind = rewindScript withP5 (zipWith p5Block p5Headers scenario) p5Header

rewindScript
    :: ((Checkpointer GenericLogger -> SQLiteEnv -> IO [Text]) -> IO [Text])
    -> [RunnableBlock GenericLogger ()]
    -> (Int -> BlockHeader)
    -> IO [Text]
rewindScript with blocks hdr = do
    a <- with $ \cp sql -> do
        extend cp Nothing blocks
        before <- dumpDb sql
        Checkpointer.rewindTo cp (Just (ParentHeader (hdr 2)))
        after <- dumpDb sql
        return $ section "before rewindTo" before
            <> section "after rewindTo parent height 2 (limit 7)" after
    b <- with $ \cp sql -> do
        extend cp Nothing blocks
        Checkpointer.rewindTo cp (Just (ParentHeader (hdr 1)))
        after <- dumpDb sql
        return $ section "after rewindTo parent height 1 (limit 4)" after
    -- The checkpointer records every mutated table, system tables included,
    -- in VersionedTableMutation, and the rewind deletes from those tables
    -- first. So its separate deletes from the five system tables only matter
    -- for rows that no mutation row accounts for. Plant such rows around the
    -- limit of parent height 5, which is 12 (txid 11 stays, 12 and 20 go), to
    -- pin those statements. No block after height 5 mutates a system table,
    -- so the generic deletes do not reach them either.
    c <- with $ \cp sql -> do
        extend cp Nothing blocks
        forM_ ["SYS:KeySets", "SYS:Modules", "SYS:Namespaces", "SYS:Pacts", "SYS:ModuleSources"] $ \t ->
            P4SQL.exec_ sql $ Utf8 $ T.encodeUtf8 $
                "INSERT INTO [" <> t <> "] (rowkey, txid, rowdata) VALUES \
                \('orphan-11', 11, x'AA'), ('orphan-12', 12, x'BB'), ('orphan-20', 20, x'CC')"
        Checkpointer.rewindTo cp (Just (ParentHeader (hdr 5)))
        after <- dumpDb sql
        return $ section "after rewindTo parent height 5 (limit 12), system tables holding rows without a mutation row" after
    return (a <> b <> c)

-- | Reads and keys of @t1@, @t2@ and @SYS:KeySets@ at the tip and rewound, in
-- Pact 4 and Pact 5. The parent heights are chosen against the ending txids
-- of the blocks (1, 4, 7, 9, 10, 12, 13):
--
-- * 6: the tip, limit 13;
-- * 5: limit 12, which is the txid of @B@, so @B@ is not visible;
-- * 2: limit 7, which is the txid of @zz@ and @ks2@, so they are not visible;
-- * 1: limit 4, below the creation of @t2@.
readsAtHeights :: IO [Text]
readsAtHeights = do
    p4 <- withP4 $ \cp _ -> do
        extend cp Nothing (zipWith p4Block p4Headers scenario)
        forM readParents $ \(p, lbl) ->
            section ("Pact 4 " <> lbl) <$> p4Read cp (p4Header p) readProbe4
    p5 <- withP5 $ \cp _ -> do
        extend cp Nothing (zipWith p5Block p5Headers scenario)
        forM readParents $ \(p, lbl) ->
            section ("Pact 5 " <> lbl) <$> p5Read cp (p5Header p) readProbe5
    return (concat p4 <> concat p5)
  where
    readParents =
        [ (6, "tip, parent height 6 (limit 13)")
        , (5, "rewound, parent height 5 (limit 12)")
        , (2, "rewound, parent height 2 (limit 7)")
        , (1, "rewound, parent height 1 (limit 4)")
        ]
    rowProbes = [ ("t1", "zz"), ("t1", "a"), ("t1", "B"), ("t1", "nokey"), ("t2", "x"), ("t2", "nokey") ]
    readProbe4 env = do
        let P4.PactDbEnv pdb mv = Pact4._cpPactDbEnv env
        rows <- forM rowProbes $ \(t, k) ->
            probe ("read " <> t <> " " <> k) $ P4._readRow pdb (ut4 t) (P4.RowKey k) mv
        keys <- forM ["t1", "t2"] $ \t -> probe ("keys " <> t) $ P4._keys pdb (ut4 t) mv
        kss <- forM ["ks1", "ks2", "nokeyset"] $ \k ->
            probe ("read keyset " <> k) $ P4._readRow pdb P4.KeySets (fromString (T.unpack k)) mv
        ksKeys <- probe "keys SYS:KeySets" $ P4._keys pdb P4.KeySets mv
        return (rows <> keys <> kss <> [ksKeys])
    readProbe5 pdb = do
        rows <- forM rowProbes $ \(t, k) ->
            probe5 ("read " <> t <> " " <> k) $ _pdbRead pdb (DUserTables (tn5 t)) (RowKey k)
        keys <- forM ["t1", "t2"] $ \t -> probe5 ("keys " <> t) $ _pdbKeys pdb (DUserTables (tn5 t))
        kss <- forM ["ks1", "ks2", "nokeyset"] $ \k ->
            probe5 ("read keyset " <> k) $ _pdbRead pdb DKeySets (KeySetName k Nothing)
        ksKeys <- probe5 "keys SYS:KeySets" $ _pdbKeys pdb DKeySets
        return (rows <> keys <> kss <> [ksKeys])

-- | '_psDoLookupSuccessful' (3.2.1's 'doLookupSuccessful') for found, unknown
-- and at-or-above-height hashes.
lookupTxs :: IO [Text]
lookupTxs = withP4 $ \cp _ -> do
    extend cp Nothing (zipWith p4Block p4Headers scenario)
    let hashes = ["tx-0-0", "tx-1-1", "tx-2-0", "tx-3-1", "tx-6-0", "unknown"]
    fmap concat $ forM [0, 2, 3, 7] $ \h -> do
        r <- _psDoLookupSuccessful (cpStore cp) (BlockHeight h) (V.fromList (SB.toShort <$> hashes))
        return $ section ("doLookupSuccessful, current height " <> sshow h <> ", " <> T.decodeUtf8 (B8.unwords hashes))
            [ T.decodeUtf8 (SB.fromShort k) <> " -> " <> sshow bh <> " " <> sshow bhash
            | (k, T2 bh bhash) <- sortOn fst (HM.toList r)
            ]

-- -------------------------------------------------------------------------- --
-- The script

data Op = Create Text | Put Text Text Integer | Ks Text

-- | A transaction: its hash (registered as successful) and its operations.
type Tx = (BS.ByteString, [Op])

tx :: Int -> Int -> [Op] -> Tx
tx b i ops = (B8.pack ("tx-" <> show b <> "-" <> show i), ops)

-- | One list of transactions per block, from genesis up.
scenario :: [[Tx]]
scenario =
    [ [ tx 0 0 [Create "t1", Put "t1" "a" 1] ]
    , [ tx 1 0 [Put "t1" "b" 2, Ks "ks1"], tx 1 1 [Put "t1" "b" 3], tx 1 2 [Put "t1" "C" 4, Put "t1" "b" 5] ]
    , [ tx 2 0 [Create "t2", Put "t2" "x" 6], tx 2 1 [Put "t2" "x" 7], tx 2 2 [Put "t1" "a" 8] ]
    , [ tx 3 0 [Put "t1" "zz" 9, Ks "ks2"], tx 3 1 [Put "t1" "k10" 10, Put "t2" "x" 11] ]
    , [ tx 4 0 [Put "t1" "k9" 12, Ks "ks1"] ]
    , [ tx 5 0 [Put "t2" "y" 13], tx 5 1 [Put "t2" "y" 14] ]
    , [ tx 6 0 [Put "t1" "B" 15] ]
    ]

-- -------------------------------------------------------------------------- --
-- Pact 4

v4 :: ChainwebVersion
v4 = slowForkingCpmTestVersion petersen

p4Headers :: [BlockHeader]
p4Headers = headersFor v4

p4Header :: Int -> BlockHeader
p4Header = (p4Headers !!)

withP4 :: (Checkpointer GenericLogger -> SQLiteEnv -> IO a) -> IO a
withP4 k = withTempSQLiteConnection chainwebPragmas $ \sql -> do
    cp <- Checkpointer.initCheckpointerResources
        defaultModuleCacheLimit sql PersistIntraBlockWrites quietLogger v4 cid
    k cp sql

ut4 :: Text -> P4.Domain P4.RowKey P4.RowData
ut4 = P4.UserTables . P4.TableName

row4 :: Integer -> P4.RowData
row4 n = P4.RowData P4.RDV1 $ P4.ObjectMap $ M.fromList [("v", P4.RDLiteral (P4.LInteger n))]

p4Block :: BlockHeader -> [Tx] -> RunnableBlock GenericLogger ()
p4Block bh txs = Pact4RunnableBlock $ \env _ -> do
    forM_ txs $ \(h, ops) -> do
        let P4.PactDbEnv pdb mv = Pact4._cpPactDbEnv env
        _ <- P4._beginTx pdb P4.Transactional mv
        forM_ ops $ \case
            Create t -> P4._createUserTable pdb (P4.TableName t) (P4.ModuleName "m" Nothing) mv
            Put t k n -> P4._writeRow pdb P4.Write (ut4 t) (P4.RowKey k) (row4 n) mv
            Ks k -> P4._writeRow pdb P4.Write P4.KeySets (fromString (T.unpack k))
                (P4.mkKeySet [P4.PublicKeyText ("pk-" <> k)] "keys-all") mv
        _ <- P4._commitTx pdb mv
        Pact4._cpRegisterProcessedTx env (P4.RequestKey (P4.Hash (SB.toShort h)))
    return ((), bh)

p4Read
    :: Checkpointer GenericLogger
    -> BlockHeader
    -> (Pact4.CurrentBlockDbEnv GenericLogger -> IO [Text])
    -> IO [Text]
p4Read cp ph k =
    Checkpointer.readFrom cp (Just (ParentHeader ph)) Pact4T (\env _ -> k env) >>= \case
        NoHistory -> return ["NoHistory"]
        Historical r -> return r

-- -------------------------------------------------------------------------- --
-- Pact 5

v5 :: ChainwebVersion
v5 = pact5CheckpointerTestVersion singletonChainGraph

p5Headers :: [BlockHeader]
p5Headers = headersFor v5

p5Header :: Int -> BlockHeader
p5Header = (p5Headers !!)

withP5 :: (Checkpointer GenericLogger -> SQLiteEnv -> IO a) -> IO a
withP5 k = withTempSQLiteConnection chainwebPragmas $ \sql -> do
    cp <- Checkpointer.initCheckpointerResources
        defaultModuleCacheLimit sql DoNotPersistIntraBlockWrites quietLogger v5 cid
    k cp sql

tn5 :: Text -> TableName
tn5 t = TableName t (ModuleName "m" Nothing)

p5Block :: BlockHeader -> [Tx] -> RunnableBlock GenericLogger ()
p5Block bh txs = Pact5RunnableBlock $ \db _ startHandle -> do
    endHandle <- foldM (runTx db) startHandle txs
    return (((), bh), endHandle)
  where
    runTx db hndl (h, ops) = fmap snd $
        doPact5DbTransaction db hndl (Just (RequestKey (Hash (SB.toShort h)))) $ \pdb -> do
            _ <- ignoreGas noInfo $ _pdbBeginTx pdb Transactional
            forM_ ops $ \case
                Create t -> ignoreGas noInfo $ _pdbCreateUserTable pdb (tn5 t)
                Put t k n -> ignoreGas noInfo $ _pdbWrite pdb Write (DUserTables (tn5 t)) (RowKey k)
                    (RowData (M.singleton (Field "v") (PLiteral (LInteger n))))
                Ks k -> ignoreGas noInfo $ _pdbWrite pdb Write DKeySets (KeySetName k Nothing)
                    (KeySet (S.singleton (PublicKeyText ("pk-" <> k))) KeysAll)
            void $ ignoreGas noInfo $ _pdbCommitTx pdb

p5Read
    :: Checkpointer GenericLogger
    -> BlockHeader
    -> (PactDb CoreBuiltin Info -> IO [Text])
    -> IO [Text]
p5Read cp ph k =
    Checkpointer.readFrom cp (Just (ParentHeader ph)) Pact5T run >>= \case
        NoHistory -> return ["NoHistory"]
        Historical r -> return r
  where
    run db hndl = fmap fst $ doPact5DbTransaction db hndl Nothing $ \pdb -> do
        _ <- ignoreGas noInfo $ _pdbBeginTx pdb Transactional
        k pdb

-- -------------------------------------------------------------------------- --
-- Utils

cid :: ChainId
cid = unsafeChainId 0

quietLogger :: GenericLogger
quietLogger = genericLogger Error (\_ -> return ())

mkHash :: Word8 -> BlockHash
mkHash n = either error id $ runGetEitherS decodeBlockHash (BS.replicate 32 n)

-- | An endless chain of fabricated headers, from genesis up, with fixed
-- hashes.
headersFor :: ChainwebVersion -> [BlockHeader]
headersFor v = go Nothing 0
  where
    go parent (i :: Int) =
        let bh = case parent of
                Nothing -> genesisBlockHeader v cid
                    & blockHash .~ mkHash (fromIntegral (0x10 + i))
                Just p -> p
                    & blockHash .~ mkHash (fromIntegral (0x10 + i))
                    & blockParent .~ view blockHash p
                    & blockHeight .~ view blockHeight p + 1
        in bh : go (Just bh) (i + 1)

extend :: Checkpointer GenericLogger -> Maybe BlockHeader -> [RunnableBlock GenericLogger ()] -> IO ()
extend cp parent blocks =
    void $ Checkpointer.restoreAndSave cp (ParentHeader <$> parent) (traverse_ Stream.yield blocks)

section :: Text -> [Text] -> [Text]
section title body = ("== " <> title) : fmap ("  " <>) body

-- | Run an action and render its result or its exception. Call stacks are
-- cut off, so that line numbers in the code under test do not leak into the
-- goldens.
tryShow :: Show a => IO a -> IO (Either Text Text)
tryShow act = handleAny (return . Left . renderExc) $ do
    a <- act
    let t = sshow a
    _ <- evaluate (T.length t)
    return (Right t)
  where
    renderExc = \case
        (fromException -> Just (PactInternalError _ msg)) -> "PactInternalError: " <> msg
        e -> T.stripEnd $ fst $ T.breakOn "CallStack (from HasCallStack)" $ T.pack $ displayException e

renderResult :: Either Text Text -> Text
renderResult = either ("error: " <>) ("ok: " <>)

probe :: Show a => Text -> IO a -> IO Text
probe label act = (\r -> label <> ": " <> renderResult r) <$> tryShow act

-- | Like 'probe' for a Pact 5 'GasM' action, keeping the Pact 5 error value
-- (which 'ignoreGas' would replace by an "out of gas" error).
probe5 :: Show a => Text -> GasM CoreBuiltin Info a -> IO Text
probe5 label act = do
    r <- tryShow (runGas act)
    return $ label <> ": " <> case r of
        Left e -> "error: " <> e
        Right t -> t
  where
    runGas :: GasM CoreBuiltin Info a -> IO (PactErrorOr a)
    runGas m = do
        gasRef <- newIORef (MilliGas 0)
        either PactErrorValue Ok <$> runExceptT (runReaderT (runGasM m)
            (GasEnv { _geGasRef = gasRef, _geGasLog = Nothing, _geGasModel = freeGasModel }, noInfo, []))

data PactErrorOr a = PactErrorValue (PactError Info) | Ok a

instance Show a => Show (PactErrorOr a) where
    show (PactErrorValue e) = "pact 5 error value: " <> show e
    show (Ok a) = "ok: " <> show a

-- | Dump the schema and every table. Tables shaped like Pact tables
-- (rowkey, txid, rowdata) are dumped as @SELECT rowid, rowkey, txid,
-- hex(rowdata) ... ORDER BY rowid@; the others quote every column.
dumpDb :: SQLiteEnv -> IO [Text]
dumpDb sql = do
    objs <- P4SQL.qry_ sql
        "SELECT type, name, tbl_name FROM sqlite_master ORDER BY type, name"
        [P4SQL.RText, P4SQL.RText, P4SQL.RText]
    let tables = [ n | [P4SQL.SText "table", P4SQL.SText n, _] <- objs ]
    dumps <- mapM dumpTable tables
    return $ section "schema" (renderRow <$> objs) <> concat dumps
  where
    dumpTable name = do
        cols <- P4SQL.qry_ sql ("PRAGMA table_info('" <> name <> "')") [P4SQL.RInt, P4SQL.RText]
        let colNames = [ c | [_, P4SQL.SText c] <- cols ]
        rows <- if colNames == ["rowkey", "txid", "rowdata"]
            then P4SQL.qry_ sql
                ("SELECT rowid, rowkey, txid, hex(rowdata) FROM [" <> name <> "] ORDER BY rowid")
                [P4SQL.RInt, P4SQL.RText, P4SQL.RInt, P4SQL.RText]
            else P4SQL.qry_ sql
                ("SELECT rowid, " <> mconcat (intersperse ", " ((\c -> "quote(" <> c <> ")") <$> colNames))
                    <> " FROM [" <> name <> "] ORDER BY rowid")
                (P4SQL.RInt : (P4SQL.RText <$ colNames))
        return $ section
            ("table " <> utf8 name <> " (rowid, " <> T.intercalate ", " (utf8 <$> colNames) <> ")")
            (renderRow <$> rows)
    utf8 (Utf8 b) = T.decodeUtf8 b
    renderRow = T.intercalate " | " . fmap renderS
    renderS = \case
        P4SQL.SInt i -> sshow i
        P4SQL.SText t -> utf8 t
        P4SQL.SBlob b -> sshow b
        P4SQL.SDouble d -> sshow d

concatMapM :: Monad m => (a -> m [b]) -> [a] -> m [b]
concatMapM f = fmap concat . mapM f
