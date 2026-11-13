{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

-- |
-- Module: Chainweb.StorageTools.Compare.PactSet
--
-- The Pact code that @cw-compare@ sends with @/local@, preflight and history
-- requests: a list of entries, each with the request classes it serves and a
-- code template with placeholders that are filled from the oracle's block at
-- the target height. The default set is @storage-tools/data/pact-set.yaml@,
-- embedded at build time.
--
module Chainweb.StorageTools.Compare.PactSet
( PactKind(..)
, PactExpect(..)
, expectsSuccess
, PactEntry(..)
, PactSet(..)
, defaultPactSet
, decodePactSet
, loadPactSet
, pactSetProblems
, liveEntries
, entriesFor
, generatedEntries
) where

import Control.Monad ((>=>))

import Data.Aeson
import qualified Data.ByteString as B
import Data.FileEmbed (embedFile)
import qualified Data.Text as T
import qualified Data.Yaml as Yaml

-- internal modules

import Chainweb.StorageTools.Compare.Types

-- | Code to execute, or a continuation of a defpact.
--
data PactKind = PactExec | PactCont
    deriving (Show, Eq)

-- | The Pact result an entry expects on both sides, from its @why@.
--
data PactExpect
    = ExpectSuccess
        -- ^ a run that passes needs at least one 'Same' answer of the entry
        -- with Pact result status success, per class
    | ExpectFailure
        -- ^ the entry tests an error both sides must give alike
    | ExpectEither
        -- ^ success or failure, depending on the target height
    deriving (Show, Eq)

expectsSuccess :: PactEntry -> Bool
expectsSuccess = (== ExpectSuccess) . _peExpect

data PactEntry = PactEntry
    { _peName :: !T.Text
    , _peClasses :: ![RequestClass]
        -- ^ of 'ClsLocal', 'ClsHistory' and 'ClsPreflight'
    , _peKind :: !PactKind
    , _peCode :: !T.Text
        -- ^ the code, or for 'PactCont' the pact id; with the placeholders
        -- @{account}@, @{txid}@ and @{pactId}@
    , _peStep :: !Int
        -- ^ the step of a continuation, required for 'PactCont' (0 for
        -- 'PactExec')
    , _peWhy :: !T.Text
        -- ^ why the entry is in the set, and the bound of its cost on a live
        -- node
    , _peSameSnapshotOnly :: !Bool
        -- ^ the entry is generated only with @--same-snapshot@ (both sides
        -- copied from one snapshot, never a production node): its cost has
        -- no known bound on a live node, or its answer depends on the tip
    , _peExpect :: !PactExpect
        -- ^ @expect: success | failure | either@, default success
    }
    deriving (Show, Eq)

newtype PactSet = PactSet { _pactEntries :: [PactEntry] }
    deriving (Show, Eq)

instance FromJSON PactEntry where
    parseJSON = withObject "PactEntry" $ \o -> do
        name <- o .: "name"
        classes <- o .: "classes" >>= mapM (\t -> case requestClassFromText t of
            Just c | c `elem` [ClsLocal, ClsHistory, ClsPreflight] -> return c
            _ -> fail $ "entry " <> T.unpack name <> ": not a Pact request class: " <> T.unpack t)
        kind <- o .:? "kind" .!= ("exec" :: T.Text) >>= \case
            "exec" -> return PactExec
            "cont" -> return PactCont
            k -> fail $ "entry " <> T.unpack name <> ": unknown kind " <> T.unpack k
        step <- case kind of
            PactCont -> o .:? "step" >>= maybe
                (fail $ "entry " <> T.unpack name <> ": kind cont needs a step")
                return
            PactExec -> o .:? "step" >>= \case
                Nothing -> return 0
                Just (_ :: Int) -> fail $ "entry " <> T.unpack name <> ": a step only with kind cont"
        PactEntry name classes kind
            <$> o .: "code"
            <*> pure step
            <*> o .:? "why" .!= ""
            <*> o .:? "sameSnapshotOnly" .!= False
            <*> (o .:? "expect" .!= ("success" :: T.Text) >>= \case
                "success" -> return ExpectSuccess
                "failure" -> return ExpectFailure
                "either" -> return ExpectEither
                e -> fail $ "entry " <> T.unpack name <> ": expect is success, failure or either, not " <> T.unpack e)

instance FromJSON PactSet where
    parseJSON = withObject "PactSet" $ \o -> PactSet <$> o .: "entries"

decodePactSet :: B.ByteString -> Either String PactSet
decodePactSet bs = case Yaml.decodeEither' bs of
    Left e -> Left (Yaml.prettyPrintParseException e)
    Right s -> Right s

-- | The Pact set of @storage-tools/data/pact-set.yaml@. It is held to
-- 'pactSetProblems' like any other set.
--
defaultPactSet :: PactSet
defaultPactSet = either (error . ("Chainweb.StorageTools.Compare.PactSet: data/pact-set.yaml: " <>)) id
    $ decodePactSet $(embedFile "storage-tools/data/pact-set.yaml") >>= checked

-- | Reads a Pact set (@--pact-set FILE@). Fails on a set that does not
-- decode, and on one with any of 'pactSetProblems'.
--
loadPactSet :: FilePath -> IO PactSet
loadPactSet f = B.readFile f
    >>= either (ioError . userError . ((f <> ": ") <>)) return . (decodePactSet >=> checked)

checked :: PactSet -> Either String PactSet
checked s = case pactSetProblems s of
    [] -> Right s
    ps -> Left (T.unpack (T.intercalate "; " ps))

-- | Why a Pact set must not be used, one text per problem; empty if it may.
-- An entry that is not 'sameSnapshotOnly' is sent to a live production
-- oracle, so it fails closed:
--
-- * its @why@ records the bound of its cost after @Bound:@;
-- * its code holds no table scan: no @txids@ or @keylog@ (no upper bound
--   below the live tip), no @select@ or @fold-db@, and @keys@ only on the
--   small @coin.allocation-table@.
--
-- Every continuation, same-snapshot or not, is at step 0: the step check
-- fails before any code of the defpact runs, while step 1 runs user code.
--
pactSetProblems :: PactSet -> [T.Text]
pactSetProblems s = concatMap problems (_pactEntries s)
  where
    problems e = map (\p -> "entry " <> _peName e <> ": " <> p) $ concat
        [ [ "kind cont at step " <> T.pack (show (_peStep e)) <> ", only step 0 is allowed"
          | _peKind e == PactCont, _peStep e /= 0
          ]
        , [ "no \"Bound:\" in its why; an entry without a known bound needs sameSnapshotOnly: true"
          | live e, not ("Bound:" `T.isInfixOf` _peWhy e)
          ]
        , [ "the code calls " <> f <> ", a table scan; it needs sameSnapshotOnly: true"
          | live e, f <- ["txids", "keylog", "select", "fold-db"], calls f (_peCode e)
          ]
        , [ "the code calls keys on a table other than coin.allocation-table; it needs sameSnapshotOnly: true"
          | live e
          , any (not . ("coin.allocation-table)" `T.isPrefixOf`)) (drop 1 (T.splitOn "(keys " (normal (_peCode e))))
          ]
        ]
    live = not . _peSameSnapshotOnly
    calls f code = ("(" <> f <> " ") `T.isInfixOf` normal code

    -- whitespace collapsed to single spaces, none right after an opening
    -- parenthesis
    normal = T.replace "( " "(" . T.unwords . T.words

-- | The entries of a set that may be sent to a live production oracle: those
-- not marked 'sameSnapshotOnly'.
--
liveEntries :: PactSet -> [PactEntry]
liveEntries = filter (not . _peSameSnapshotOnly) . _pactEntries

-- | The entries that serve a request class, in the order of the set.
--
entriesFor :: PactSet -> RequestClass -> [PactEntry]
entriesFor s c = [ e | e <- _pactEntries s, c `elem` _peClasses e ]

-- | The entries that the generator picks from for a request class:
-- @generatedEntries sameSnapshot@ leaves out the entries marked
-- 'sameSnapshotOnly' unless @sameSnapshot@ (ruling C1 on Task 6), so that a
-- run against a live oracle sends only entries with a bounded cost.
--
generatedEntries :: Bool -> PactSet -> RequestClass -> [PactEntry]
generatedEntries sameSnapshot s c =
    [ e | e <- entriesFor s c, sameSnapshot || not (_peSameSnapshotOnly e) ]
