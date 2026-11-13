{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
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
, PactEntry(..)
, PactSet(..)
, defaultPactSet
, decodePactSet
, loadPactSet
, entriesFor
, generatedEntries
) where

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

data PactEntry = PactEntry
    { _peName :: !T.Text
    , _peClasses :: ![RequestClass]
        -- ^ of 'ClsLocal', 'ClsHistory' and 'ClsPreflight'
    , _peKind :: !PactKind
    , _peCode :: !T.Text
        -- ^ the code, or for 'PactCont' the pact id; with the placeholders
        -- @{account}@, @{txid}@ and @{pactId}@
    , _peStep :: !Int
        -- ^ the step of a continuation
    , _peWhy :: !T.Text
        -- ^ why the entry is in the set, and the bound of its cost on a live
        -- node
    , _peSameSnapshotOnly :: !Bool
        -- ^ the entry is generated only with @--same-snapshot@ (both sides
        -- copied from one snapshot, never a production node): its cost has
        -- no known bound on a live node, or its answer depends on the tip
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
        PactEntry name classes kind
            <$> o .: "code"
            <*> o .:? "step" .!= 1
            <*> o .:? "why" .!= ""
            <*> o .:? "sameSnapshotOnly" .!= False

instance FromJSON PactSet where
    parseJSON = withObject "PactSet" $ \o -> PactSet <$> o .: "entries"

decodePactSet :: B.ByteString -> Either String PactSet
decodePactSet bs = case Yaml.decodeEither' bs of
    Left e -> Left (Yaml.prettyPrintParseException e)
    Right s -> Right s

-- | The Pact set of @storage-tools/data/pact-set.yaml@.
--
defaultPactSet :: PactSet
defaultPactSet = either (error . ("Chainweb.StorageTools.Compare.PactSet: data/pact-set.yaml: " <>)) id
    $ decodePactSet $(embedFile "storage-tools/data/pact-set.yaml")

loadPactSet :: FilePath -> IO PactSet
loadPactSet f = B.readFile f >>= either (ioError . userError . ((f <> ": ") <>)) return . decodePactSet

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
