module Main (main) where

import Test.Tasty

import qualified Chainweb.StorageTools.Test.Compare as Compare
import qualified Chainweb.StorageTools.Test.Http as Http
import qualified Chainweb.StorageTools.Test.RangeDb as RangeDb

main :: IO ()
main = defaultMain $ testGroup "storage-tools"
    [ Compare.tests
    , Http.tests
    , RangeDb.tests
    ]
