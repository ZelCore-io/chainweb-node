module Main (main) where

import Test.Tasty

import qualified Chainweb.StorageTools.Test.Cold as Cold
import qualified Chainweb.StorageTools.Test.Compare as Compare
import qualified Chainweb.StorageTools.Test.Http as Http
import qualified Chainweb.StorageTools.Test.InProcess as InProcess
import qualified Chainweb.StorageTools.Test.Migrate as Migrate
import qualified Chainweb.StorageTools.Test.RangeDb as RangeDb
import qualified Chainweb.StorageTools.Test.Requests as Requests

main :: IO ()
main = defaultMain $ testGroup "storage-tools"
    [ Compare.tests
    , Requests.tests
    , Http.tests
    , RangeDb.tests
    , Cold.tests
    , InProcess.tests
    , Migrate.tests
    ]
