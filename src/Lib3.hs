{-# LANGUAGE DeriveFunctor #-}

module Lib3
    (
        executeSql,
        Execution,
        ExecutionAlgebra(..)
    )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame)
import InMemoryTables (database)
import Data.Time ( UTCTime )
import Text.JSON.Generic

type TableName = String
type TableContent = String
type ErrorMessage = String

data ExecutionAlgebra next
    = GetTime (UTCTime -> next)
    | SaveDatabase TableContent (() -> next)
    | LoadDatabase (TableContent -> next)
    -- feel free to add more constructors here
    deriving Functor

type Execution = Free ExecutionAlgebra

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

saveDatabase :: TableContent -> Execution ()
saveDatabase content = liftF $ SaveDatabase content id

loadDatabase :: Execution TableContent
loadDatabase = liftF $ LoadDatabase id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    return $ Left "implement me"


-- TODO: implement this; for now it returns an empty string
databaseToJson :: [(TableName, DataFrame)] -> TableContent
databaseToJson _ = ""
