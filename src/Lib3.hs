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
import InMemoryTables ( tableEmployees )
import Data.Time ( UTCTime )
import Text.JSON.Generic

type TableName = String
type TableContent = String
type ErrorMessage = String

data ExecutionAlgebra next
    = GetTime (UTCTime -> next)
    | SaveTable TableName TableContent (() -> next)
    | LoadTable TableName (TableContent -> next)
    -- feel free to add more constructors here
    deriving Functor

type Execution = Free ExecutionAlgebra

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

saveTable :: TableName -> TableContent -> Execution ()
saveTable name content = liftF $ SaveTable name content id

loadTable :: TableName -> Execution TableContent
loadTable name = liftF $ LoadTable name id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    return $ Left "implement me"
    a <- loadTable "foo"
    return $ Left a
