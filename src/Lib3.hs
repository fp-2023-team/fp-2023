{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib3
    (
        executeSql,
        serialize,
        deserialize,
        Execution,
        ExecutionAlgebra(..),
        JSONserializable(..),
        convertDF
    )
where

import Control.Monad.Free (Free (..), liftF, retract)
import DataFrame
import InMemoryTables ( database )
import Data.Time ( UTCTime, getCurrentTime )
import GHC.Generics
import Data.Aeson
import Data.ByteString.Lazy.Char8 (pack, unpack)
import Data.Char
import Lib2
import Data.Either (Either(Right))
import EitherT
import Control.Monad.Trans.State.Strict

type TableName = String
type TableContent = String

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
  parsed <- Pure $ parseStatement sql
  fixedParsed <- 
    let eitherRes = runState (runEitherT parsed) ""
    in case (eitherRes) of
      (Left err, _) -> Pure $ Left err
      (Right value, _) -> Pure $ Right value
  database <- getRelevantTables ["duplicates", "employees", "flags", "invalid1", "invalid2", "long_strings", "jobs"]
  time <- getTime
  executionResult <- case(fixedParsed) of
    Left e -> return $ Left e 
    Right parsedStmt -> case(database) of
      Left e -> return $ Left e
      Right exist -> case findNow parsedStmt of
        False -> Pure $ executeStatement parsedStmt exist
        True -> Pure $ executeStatement (changeNow parsedStmt) (timeTable time:exist)
  case (executionResult) of
    Left e -> return $ Left e
    Right result -> case (fixedParsed) of
      Right (SelectStatement _ _ _) -> return $ Right result
      Right (ShowTableStatement _) -> return $ Right result
      Right (InsertIntoStatement tablename _ _) -> do 
        persistTable tablename result
        return $ Right result
      Right (UpdateStatement tablename _ _) -> do
        persistTable tablename result
        return $ Right result
      Right (DeleteStatement tablename _) -> do
        persistTable tablename result
        return $ Right result

getRelevantTables :: [TableName] -> Execution (Either ErrorMessage [(TableName, DataFrame)])
getRelevantTables [] = do
  Pure $ Right []
getRelevantTables (x:xs) = do
  table <- getTable x
  case table of
    Left e -> return $ Left e
    Right goodTable -> do
      otherTables <- getRelevantTables xs
      case otherTables of
        Left e -> return $ Left e
        Right goodTables -> Pure $ Right (goodTable : goodTables)

persistTable :: TableName -> DataFrame -> Execution ()
persistTable name duom = do
  serial <- Pure $ serialize duom
  saveTable name serial

getTable :: TableName -> Execution (Either ErrorMessage (TableName, DataFrame))
getTable name = do
  table <- loadTable name
  deserializedTable1 <- Pure $ deserialize table 
  case (deserializedTable1) of
    Nothing -> return $ Left $ "Failed to deserialize table \"" ++ name ++ "\""
    Just a -> Pure $ Right (name, a)

findNow :: ParsedStatement -> Bool
findNow (SelectStatement a _ _) = findNow' a
  where
    findNow' :: [Either ([(Maybe String, String)], Lib2.Function) (Maybe String, String)] -> Bool
    findNow' [] = False
    findNow' (x:xs) = case x of
      Left (_, Func0 _) -> True
      _ -> findNow' xs
findNow _ = False

timeTable :: UTCTime -> (TableName, DataFrame)
timeTable time = ("datetime", DataFrame [(Column "datetime" StringType)] [[StringValue $ show time]])

changeNow :: ParsedStatement -> ParsedStatement
changeNow (SelectStatement a b c) = (SelectStatement (map changeNow' a) ("datetime":b) c)
  where
    changeNow' :: Either ([(Maybe String, String)], Lib2.Function) (Maybe String, String) -> Either ([(Maybe String, String)], Lib2.Function) (Maybe String, String)
    changeNow' (Left (_, Func0 _)) = Right (Just "datetime", "datetime")
    changeNow' other = other
changeNow notSelect = notSelect
  
class JSONserializable a where
  serialize :: a -> String
  deserialize :: String -> Maybe a

data ColumnType'
  = IT
  | ST
  | BT
  deriving (Show, Eq, Generic)

data Column' = C String ColumnType'
  deriving (Show, Eq, Generic)

data Value'
  = IV Integer
  | SV String
  | BV Bool
  | NV
  deriving (Show, Eq, Generic)

type Row' = [Value']

data DataFrame' = DF [Column'] [Row']
  deriving (Show, Eq, Generic)

convertDF :: DataFrame -> DataFrame'
convertDF (DataFrame a b) = DF (map convertC a) (map (map convertV) b)

convertC :: Column -> Column'
convertC (Column a b) = (C a (convertCT b))

convertCT :: ColumnType -> ColumnType'
convertCT IntegerType = IT
convertCT StringType = ST
convertCT BoolType = BT

convertV :: DataFrame.Value -> Value'
convertV (IntegerValue a) = IV a
convertV (StringValue a) = SV a
convertV (BoolValue a) = BV a
convertV NullValue = NV

unconvertDF :: DataFrame' -> DataFrame
unconvertDF (DF a b) = DataFrame (map unconvertC a) (map (map unconvertV) b)

unconvertC :: Column' -> Column
unconvertC (C a b) = (Column a (unconvertCT b))

unconvertCT :: ColumnType' -> ColumnType
unconvertCT IT = IntegerType
unconvertCT ST = StringType
unconvertCT BT = BoolType

unconvertV :: Value' -> DataFrame.Value
unconvertV (IV a) = IntegerValue a
unconvertV (SV a) = StringValue a
unconvertV (BV a) = BoolValue a
unconvertV NV = NullValue

instance FromJSON ColumnType' where
instance FromJSON Column' where
instance FromJSON DataFrame' where
instance FromJSON Value' where
instance ToJSON ColumnType' where
instance ToJSON Column' where
instance ToJSON DataFrame' where
instance ToJSON Value' where

instance JSONserializable DataFrame where
  serialize a = serialize $ convertDF a
  deserialize a = case (decode $ pack a) of
    Just b -> Just (unconvertDF b)
    Nothing -> Nothing

instance JSONserializable DataFrame' where
  serialize (DF a b) = "[[" ++ concat (mapComma (map serialize a)) ++ "],[" ++ concat (mapComma (map serialize b)) ++ "]]"
  deserialize a = decode $ pack a

instance JSONserializable Column' where
  serialize (C a b) = "[" ++ serialize a ++ "," ++ serialize b ++ "]"
  deserialize a = decode $ pack a

instance JSONserializable ColumnType' where
  serialize ST = "\"ST\""
  serialize IT = "\"IT\""
  serialize BT = "\"BT\""
  deserialize a = decode $ pack a

instance JSONserializable Row' where
  serialize a = "[" ++ concat (mapComma (map serialize a)) ++ "]"
  deserialize a = decode $ pack a

instance JSONserializable Value' where
  serialize (IV a) = "{\"contents\":" ++ show a ++ ",\"tag\":\"IV\"}"
  serialize (SV a) = "{\"contents\":" ++ show a ++ ",\"tag\":\"SV\"}"
  serialize (BV a) = "{\"contents\":" ++ map toLower (show a) ++ ",\"tag\":\"BV\"}"
  serialize (NV) = "{\"tag\":\"NV\"}"
  deserialize a = decode $ pack a

instance JSONserializable String where
  serialize a = show a
  deserialize a = decode $ pack a

mapComma :: [String] -> [String]
mapComma [] = []
mapComma (x:[]) = (x:[])
mapComma (x:xs) = ((x ++ ","):mapComma xs)
