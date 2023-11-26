{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib3
    (
        executeSql,
        Execution,
        ExecutionAlgebra(..),
        JSONserializable(..)
    )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame
import InMemoryTables ( database )
import Data.Time ( UTCTime )
import GHC.Generics
import Data.Aeson
import Data.ByteString.Lazy.Char8 (pack, unpack)
import Data.Char
import Lib2

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

-- We're using a temporary from memory database
executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    case parseStatement sql of
        Left err -> return $ Left err
        Right parsed -> return $ executeStatement parsed InMemoryTables.database

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
