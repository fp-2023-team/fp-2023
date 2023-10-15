{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (DataFrame)
import InMemoryTables (TableName)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  deriving (Show, Eq)

-- Keep the type, modify constructors
data ParsedStatement = SelectStatement {
        -- Either single max(column_name), sum(column_name) or list of column names
        selectArgs :: Either (String, [Value] -> Value) [String],
        -- Table names
        -- TODO: ask the lecturer whether onr not there may be multiple tables
        fromArgs :: [String],
        -- All 'where' args are column_name0 ?=? column_name1 ORed
        -- TODO: ask the lecturer whether there will be ()
        whereArgs :: [(String, String, Value -> Value -> Bool)]
    }
    | ShowTableStatement {
        -- Either TABLES or TABLE name[, ...]
        showTableArgs :: Maybe [String]
    }


-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement a = parseStatementList $ parsePhrase $ parseEndSemicolon a
  where 
    parseStatementList :: [String] -> Either ErrorMessage ParsedStatement
    parseStatementList [] = Left "Bad"
    parseStatementList (x:xs) = 
      if(parseCompare x "select") 
        then Left "Parse Select not implemented"--parseSelect xs  
        else if(parseCompare x "show")
          then parseShow xs 
          else Left "Keyword unrecognised"

--parseSelect :: [String] -> Either ErrorMessage ParsedStatement
--parseSelect [] = Left "Error: incomplete select statement"
--parseSelect x = 

parseShow :: [String] -> Either ErrorMessage ParsedStatement
parseShow [] = Left "Show statement incomplete"
parseShow (x:xs) =
  if(parseCompare x "tables")
    then Right ShowTableStatement { showTableArgs = Nothing }
    else if(parseCompare x "table")
      then parseTableName xs
      else Left "Unrecognised show command"

parseTableName :: [String] -> Either ErrorMessage ParsedStatement
parseTableName (a : _) = Right ShowTableStatement { showTableArgs = Just [(parseCommas [a]) !! 0]}
parseTableName _ = Left "Incorrect table name input"

parseCommas :: [String] -> [String]
parseCommas [] = []
parseCommas [x] = parseCommaPhrase x
parseCommas (x : xs) = (parseCommaPhrase x) ++ (parseCommas xs)

parseCommaPhrase :: String -> [String]
parseCommaPhrase x = removeEmpty $ parsePhrase $ parseComma x
  where
    removeEmpty :: [String] -> [String]
    removeEmpty [] = []
    removeEmpty (x : xs) = if (x == "") then removeEmpty xs else x : removeEmpty xs

parseComma :: String -> String
parseComma [] = []
parseComma (x : xs) = if (x == ',') then ' ' : (parseComma xs) else x : (parseComma xs)

parseEndSemicolon :: String -> String
parseEndSemicolon [] = ""
parseEndSemicolon (x:xs) = if (x == ';') then "" else x : parseEndSemicolon xs

parsePhrase :: String -> [String]
parsePhrase [] = []
parsePhrase stringy = (parseWord stringy) : parsePhrase(removeWord stringy)

removeWord :: String -> String
removeWord [] = []
removeWord (x:xs) = if (x == ' ') then xs else removeWord xs

parseCompare :: String -> String -> Bool
parseCompare [] [] = True
parseCompare (x:xs) (a:ab) = if (toLower(x) == toLower(a)) then parseCompare xs ab else False
    where
      toLower :: Char -> Char
      toLower ch
        | elem ch ['A'..'Z'] = toEnum $ fromEnum ch + 32
        | otherwise = ch
parseCompare _ _ = False

parseWord :: String -> String
parseWord [] = ""
parseWord (a:xs) = if (a == ' ') then "" else a : parseWord xs

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented: executeStatement"
