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
data ParsedStatement = ParsedStatement

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement a = parseStatementList $ parsePhrase a
  where 
    parseStatementList :: [String] -> Either ErrorMessage ParsedStatement
    parseStatementList [] = Left "Bad"
    parseStatementList (x:xs) = 
      if(parseCompare x "select") 
        then Left "Parse Select not implemented"--parseSelect xs  
        else if(parseCompare x "show")
          then parseShow xs 
          else Left "Keyword unrecognised"
    
parseShow :: [String] -> Either ErrorMessage ParsedStatement
parseShow [] = Left "Show statement incomplete"
parseShow (x:xs) =
  if(parseCompare x "tables")
    then Left "Show tables statement not implemented" -- TODO: add SHOW TABLES to parsed statement
    else if(parseCompare x "table")
      then parseTableName xs
      else Left "Unrecognised show command"

parseTableName :: [String] -> Either ErrorMessage ParsedStatement
parseTableName [a] = Left "Parse table name not implemented" --TODO: add SHOW TABLE <name> to parsed statement
parseTableName _ = Left "Incorrect table name input"

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
