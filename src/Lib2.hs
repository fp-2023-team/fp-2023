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
parseStatement a = if(parseCompare (parseWord a) "select" || parseCompare (parseWord a) "show") then Left "Greaghcit" else Left "Bad"

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
