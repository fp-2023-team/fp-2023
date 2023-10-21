{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame
import InMemoryTables
import Lib1

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = SelectStatement {
        -- Either single max(column_name), sum(column_name) or list of column names
        selectArgs :: [Either (String, [Value] -> Value) String],
        --selectArgs :: [String], --Have doubts if having functions in the statements is not too much
        -- Table names
        -- TODO: ask the lecturer whether onr not there may be multiple tables
        --fromArgs :: [String],
        fromArgs :: String, --One table for now
        -- All 'where' args are column_name0 ?=? column_name1 ORed
        -- TODO: ask the lecturer whether there will be ()
        --whereArgs :: [(String, String, Value -> Value -> Bool)]
        whereArgs :: [(String, String, String -> String -> Bool)] -- No idea how to convert string to value
        --whereArgs :: [String]
    }
    | ShowTableStatement {
        -- Either TABLES or TABLE name[, ...]
        showTableArgs :: Maybe String
    }

-- Functions to implement
max :: [Value] -> Value
max a = a !! 0

sum :: [Value] -> Value
sum a = a !! 0

equal :: String -> String -> Bool
equal a b = False

unequal :: String -> String -> Bool
unequal a b = False

lessOrEqual :: String -> String -> Bool
lessOrEqual a b = False

moreOrEqual :: String -> String -> Bool
moreOrEqual a b = False

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement a = parseStatementList $ parseEndSemicolon a
  where 
    parseStatementList :: String -> Either ErrorMessage ParsedStatement
    parseStatementList [] = Left "No statement found" 
    parseStatementList a = case (parseKeyword a) of
      Left e -> Left e
      Right (x, xs) -> if(parseCompare x "select") 
        then if (xs /= "") then parseSelect xs else Left "Select statement incomplete"
        else if(parseCompare x "show")
          then if (xs /= "") then parseShow xs else Left "Show statement incomplete"
          else Left "Keyword unrecognised"

parseKeyword :: String -> Either ErrorMessage (String, String)
parseKeyword a = if (isTerminating (trpl2 parseResult))
  then Left $ "Unexpected " ++ [(trpl2 parseResult)] ++ " after " ++ (trpl1 parseResult)
  else Right (trpl1 parseResult, trpl3 parseResult)   
  where parseResult = parseWord a

isTerminating :: Char -> Bool
isTerminating a | elem a "(),=<>" = True
isTerminating _ = False

parseSelect :: String -> Either ErrorMessage ParsedStatement
parseSelect [] = Left "Incomplete select statement"
parseSelect x = parseWhereArgs $ parseFromArgs $ parseSelectArgs x
  where
    parseSelectArgs :: String -> Either ErrorMessage (String, ParsedStatement)
    parseSelectArgs [] = Left "Reached unknown state"
    parseSelectArgs a = case (parseWord a) of
      (x, sym, _) | elem sym "=<>)" -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x
      (_, ';', _) -> Left "Missing from statememnt"
      (x, ' ', xs) -> Right (xs, SelectStatement { selectArgs = [Right x]})
      (x, ',', xs) -> case (parseSelectArgs xs) of
        Left e -> Left e
        Right (parseRem, parseRes) -> Right (parseRem, SelectStatement { selectArgs = (((Right x) : (selectArgs parseRes)))})
      (x, '(', xs) -> case (parseWord xs) of
        (x1, ')', xs1)
          | (getTermination xs1) == ' ' -> case (getFunction x) of
              Left e -> Left e
              Right func -> Right (xs1, SelectStatement { selectArgs = [Left (x1, func)]})
          | (getTermination xs1) == ',' -> case (parseSelectArgs (trpl3 (parseWord xs1))) of
              Left e -> Left e
              Right (parseRem, parseRes) -> case (getFunction x) of
                Left e -> Left e
                Right func -> Right (parseRem, SelectStatement { selectArgs = (Left (x1, func)) : (selectArgs parseRes)})
          | otherwise -> Left $ "Unexpected " ++ [getTermination xs1] ++ " after (" ++ x1 ++ ")"
        _ -> Left $ "Too many function arguments in function " ++ x;

    getTermination :: String -> Char
    getTermination [] = ';'
    getTermination (' ':xs) = getTermination xs
    getTermination (x:xs) | elem x ",<>=()" = x
    getTermination (_:xs) = ' '

    parseFromArgs :: Either ErrorMessage (String, ParsedStatement) -> Either ErrorMessage (String, ParsedStatement)
    parseFromArgs (Left e) = Left e
    parseFromArgs (Right (a, b)) = case (parseKeyword a) of
      Left e -> Left e
      Right (x, xs) -> if (parseCompare x "from" && xs /= "")
        then case (parseWord xs) of
          (x1, sym, xs1) | elem sym "; " -> Right (xs1, SelectStatement { selectArgs = (selectArgs b), fromArgs = x1})
          (x1, sym, _) -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x1
        else Left "Missing from statement"

    parseWhereArgs :: Either ErrorMessage (String, ParsedStatement) -> Either ErrorMessage ParsedStatement
    parseWhereArgs (Left e) = Left e
    parseWhereArgs (Right ([], a)) = Right SelectStatement { selectArgs = (selectArgs a), fromArgs = (fromArgs a), whereArgs = []}
    parseWhereArgs (Right (a, b)) = case (parseKeyword a) of
      Left e -> Left e
      Right (x, xs) -> if (parseCompare x "where")
        then parseWhereArgs' (xs, b)
      else Left $ "Unrecognised statement: " ++ x
      where
        parseWhereArgs' :: (String, ParsedStatement) -> Either ErrorMessage ParsedStatement
        parseWhereArgs' (a, b) = case (parseWord a) of
          (x1, sym, _) | elem sym ",()" -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x1
          (x1, sym, _) | elem sym "; " -> Left $ "Missing predicate after " ++ x1
          (x1, '=', xs1) -> case (parseWord xs1) of
            (x2, ';', _) -> Right SelectStatement { selectArgs = (selectArgs b), fromArgs = (fromArgs b), whereArgs = [(x1, x2, equal)] }
            (x2, ' ', xs2) -> case (parseKeyword xs2) of
              Left e -> Left e
              Right (x3, xs3) -> if (parseCompare x3 "or")
                then case (parseWhereArgs' (xs3, b)) of
                  Left e -> Left e
                  Right parseRes -> Right SelectStatement { selectArgs = (selectArgs b), fromArgs = (fromArgs b), whereArgs = (x1, x2, equal) : (whereArgs parseRes)}
                else Left $ "Unrecognised statement: " ++ x3
            (x2, _, _) -> Left $ "Unexpected symbol after " ++ x2
          (x1, '>', xs1) -> if ((xs1 !! 0) == '=')
            then case (parseWord $ tail xs1) of
              (x2, ';', _) -> Right SelectStatement { selectArgs = (selectArgs b), fromArgs = (fromArgs b), whereArgs = [(x1, x2, moreOrEqual)] }
              (x2, ' ', xs2) -> case (parseKeyword xs2) of
                Left e -> Left e
                Right (x3, xs3) -> if (parseCompare x3 "or")
                    then case(parseWhereArgs' (xs3, b)) of
                      Left e -> Left e
                      Right parseRes -> Right SelectStatement { selectArgs = (selectArgs b), fromArgs = (fromArgs b), whereArgs = (x1, x2, moreOrEqual) : (whereArgs parseRes)}
                    else Left $ "Unrecognised statement: " ++ x3
              (x2, _, _) -> Left $ "Unexpected symbol after " ++ x2
            else Left "Unexpected symbol after >"
          (x1, '<', xs1) -> if ((xs1 !! 0) == '=')
            then case (parseWord $ tail xs1) of
              (x2, ';', _) -> Right SelectStatement { selectArgs = (selectArgs b), fromArgs = (fromArgs b), whereArgs = [(x1, x2, lessOrEqual)] }
              (x2, ' ', xs2) -> case (parseKeyword xs2) of
                Left e -> Left e
                Right (x3, xs3) -> if (parseCompare x3 "or")
                    then case(parseWhereArgs' (xs3, b)) of
                      Left e -> Left e
                      Right parseRes -> Right SelectStatement { selectArgs = (selectArgs b), fromArgs = (fromArgs b), whereArgs = (x1, x2, lessOrEqual) : (whereArgs parseRes)}
                    else Left $ "Unrecognised statement: " ++ x3
              (x2, _, _) -> Left $ "Unexpected symbol after " ++ x2
            else if ((xs1 !! 0) == '>')
              then case (parseWord $ tail xs1) of
                (x2, ';', _) -> Right SelectStatement { selectArgs = (selectArgs b), fromArgs = (fromArgs b), whereArgs = [(x1, x2, unequal)] }
                (x2, ' ', xs2) -> case (parseKeyword xs2) of
                  Left e -> Left e
                  Right (x3, xs3) -> if (parseCompare x3 "or")
                    then case(parseWhereArgs' (xs3, b)) of
                      Left e -> Left e
                      Right parseRes -> Right SelectStatement { selectArgs = (selectArgs b), fromArgs = (fromArgs b), whereArgs = (x1, x2, unequal) : (whereArgs parseRes)}
                    else Left $ "Unrecognised statement: " ++ x3
                (x2, _, _) -> Left $ "Unexpected symbol after " ++ x2
              else Left "Unexpected symbol after <"

getFunction :: String -> Either ErrorMessage ([Value] -> Value)
getFunction a = if (parseCompare a "max")
  then Right Lib2.max
  else if (parseCompare a "sum")
    then Right Lib2.sum
    else Left $ "Unrecognised function: " ++ a

trpl1 (a, _, _) = a
trpl2 (_, a, _) = a
trpl3 (_, _, a) = a

parseWord :: String -> (String, Char, String)
parseWord [] = ("", ';', "")
parseWord a = parseWord' (fst $ removeWhitespace a)
  where
    parseWord' :: String -> (String, Char, String)
    parseWord' [] = ("", ';', "")
    parseWord' (x:xs) = if (isTerminating x) 
      then ("", x, xs)
      else if (x /= ' ') then (x : trpl1 parseResult, trpl2 parseResult, trpl3 parseResult)
      else ("", snd unwhitespaced, fst unwhitespaced)
      where parseResult = parseWord' xs
            unwhitespaced = removeWhitespace xs

removeWhitespace :: String -> (String, Char)
removeWhitespace [] = ("", ';')
removeWhitespace (' ':xs) = removeWhitespace xs
removeWhitespace (x:xs) = if (isTerminating x) then (xs, x) else (x:xs, ' ')

parseShow :: String -> Either ErrorMessage ParsedStatement
parseShow [] = Left "Show statement incomplete"
parseShow a = case (parseKeyword a) of
  Left e -> Left e
  Right (x, xs) -> if (parseCompare x "tables")
    then if (xs == "") 
      then Right ShowTableStatement { showTableArgs = Nothing } 
      else Left "Too many arguments in show tables command"
    else if(parseCompare x "table")
      then if (xs /= "") then parseTableName xs else Left "Missing table arguments"
      else Left "Unrecognised show command"

parseTableName :: String -> Either ErrorMessage ParsedStatement
parseTableName a = case (parseWord a) of
  (result, ';', _) -> Right ShowTableStatement { showTableArgs = Just result}
  _ -> Left "Too many arguments in show table statement"

parseEndSemicolon :: String -> String
parseEndSemicolon [] = ""
parseEndSemicolon (x:xs) = if (x == ';') then "" else x : parseEndSemicolon xs

parseCompare :: String -> String -> Bool
parseCompare [] [] = True
parseCompare (x:xs) (a:ab) = if (toLower(x) == toLower(a)) then parseCompare xs ab else False
parseCompare _ _ = False

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (SelectStatement selectArgs fromArgs whereArgs) = case table of
    Nothing -> Left "Could not find table"
    Just (DataFrame columns rows) -> case selectArgs of
        list@(Left _:_) -> if True then
                Left $ "Column selection and table column mismatch"
            else
                Left $ "Function select not implemented yet"
        list@(Right _:_) -> if True then
                Left $ "Column selection and table column mismatch"
            else
                Left $ "Column select not implemented yet"
        _ -> Left "Got an empty select list"
    where
        table :: Maybe DataFrame
        table = findTableByName database fromArgs
executeStatement (ShowTableStatement showTableArgs) = case showTableArgs of
    Nothing -> Right $ DataFrame
        [ Column "table name" StringType ]
        [ [ StringValue (fst table) ] | table <- database ]
    Just tableName -> maybe (Left $ "Could not find table " ++ tableName)
        (\value -> Right value)
        (findTableByName database tableName)
