{-# LANGUAGE FlexibleInstances #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    WhereOperand (..)
  )
where

import Data.Either
import Data.List
import DataFrame
import InMemoryTables
import Lib1
import Data.Either (Either(Right))

type ErrorMessage = String
type Database = [(TableName, DataFrame)]
type WhereOperator = (String -> String -> Bool)

-- Keep the type, modify constructors
data ParsedStatement = SelectStatement {
        -- Either single max(column_name), sum(column_name) or list of column names
        selectArgs :: [Either (String, [Value] -> Value) String],
        -- Table names
        --fromArgs :: [String],
        fromArgs :: String, --One table for now
        -- All 'where' args are column_name0 ?=? column_name1 ORed
        --whereArgs :: [(String, String, Value -> Value -> Bool)]
        whereArgs :: [(WhereOperand, WhereOperand, WhereOperator)]
    }
    | ShowTableStatement {
        -- Either TABLES or TABLE name[, ...]
        showTableArgs :: Maybe String
    }
    deriving (Eq)

data WhereOperand = Constant String | ColumnName String deriving Show

-- Functions to implement
max :: [Value] -> Value
max values@(value:_) = case value of
    IntegerValue _ -> maxInteger values
    StringValue _ -> maxString values
    BoolValue _ -> maxBool values
    _ -> NullValue
    where
        maxInteger :: [Value] -> Value
        maxInteger values = do
            let values' = [value | IntegerValue value <- values]
            if null values' then
                NullValue
            else
                IntegerValue $ maximum values'
        maxString :: [Value] -> Value
        maxString values = do
            let values' = [value | StringValue value <- values]
            if null values' then
                NullValue
            else
                StringValue $ maximum values'
        maxBool :: [Value] -> Value
        maxBool values = do
            let values' = [value | StringValue value <- values]
            if null values' then
                NullValue
            else
                StringValue $ maximum values'

sum :: [Value] -> Value
sum values@(value:_) = case value of
    IntegerValue _ -> sumInteger values
    _ -> NullValue
    where
        sumInteger :: [Value] -> Value
        sumInteger values = do
            let values' = [value | IntegerValue value <- values]
            if null values' then
                NullValue
            else
                IntegerValue $ Data.List.sum values'

equal :: String -> String -> Bool
equal = (==)

unequal :: String -> String -> Bool
unequal = (/=)

lessOrEqual :: String -> String -> Bool
lessOrEqual = (<=)

moreOrEqual :: String -> String -> Bool
moreOrEqual = (>=)

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement a = parseStatement' $ normaliseString $ parseEndSemicolon a
  where 
    parseStatement' :: String -> Either ErrorMessage ParsedStatement
    parseStatement' [] = Left "No statement found" 
    parseStatement' a = do
      (x, xs) <- parseKeyword a
      result <- if(parseCompare x "select")
        then if (xs /= "") then parseSelect xs else Left "Select statement incomplete"
        else if(parseCompare x "show")
          then if (xs /= "") then parseShow xs else Left "Show statement incomplete"
          else Left "Keyword unrecognised"
      return(result)

normaliseString :: String -> String
normaliseString [] = []
normaliseString (x:xs) = ((normaliseChar x):(normaliseString xs))
  where 
    normaliseChar :: Char -> Char
    normaliseChar x | elem x "\n\t" = ' '
    normaliseChar x = x

parseKeyword :: String -> Either ErrorMessage (String, String)
parseKeyword a = if (isTerminating (trpl2 parseResult))
  then if (trpl2 parseResult /= '\'')
    then Left $ "Unexpected " ++ [(trpl2 parseResult)] ++ " after " ++ (trpl1 parseResult)
    else Right (trpl1 parseResult, '\'':(trpl3 parseResult))  
  else Right (trpl1 parseResult, trpl3 parseResult)   
  where parseResult = parseWord a

isTerminating :: Char -> Bool
isTerminating a | elem a "(),=<>'" = True
isTerminating _ = False

parseSelect :: String -> Either ErrorMessage ParsedStatement
parseSelect [] = Left "Incomplete select statement"
parseSelect x = do
  x1 <- parseSelectArgs x
  x2 <- parseFromArgs x1
  result <- parseWhereArgs x2
  return(result)
  where
    parseSelectArgs :: String -> Either ErrorMessage (String, ParsedStatement)
    parseSelectArgs [] = Left "Reached unknown state"
    parseSelectArgs a = case (parseWord a) of
      (x, sym, _) | x == "" -> Left $ "Unexpected " ++ [sym]
                  | (elem sym "=<>)'") -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x
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
    getTermination (x:xs) | isTerminating x = x
    getTermination (_:xs) = ' '

    parseFromArgs :: (String, ParsedStatement) -> Either ErrorMessage (String, ParsedStatement)
    parseFromArgs (a, b) = case (parseKeyword a) of
      Left e -> Left e
      Right (x, xs) -> if (parseCompare x "from" && xs /= "")
        then case (parseWord xs) of
          (x1, sym, xs1) | elem sym "; " -> Right (xs1, SelectStatement { selectArgs = (selectArgs b), fromArgs = x1})
          (x1, sym, _) | x1 == ""  -> Left $ "Unexpected " ++ [sym]
                       | otherwise -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x1
        else Left "Missing from statement"

    parseWhereArgs :: (String, ParsedStatement) -> Either ErrorMessage ParsedStatement
    parseWhereArgs ([], a) = Right SelectStatement { selectArgs = (selectArgs a), fromArgs = (fromArgs a), whereArgs = []}
    parseWhereArgs (a, b) = case (parseKeyword a) of
      Left e -> Left e
      Right (x, xs) -> if (parseCompare x "where")
        then if (xs /= "") then parseWhereArgs' (xs, b) else Left "Missing where statement"
        else Left $ "Unrecognised statement: " ++ x
      where
        parseWhereArgs' :: (String, ParsedStatement) -> Either ErrorMessage ParsedStatement
        parseWhereArgs' (a, b) = case (parseWord a) of
          (x, '\'', xs) | x == "" -> case (parseConstant xs) of
                          ("", _) -> Left "Constant can not be emtpy"
                          (x1, xs1) -> case (parseWord xs1) of
                            (_, '\'', _) -> Left $ "Unexpected \' after " ++ x
                            ("", sym, xs2) -> parseOperator (Constant x1, sym, xs2) b
                            (x2, _, _) -> Left $ "Unexpected " ++ x2 ++ " after " ++ x1 
                        | otherwise -> Left $ "Unexpected \' after " ++ x  
          (x, sym, xs) -> parseOperator (ColumnName x, sym, xs) b
          where
            parseOperator :: (WhereOperand, Char, String) -> ParsedStatement -> Either ErrorMessage ParsedStatement
            parseOperator (x1, sym, _) _ | elem sym ",()" = Left $ "Unexpected " ++ [sym] ++ " after " ++ show x1
                                         | show x1 == "" = Left $ "Unexpected " ++ [sym]
                                         | elem sym "; " = Left $ "Missing predicate after " ++ show x1
            parseOperator (x1, '=', xs1) b = parseSecondOperand equal b (x1, xs1)
            parseOperator (x1, '>', xs1) b = if (xs1 /= "" && (xs1 !! 0) == '=')
              then parseSecondOperand moreOrEqual b (x1, tail xs1)
              else Left "Unexpected symbol after >"
            parseOperator (x1, '<', xs1) b = if (xs1 /= "" && (xs1 !! 0) == '=')
              then parseSecondOperand lessOrEqual b (x1, tail xs1)
              else if (xs1 /= "" && (xs1 !! 0) == '>')
                then parseSecondOperand unequal b (x1, tail xs1)
                else Left "Unexpected symbol after <"

        parseSecondOperand :: (String -> String -> Bool) -> ParsedStatement -> (WhereOperand, String) -> Either ErrorMessage ParsedStatement
        parseSecondOperand func statement (a, ab) = case (parseWord ab) of
          (x, '\'', xs) 
            | x == "" -> case (parseConstant xs) of
              ("", _) -> Left "Constant can not be emtpy"
              (x1, xs1) -> case (parseWord xs1) of
                (x2, ';', _)  | x2 == "" -> Right SelectStatement { selectArgs = (selectArgs statement), fromArgs = (fromArgs statement), whereArgs = [(a, Constant x1, func)] }
                              | parseCompare x2 "or" -> Left $ "Missing statement after " ++ x2 
                              | otherwise -> Left $ "Unexpected " ++ x2 ++ " after " ++ x1  
                (x2, ' ', xs2) | parseCompare x2 "or" -> if (xs2 /= "")
                                  then case (parseWhereArgs' (xs2, statement)) of
                                    Left e -> Left e
                                    Right parseRes -> Right SelectStatement { selectArgs = (selectArgs statement), fromArgs = (fromArgs statement), whereArgs = (a, Constant x1, func) : (whereArgs parseRes)}
                                  else Left $ "Missing statement after " ++ x2
                               | otherwise -> Left $ "Unrecognised statement: " ++ x1
                (x2, '\'', xs2) | parseCompare x2 "or" -> if (xs2 /= "")
                                  then case (parseWhereArgs' ('\'':xs2, statement)) of
                                    Left e -> Left e
                                    Right parseRes -> Right SelectStatement { selectArgs = (selectArgs statement), fromArgs = (fromArgs statement), whereArgs = (a, Constant x1, func) : (whereArgs parseRes)}
                                  else Left $ "Missing statement after " ++ x2
                               | otherwise -> Left $ "Unrecognised statement: " ++ x1
                (x2, sym, _) -> Left $ "Mega Unexpected " ++ [sym] ++ " after " ++ x2
            | otherwise -> Left $ "Unexpected \' after " ++ x  
          (x, ';', _) | x /= "" -> Right SelectStatement { selectArgs = (selectArgs statement), fromArgs = (fromArgs statement), whereArgs = [(a, ColumnName x, func)] }
                      | otherwise -> Left "Missing second operand"
          (x, ' ', xs) -> case (parseKeyword xs) of
            Left e -> Left e
            Right (x1, xs1) -> if (parseCompare x1 "or")
              then if (xs1 /= "")
                then case (parseWhereArgs' (xs1, statement)) of
                  Left e -> Left e
                  Right parseRes -> Right SelectStatement { selectArgs = (selectArgs statement), fromArgs = (fromArgs statement), whereArgs = (a, ColumnName x, func) : (whereArgs parseRes)}
                else Left $ "Missing statement after " ++ x1 
              else Left $ "Unrecognised statement: " ++ x1
          (x, sym, _) -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x

parseConstant :: String -> (String, String)
parseConstant [] = ("", "")
parseConstant (x:xs) = if (x == '\'') then ("", xs) else (x : (fst parseResult), snd parseResult)
  where parseResult = parseConstant xs

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
      else ("", snd unwhitespaced, removeCharIfTerminating (fst unwhitespaced))
      where parseResult = parseWord' xs
            unwhitespaced = removeWhitespace xs

removeCharIfTerminating :: String -> String
removeCharIfTerminating [] = []
removeCharIfTerminating (x:xs) = if(isTerminating x) then xs else x:xs 

removeWhitespace :: String -> (String, Char)
removeWhitespace [] = ("", ';')
removeWhitespace (' ':xs) = removeWhitespace xs
removeWhitespace (x:xs) = if (isTerminating x) then (x:xs, x) else (x:xs, ' ')

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
executeStatement (SelectStatement selectArgs' fromArgs' whereArgs') = case findTableByName database fromArgs' of
    Nothing -> Left $ "Could not find table " ++ fromArgs'
    -- If the table was found,
    Just table@(DataFrame columns rows) ->
        -- Ensure we have some select columns
        if (null selectColumnNames) then
            Left $ "Got zero columns to select"
        -- Ensure select only has selection by column value or only by function
        else if (any (isLeft) selectArgs' && any(isRight) selectArgs') then
            Left $ "Cannot select by column value and by function at the same time"
        -- Wildcard only once
        else if (let (x:xs) = selectColumnNames in x == "*" && not (null xs)) then
            Left $ "Can only select all columns once"
        -- Cannot use wildcard with functions
        else if (let (x:xs) = selectColumnNames in x == "*" && not (null (getOnlyLefts selectArgs'))) then
            Left $ "Cannot apply function to wildcard"
        ---- Check if the column names mentioned in the selection arguments actually exist,
        ---- fall-through for wildcard
        else if ((intersect selectColumnNames tableColumnNames /= selectColumnNames)
                        && not (let (x:xs) = selectColumnNames in x == "*" && null xs)
                || (intersect whereColumnNames tableColumnNames /= whereColumnNames))
        then
            Left $ "Statement references columns which do not exist in table"
        -- Ensure comparisons are done between string type columns and constants only
        else if (any
                (\(operand1, operand2, _) -> (not $ whereColumnIsStringType columns operand1)
                    || (not $ whereColumnIsStringType columns operand2))
                whereArgs') then
            Left $ "Only comparisons between string type columns and constants are allowed in `where`"
        -- Finally print the table
        else do
            let DataFrame columns' rows' = createFilteredTable table whereArgs'
            let columnValues = combineColumnsWithValues columns' (if null rows' then [map (\x -> NullValue) columns'] else rows') -- [(Column, [Value])]
            -- Check for wildcard, too
            let selectArgs'' = let (x:_) = selectColumnNames in if x == "*" then [Right columnName | columnName <- tableColumnNames] else selectArgs'
            case selectArgs'' of
                -- If selectArgs'' is Left, then it's (String, [Value] -> Value)
                (Left _:_) -> do
                    let columnNamesAndFunctions = getOnlyLefts selectArgs''
                    -- Check if column names match and apply functions
                    let resultColumnValues = [(Column colName colType, [func $ values]) | (name, func) <- columnNamesAndFunctions, (Column colName colType, values) <- columnValues, name == colName]
                    let resultColumnsRows = uncombineColumnsFromValues resultColumnValues
                    Right $ DataFrame (fst resultColumnsRows) (snd resultColumnsRows)
                -- If selectArgs'' is Right, then it's only String
                (Right _:_) -> do
                    let columnNames = getOnlyRights selectArgs''
                    -- Check if column names match
                    let resultColumnValues = [columnValue | columnName <- columnNames, columnValue@(Column name _, _) <- columnValues, columnName == name]
                    let resultColumnsRows = uncombineColumnsFromValues resultColumnValues
                    Right $ DataFrame (fst resultColumnsRows) (snd resultColumnsRows) 
                _ -> Left $ "Empty select list"
        where
            tableColumnNames :: [String]
            tableColumnNames = [ columnName | Column columnName _ <- columns ]
            selectColumnNames :: [String]
            selectColumnNames = map (getSelectColumnName) selectArgs'
                where
                    getSelectColumnName :: Either (String, [Value] -> Value) String -> String
                    getSelectColumnName (Right str) = str
                    getSelectColumnName (Left (str, _)) = str
            whereColumnNames :: [String]
            whereColumnNames = concat [[a | (ColumnName a, _, _) <- whereArgs'],
                [b | (_, ColumnName b, _) <- whereArgs']]
executeStatement (ShowTableStatement showTableArgs') = case showTableArgs' of
    Nothing -> Right $ DataFrame
        [ Column "table_name" StringType ]
        [ [ StringValue (fst table) ] | table <- database ]
    Just tableName -> maybe (Left $ "Could not find table " ++ tableName)
        (\(DataFrame cols _) -> Right $ DataFrame
            [Column "column_name" StringType, Column "column_type" StringType]
            [[StringValue name, StringValue $ show colType] | Column name colType <- cols])
        (findTableByName database tableName)

whereColumnIsStringType :: [Column] -> WhereOperand -> Bool
whereColumnIsStringType columns (ColumnName columnName) = getColumnTypeByName columns columnName == Just StringType
    where
        getColumnTypeByName :: [Column] -> String -> Maybe ColumnType
        getColumnTypeByName cols name = do
            (Column _ colType) <- find (\(Column colName _) -> colName == name) cols
            return colType
whereColumnIsStringType _ _ = True

nullOrAny :: (Foldable t) => (a -> Bool) -> t a -> Bool
nullOrAny f x = null x || any f x

-- Applies where filters and column selection to a table, creating a new table
createFilteredTable :: DataFrame
    -> [(WhereOperand, WhereOperand, String -> String -> Bool)]
    -> DataFrame
createFilteredTable (DataFrame columns rows) whereArgs' = DataFrame columns filteredRows
    where
        -- Give row values their column name
        namedRows :: [[(String, Value)]]
        namedRows = [zip [columnName | (Column columnName _) <- columns] row | row <- rows]
        -- Apply where filter to rows
        filteredNamedRows :: [[(String, Value)]]
        filteredNamedRows = [ namedValues | namedValues <- namedRows,
            nullOrAny
                (\(whereOperand1, whereOperand2, operator) -> operator
                    (getStringValue namedValues whereOperand1)
                    (getStringValue namedValues whereOperand2))
                whereArgs']
            where
                getStringValue :: [(String, Value)] -> WhereOperand -> String
                getStringValue _ (Constant string) = string
                getStringValue namedValues (ColumnName name) = case lookup name namedValues of
                    Just (StringValue string) -> string
                    _ -> ""
        -- Finally remove column names from row values
        filteredRows :: [Row]
        filteredRows = [snd $ unzip namedValues | namedValues <- filteredNamedRows]


-- Culls a list of Either from Right elements
getOnlyLefts :: [Either a b] -> [a]
getOnlyLefts list = [val | Left val <- list]

-- Culls a list of Either from Left elements
getOnlyRights :: [Either a b] -> [b]
getOnlyRights list = [val | Right val <- list]

-- Zips row values by column
combineColumnsWithValues :: [a] -> [[b]] -> [(a, [b])]
combineColumnsWithValues columns rows = zip columns $ transpose rows

-- Unzips row values by column
uncombineColumnsFromValues :: [(a, [b])] -> ([a], [[b]])
uncombineColumnsFromValues mapped = (fst $ unzip mapped, transpose $ snd $ unzip mapped)

instance Eq ([Value] -> Value) where
  a == b = True
  a /= b = False

instance Eq (String -> String -> Bool) where
  a == b = True
  a /= b = False

instance Eq WhereOperand where
  Constant a == Constant b = a == b
  ColumnName a == ColumnName b = a == b
  Constant _ == ColumnName _ = False
  ColumnName _ == Constant _ = False
  a /= b = not (a == b)