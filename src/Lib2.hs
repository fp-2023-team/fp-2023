{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import Data.Either
import Data.List
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
executeStatement (SelectStatement selectArgs' fromArgs' whereArgs') = case findTableByName database fromArgs' of
    Nothing -> Left $ "Could not find table " ++ fromArgs'
    -- If the table was found,
    Just table@(DataFrame columns _) ->
        -- Ensure select only has selection by column value or only by function
        if (any (isLeft) selectArgs' && any(isRight) selectArgs') then
            Left $ "Cannot select by column value and by function at the same time"
        -- Check if the column names mentioned in the selection arguments actually exist
        else if (intersect selectColumnNames tableColumnNames /= selectColumnNames)
                || (intersect whereColumnNames tableColumnNames /= whereColumnNames) then
            Left $ "Statement references columns which do not exist in table"
        -- Ensure comparisons are done between string type columns only
        else if (any
                (\(columnName1, columnName2, _) -> getColumnTypeByName columns columnName1 /= Just StringType
                    || getColumnTypeByName columns columnName2 /= Just StringType)
                whereArgs') then
            Left $ "Only comparisons between string type columns are allowed in `where`"
        -- Finally print the table
        else do
            let selectionTable = createFilteredTable table selectColumnNames whereArgs'
            case selectArgs' of
                -- If selectArgs' is Left, then it's (String, [Value] -> Value)
                (Left _:_) -> do
                    let DataFrame filteredCols filteredRows = selectionTable
                    let functionsToApply = map (snd) (getOnlyLefts selectArgs')
                    let transposedRows = transpose filteredRows
                    let mappedRows = [ mapByIndex functionsToApply transposedRows ]
                    Right $ DataFrame filteredCols mappedRows
                -- If selectArgs' is Right, then it's only String
                (Right _:_) -> Right selectionTable
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
            whereColumnNames = getWhereColumnNames whereArgs' []
                where
                    getWhereColumnNames :: [(String, String, String -> String -> Bool)]
                            -> [String] -> [String]
                    getWhereColumnNames [] result = result
                    getWhereColumnNames ((columnName1, columnName2, _):xs) result =
                        getWhereColumnNames xs (columnName1:columnName2:result)
executeStatement (ShowTableStatement showTableArgs') = case showTableArgs' of
    Nothing -> Right $ DataFrame
        [ Column "table_name" StringType ]
        [ [ StringValue (fst table) ] | table <- database ]
    Just tableName -> maybe (Left $ "Could not find table " ++ tableName)
        (\value -> Right value)
        (findTableByName database tableName)

getColumnTypeByName :: [Column] -> String -> Maybe ColumnType
getColumnTypeByName cols name = do
    (Column _ colType) <- find (\(Column colName _) -> colName == name) cols
    return colType

nullOrAny :: (Foldable t) => (a -> Bool) -> t a -> Bool
nullOrAny f x = null x || any f x

-- Applies where filters and column selection to a table, creating a new table
createFilteredTable :: DataFrame
    -> [String]
    -> [(String, String, String -> String -> Bool)]
    -> DataFrame
createFilteredTable (DataFrame columns rows) selectColumnNames whereArgs' = DataFrame selectionCols selectionRows
    where
        -- The columns we'll be keeping
        selectionCols :: [Column]
        selectionCols = [column | column@(Column name _) <- columns, elem name selectColumnNames]
        -- Give row values their column name
        namedRows :: [[(String, Value)]]
        namedRows = [zip [columnName | (Column columnName _) <- columns] row | row <- rows]
        -- Apply where filter to rows
        filteredNamedRows :: [[(String, Value)]]
        filteredNamedRows = [ namedValues | namedValues <- namedRows,
            nullOrAny
                (\(colName1, colName2, op) -> op (getStringValue $ lookup colName1 namedValues)
                (getStringValue $ lookup colName2 namedValues)) whereArgs']
            where
                getStringValue :: Maybe Value -> String
                getStringValue (Just (StringValue string)) = string
                getStringValue _ = ""
        -- Rid the values in a row where the column name is not used in selection
        selectionNamedRows :: [[(String, Value)]]
        selectionNamedRows = [[namedValue | namedValue@(name, _) <- namedValues,
                elem name selectColumnNames]
            | namedValues <- filteredNamedRows]
        -- Finally remove column names from row values
        selectionRows :: [Row]
        selectionRows = [snd $ unzip namedValues | namedValues <- selectionNamedRows]

-- Applies each function in the list to the second list element of the same index
-- So 1st function affects only the 1st element,
-- 2nd function affects the 2nd element,
-- and so on.
mapByIndex :: [(a -> b)] -> [a] -> [b]
mapByIndex functions lists = reverse $ mapLists' functions lists []
    where
        mapLists' :: [(a -> b)] -> [a] -> [b] -> [b]
        mapLists' [] _ result = result
        mapLists' _ [] result = result
        mapLists' (f:fs) (x:xs) result = mapLists' fs xs ((f x):result)

-- Culls a list of Either from Right elements
getOnlyLefts :: [Either a b] -> [a]
getOnlyLefts list = [val | Left val <- list]

-- Culls a list of Either from Left elements
getOnlyRights :: [Either a b] -> [b]
getOnlyRights list = [val | Right val <- list]

