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
        --Right side: Maybe String = table name (optional), String = collumn name
        selectArgs :: [Either ([(Maybe String, String)], Function) (Maybe String, String)],
        -- Table names
        fromArgs :: [String],
        -- All 'where' args are column_name0 ?=? column_name1 ORed
        whereArgs :: [(WhereOperand, WhereOperand, WhereOperator)]
    }
    | ShowTableStatement {
        -- Either TABLES or TABLE name[, ...]
        showTableArgs :: Maybe String
    }
    | UpdateStatement {
      tablename :: String,
      assignedValues :: [(String, Value)],
      whereArgs :: [(WhereOperand, WhereOperand, WhereOperator)]
    }
    | InsertIntoStatement {
      tablename :: String,
      valuesOrder :: Maybe [String],
      values :: [Value]
    }
    | DeleteStatement {
      tablename :: String,
      whereArgs :: [(WhereOperand, WhereOperand, WhereOperator)]
    }
    --deriving (Eq) IDK what this does just temporary this

data WhereOperand = Constant String | ColumnName (Maybe String, String) deriving Show

data Function = Func0 {
    function0::String
  }
  | Func1{
    function1::[Value] -> Value
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

now :: String
now = "End of time"

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
      result <- if(parseCompare x "select") then parseSelect xs
        else if(parseCompare x "show") then parseShow xs
        else if(parseCompare x "insert") then parseInsert xs
        else if(parseCompare x "update") then parseUpdate xs
        else if(parseCompare x "delete") then parseDelete xs
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
  parseWhereArgs x2
  where
    parseSelectArgs :: String -> Either ErrorMessage (String, ParsedStatement)
    parseSelectArgs [] = Left "Reached unknown state"
    parseSelectArgs a = case (parseWord a) of
      (x, sym, _) | x == "" -> Left $ "Unexpected " ++ [sym]
                  | (elem sym "=<>)'") -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x
      (_, ';', _) -> Left "Missing from statememnt"
      (x, ' ', xs) -> do
                        fullName <- parseFullCollumnName x 
                        Right (xs, SelectStatement { selectArgs = [Right fullName]})
      (x, ',', xs) -> case (parseSelectArgs xs) of
        Left e -> Left e
        Right (parseRem, parseRes) -> do
                                        fullName <- parseFullCollumnName x 
                                        Right (parseRem, SelectStatement { selectArgs = (((Right fullName) : (selectArgs parseRes)))})
      (x, '(', xs) -> do
        (bracketStuff, rem) <- parseBrackets xs
        func <- getFunction x
        args <- if(parseCompare x "now")
                  then parseFuncArgs bracketStuff "empty"
                  else parseFuncArgs bracketStuff "arg"
        fullNames <- parseAllFullCollumnNames args
        result <- if ((getTermination rem) == ' ' && (head rem) == ' ') then 
                      Right (rem, SelectStatement { selectArgs = [Left (fullNames, func)]})
                  else if((getTermination rem) == ',') then case (parseSelectArgs (trpl3 (parseWord rem))) of
                      Left e -> Left e
                      Right (parseRem, parseRes) -> Right (parseRem, SelectStatement { selectArgs = (Left (fullNames, func)) : (selectArgs parseRes)})
                  else Left $ "Unexpected " ++ [getTermination rem] ++ " after (" ++ (head args) ++ ")"
        return (result)

    parseFromArgs :: (String, ParsedStatement) -> Either ErrorMessage (String, ParsedStatement)
    parseFromArgs (a, b) = case (parseKeyword a) of
      Left e -> Left e
      Right (x, xs) -> if (parseCompare x "from" && xs /= "")
        then parseFromArgs' (xs, b)
        else Left "Missing from statement"
        where
          parseFromArgs' :: (String, ParsedStatement) -> Either ErrorMessage (String, ParsedStatement)
          parseFromArgs' (a, b) = case (parseWord a) of
            (x, sym, xs) | elem sym "; " -> Right (xs, SelectStatement { selectArgs = (selectArgs b), fromArgs = [x]})
            (x, sym, xs) | elem sym ","  -> case (parseFromArgs' (xs, b)) of
                Left e -> Left e
                Right (parseRem, parseRes) -> Right (parseRem, SelectStatement { selectArgs = (selectArgs parseRes), fromArgs = (x : (fromArgs parseRes))})
            (x, sym, _)  | x == ""   -> Left $ "Unexpected " ++ [sym]
                         | otherwise -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x
              where parseResult = parseFromArgs' (xs, b)

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
      (x, sym, xs) ->  do
                        fullName <- parseFullCollumnName x 
                        parseOperator (ColumnName fullName, sym, xs) b
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
      (x, ';', _) | x /= "" ->  do
                    fullName <- parseFullCollumnName x 
                    Right SelectStatement { selectArgs = (selectArgs statement), fromArgs = (fromArgs statement), whereArgs = [(a, ColumnName fullName, func)] }
                  | otherwise -> Left "Missing second operand"
      (x, ' ', xs) -> case (parseKeyword xs) of
        Left e -> Left e
        Right (x1, xs1) -> if (parseCompare x1 "or")
          then if (xs1 /= "")
            then do 
              parseRes <- (parseWhereArgs' (xs1, statement))
              fullName <- parseFullCollumnName x
              Right SelectStatement { selectArgs = (selectArgs statement), fromArgs = (fromArgs statement), whereArgs = (a, ColumnName fullName, func) : (whereArgs parseRes)}
            else Left $ "Missing statement after " ++ x1 
          else Left $ "Unrecognised statement: " ++ x1
      (x, sym, _) -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x

parseInsert :: String -> Either ErrorMessage ParsedStatement
parseInsert [] = Left "Insert statement incomplete"
parseInsert a = do
  (x, sym, xs) <- Right $ parseWord a
  result <- if(parseCompare x "into" && (elem sym " ("))
    then do
      (parsedTablename, nextSym, rem) <- do
        (parsedWord, symbol, parseRem) <- Right $ parseWord xs
        tablenameresult <- case (symbol) of
          termSym | elem termSym " (" -> Right (parsedWord, symbol, parseRem)
                  | termSym == ';' -> Left "Missing values in INSERT statement"
          otherwise -> Left $ "Unexpected " ++ [symbol] ++ " after " ++ parsedWord
        return (tablenameresult)
      (parsedValuesOrder, rem2) <- if(nextSym == '(') then parseCollumnnameList rem else Right ([], rem)
      parsedValues <- do
        (keyword, symbol, parseRem) <- Right $ parseWord rem2
        valuesResult <- if(parseCompare keyword "values" && symbol == '(')
          then parseConstantList parseRem
          else if(symbol /= '(') 
            then if(symbol == ';') then Left "Missing value list" else Left $ "Unexpected " ++ [symbol] ++ " after " ++ parseRem 
            else Left $ "Unrecognised keyword: " ++ keyword
        return (valuesResult)
      return (InsertIntoStatement { tablename = parsedTablename, valuesOrder = ifEmptyReturnNothing parsedValuesOrder, values = parsedValues})
    else Left "Unrecognised keyword after INSERT"
  return (result)
  where
    ifEmptyReturnNothing :: [String] -> Maybe [String]
    ifEmptyReturnNothing [] = Nothing
    ifEmptyReturnNothing a = Just a

parseUpdate :: String -> Either ErrorMessage ParsedStatement
parseUpdate [] = Left "Update statement incomplete"
parseUpdate a = do
  (parsedTablename, sym, rem) <- Right $ parseWord a
  _ <- if (sym == ';') then Left "Missing list of updated values" else if(sym /= ' ') then Left $ "Unexpected " ++ [sym] ++ " after " ++ parsedTablename else Right Nothing
  (newAssignedValues, termChar, rem2) <- parseValueAssignment rem
  partialParsedStatememnt <- Right $ UpdateStatement {tablename = parsedTablename, assignedValues = newAssignedValues}
  result <- case (termChar) of
    ' ' -> parseWhereArgs (rem2 , partialParsedStatememnt)
    ';' -> Right UpdateStatement {tablename = parsedTablename, assignedValues = newAssignedValues, whereArgs = []}
    otherwise -> Left $ "Unexpected " ++ [termChar] ++ " after values assignment"
  return (result)

parseValueAssignment :: String -> Either ErrorMessage ([(String, Value)], Char, String)
parseValueAssignment a = case (parseKeyword a) of
  Left e -> Left e
  Right (keyword, rem) | parseCompare keyword "set" -> parseValueAssignment' rem
                       | otherwise -> Left $ "Unrecognised keyword: " ++ keyword
  where
    parseValueAssignment' :: String -> Either ErrorMessage ([(String, Value)], Char, String)
    parseValueAssignment' [] = Left "Missing values to be assigned"
    parseValueAssignment' b = do
      (collumnName, value, sym, rem2) <- parseAssignmentExpr b
      result <- case (sym) of
        ',' -> do
          (laterAssignments, termSym, rem3) <- parseValueAssignment' rem2 
          return ((collumnName, value) : laterAssignments, termSym, rem3)
        tempSym | elem tempSym "; " -> return ([(collumnName, value)], tempSym, rem2)
        otherwise -> Left $ "Unexpected " ++ [sym] ++ " after " ++ rem2
      return (result)

parseAssignmentExpr :: String -> Either ErrorMessage (String, Value, Char, String)
parseAssignmentExpr [] = Left "Unexpected error when trying to parse assignment"
parseAssignmentExpr a = do
  (collumnName, sym, rem) <- Right $ parseWord a
  _ <- if(sym == ';') then Left $ "Missing = sign after " ++ collumnName else if (sym /= '=') then Left $ "Unexpected " ++ [sym] ++ " after " ++ rem else Right Nothing
  (value, endingSym, rem2) <- parseValue rem
  return (collumnName, value, endingSym, rem2)

parseValue :: String -> Either ErrorMessage (Value, Char, String)
parseValue [] = Left "Missing constant after ="
parseValue a = do
  (parsedWord, sym, remainder) <- Right $ parseWord a
  (manipulatedWord, newsym, newrem) <- case (sym) of
    '\'' -> if(parsedWord /= "")
      then Left $ "Unexpected \' after " ++ parsedWord
      else case (parseConstant remainder) of
        ("", "") -> Left "Missing \' character"
        ("", _) -> Left "Value inside \'\' can not be empty"
        (constant, constantRemainder) -> Right (StringValue constant, sym, trpl3(parseWord constantRemainder))
    tempSym -> case (parsedWord) of
      word | parseCompare word "null" -> Right (NullValue, sym, remainder)
           | parseCompare word "true" -> Right (BoolValue True, sym, remainder)
           | isNumber word -> Right (IntegerValue $ getNumber word, sym, remainder)
  return (manipulatedWord, newsym, newrem)

parseDelete :: String -> Either ErrorMessage ParsedStatement
parseDelete [] = Left "Missing delete statement"
parseDelete a = do
  (keyword, rem) <- parseKeyword a
  _ <- if(parseCompare keyword "from") then Right Nothing else Left "Missing FROM keyword"
  (parsedTablename, sym, rem2) <- Right $ parseWord rem
  result <- case (sym) of
    ' ' -> do
      partialStatement <- Right $ DeleteStatement {tablename = parsedTablename}
      result <- parseWhereArgs (rem2, partialStatement)
      return (result)
    ';' -> Right $ DeleteStatement {tablename = parsedTablename, whereArgs = []}
    otherwise -> Left $ "Unexpected " ++ [sym] ++ " after " ++ parsedTablename
  return (result)

getTermination :: String -> Char
getTermination [] = ';'
getTermination (' ':xs) = getTermination xs
getTermination (x:xs) | isTerminating x = x
getTermination (_:xs) = ' '

parseCollumnnameList :: String -> Either ErrorMessage ([String], String)
parseCollumnnameList [] = Left "Missing statement after ("
parseCollumnnameList a = case (parseWord a) of
  (x, ',', xs) -> do
    (parseRes, rem) <- parseCollumnnameList xs
    return (x : parseRes, rem)
  (x, ')', xs) -> Right ([x], xs)
  (x, sym, _) -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x

parseConstantList :: String -> Either ErrorMessage [Value]
parseConstantList [] = Left "Missing statement after ("
parseConstantList a = do 
  (parsedWord, sym, remainder) <- Right $ parseWord a
  (manipulatedWord, newsym, newrem) <- case (sym) of
    '\'' -> if(parsedWord /= "")
      then Left $ "Unexpected \' after " ++ parsedWord
      else case (parseConstant remainder) of
        ("", "") -> Left "Missing \' character"
        ("", _) -> Left "Value inside \'\' can not be empty"
        (constant, constantRemainder) -> case (getTermination constantRemainder) of
          symbol | elem symbol ",)" -> Right (StringValue constant, symbol, trpl3(parseWord constantRemainder))
                 | symbol == ';' -> Left $ "Missing closing ) bracket in element list"
                 | otherwise -> Left $ "Unexpected " ++ [symbol] ++ " after " ++ constant
    tempSym | elem tempSym ",)"  -> case (parsedWord) of
              word | parseCompare word "null" -> Right (NullValue, sym, remainder)
                   | parseCompare word "true" -> Right (BoolValue True, sym, remainder)
                   | isNumber word -> Right (IntegerValue $ getNumber word, sym, remainder)
    otherwise -> Left $ "Unexpected " ++ [sym] ++ " after " ++ parsedWord
  result <- case (newsym) of
    ')' -> Right [manipulatedWord]
    ',' -> do
      parseRes <- parseConstantList newrem
      Right $ manipulatedWord : parseRes
  return result
  -- (x, ',', xs) -> do
  --   (parseRes, rem) <- parseConstantList xs
  --   return (x : parseRes, rem)
  -- (x, ')', xs) -> Right ([x], xs)
  -- (x, sym, _) -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x

isNumber :: String -> Bool
isNumber [] = False
isNumber (x:xs) | elem x "+-" = isNumber' xs False 
                | elem x "0123456789" = isNumber' xs True 
                | otherwise = False
  where
    isNumber' :: String -> Bool -> Bool
    isNumber' [] cond = if (cond) then True else False
    isNumber' (x:xs) _ = isNumber' xs True  

getNumber :: String -> Integer
getNumber a = toInteger $ getNumber' (reverse a)
  where
    getNumber' :: String -> Int
    getNumber' [x] = (fromEnum x) - 48
    getNumber' (x:xs) = 10 * (getNumber' xs) + ((fromEnum x) - 48)

parseFuncArgs :: String -> String -> Either ErrorMessage [String]
parseFuncArgs a "empty" | a == "" = Right []
                        | otherwise = Left $ "Unexpected " ++ a ++ " in the function argument list"
parseFuncArgs a "arg" = case (parseWord a) of
    (_, ';', _) | a /= "" -> Right $ [a]
                | otherwise -> Left "Missing function argument"
    (x, sym, _) -> Left $ "Unexpected " ++ [sym] ++ " after " ++ x                         

parseBrackets :: String -> Either ErrorMessage (String, String)
parseBrackets [] = Left "Missing closing bracket of a function"
parseBrackets (x:xs) = if (x == ')') then Right ("", xs) 
  else do
    parseResult <- parseBrackets xs
    result <- Right (x : (fst parseResult), snd parseResult)
    return (result)

parseConstant :: String -> (String, String)
parseConstant [] = ("", "")
parseConstant (x:xs) = if (x == '\'') then ("", xs) else (x : (fst parseResult), snd parseResult)
  where parseResult = parseConstant xs

parseAllFullCollumnNames :: [String] -> Either ErrorMessage [(Maybe String, String)]
parseAllFullCollumnNames [] = Right []
parseAllFullCollumnNames (x:xs) = do
  xFull <- parseFullCollumnName x
  xsFull <- parseAllFullCollumnNames xs
  return (xFull:xsFull)

parseFullCollumnName :: String -> Either ErrorMessage (Maybe String, String)
parseFullCollumnName a | checkForDots a = case (parseFullCollumnName' a) of
                          Right (a, b) -> Right (Just a, b)
                          Left e -> Left e
                       | otherwise = Right (Nothing, a)
  where
    parseFullCollumnName':: String -> Either ErrorMessage (String, String)
    parseFullCollumnName' (x:xs) | x == '.' = if(checkForDots xs) 
                                  then Left "Unexpected . symbol"
                                  else Right ("", xs)
                                 | otherwise = do
                                   (table, collumn) <- parseFullCollumnName' xs
                                   return (((x : table), collumn))

checkForDots :: String -> Bool
checkForDots [] = False
checkForDots (x:xs) | x == '.' = True
                    | otherwise = checkForDots xs

getFunction :: String -> Either ErrorMessage Function
getFunction a 
  |(parseCompare a "max") = Right $ Func1 Lib2.max
  |(parseCompare a "sum") = Right $ Func1 Lib2.sum
  |(parseCompare a "now") = Right $ Func0 Lib2.now
  | otherwise = Left $ "Unrecognised function: " ++ a

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
executeStatement _ = Left "Once again under renovation"
-- executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
-- executeStatement (SelectStatement selectArgs' fromArgs' whereArgs') = case findTableByName database fromArgs' of
--     Nothing -> Left $ "Could not find table " ++ fromArgs'
--     -- If the table was found,
--     Just table@(DataFrame columns rows) ->
--         -- Ensure we have some select columns
--         if (null selectColumnNames) then
--             Left $ "Got zero columns to select"
--         -- Ensure select only has selection by column value or only by function
--         else if (any (isLeft) selectArgs' && any(isRight) selectArgs') then
--             Left $ "Cannot select by column value and by function at the same time"
--         -- Wildcard only once
--         else if (let (x:xs) = selectColumnNames in x == "*" && not (null xs)) then
--             Left $ "Can only select all columns once"
--         -- Cannot use wildcard with functions
--         else if (let (x:xs) = selectColumnNames in x == "*" && not (null (getOnlyLefts selectArgs'))) then
--             Left $ "Cannot apply function to wildcard"
--         ---- Check if the column names mentioned in the selection arguments actually exist,
--         ---- fall-through for wildcard
--         else if ((intersect selectColumnNames tableColumnNames /= selectColumnNames)
--                         && not (let (x:xs) = selectColumnNames in x == "*" && null xs)
--                 || (intersect whereColumnNames tableColumnNames /= whereColumnNames))
--         then
--             Left $ "Statement references columns which do not exist in table"
--         -- Ensure comparisons are done between string type columns and constants only
--         else if (any
--                 (\(operand1, operand2, _) -> (not $ whereColumnIsStringType columns operand1)
--                     || (not $ whereColumnIsStringType columns operand2))
--                 whereArgs') then
--             Left $ "Only comparisons between string type columns and constants are allowed in `where`"
--         -- Finally print the table
--         else do
--             let DataFrame columns' rows' = createFilteredTable table whereArgs'
--             let columnValues = combineColumnsWithValues columns' (if null rows' then [map (\x -> NullValue) columns'] else rows') -- [(Column, [Value])]
--             -- Check for wildcard, too
--             let selectArgs'' = let (x:_) = selectColumnNames in if x == "*" then [Right columnName | columnName <- tableColumnNames] else selectArgs'
--             case selectArgs'' of
--                 -- If selectArgs'' is Left, then it's (String, [Value] -> Value)
--                 (Left _:_) -> do
--                     let columnNamesAndFunctions = getOnlyLefts selectArgs''
--                     -- Check if column names match and apply functions
--                     let resultColumnValues = [(Column colName colType, [func $ values]) | (name, func) <- columnNamesAndFunctions, (Column colName colType, values) <- columnValues, name == colName]
--                     let resultColumnsRows = uncombineColumnsFromValues resultColumnValues
--                     Right $ DataFrame (fst resultColumnsRows) (snd resultColumnsRows)
--                 -- If selectArgs'' is Right, then it's only String
--                 (Right _:_) -> do
--                     let columnNames = getOnlyRights selectArgs''
--                     -- Check if column names match
--                     let resultColumnValues = [columnValue | columnName <- columnNames, columnValue@(Column name _, _) <- columnValues, columnName == name]
--                     let resultColumnsRows = uncombineColumnsFromValues resultColumnValues
--                     Right $ DataFrame (fst resultColumnsRows) (snd resultColumnsRows) 
--                 _ -> Left $ "Empty select list"
--         where
--             tableColumnNames :: [String]
--             tableColumnNames = [ columnName | Column columnName _ <- columns ]
--             selectColumnNames :: [String]
--             selectColumnNames = map (getSelectColumnName) selectArgs'
--                 where
--                     getSelectColumnName :: Either (String, [Value] -> Value) String -> String
--                     getSelectColumnName (Right str) = str
--                     getSelectColumnName (Left (str, _)) = str
--             whereColumnNames :: [String]
--             whereColumnNames = concat [[a | (ColumnName a, _, _) <- whereArgs'],
--                 [b | (_, ColumnName b, _) <- whereArgs']]
-- executeStatement (ShowTableStatement showTableArgs') = case showTableArgs' of
--     Nothing -> Right $ DataFrame
--         [ Column "table_name" StringType ]
--         [ [ StringValue (fst table) ] | table <- database ]
--     Just tableName -> maybe (Left $ "Could not find table " ++ tableName)
--         (\(DataFrame cols _) -> Right $ DataFrame
--             [Column "column_name" StringType, Column "column_type" StringType]
--             [[StringValue name, StringValue $ show colType] | Column name colType <- cols])
--         (findTableByName database tableName)

-- whereColumnIsStringType :: [Column] -> WhereOperand -> Bool
-- whereColumnIsStringType columns (ColumnName columnName) = getColumnTypeByName columns columnName == Just StringType
--     where
--         getColumnTypeByName :: [Column] -> String -> Maybe ColumnType
--         getColumnTypeByName cols name = do
--             (Column _ colType) <- find (\(Column colName _) -> colName == name) cols
--             return colType
-- whereColumnIsStringType _ _ = True

-- nullOrAny :: (Foldable t) => (a -> Bool) -> t a -> Bool
-- nullOrAny f x = null x || any f x

-- -- Applies where filters and column selection to a table, creating a new table
-- createFilteredTable :: DataFrame
--     -> [(WhereOperand, WhereOperand, String -> String -> Bool)]
--     -> DataFrame
-- createFilteredTable (DataFrame columns rows) whereArgs' = DataFrame columns filteredRows
--     where
--         -- Give row values their column name
--         namedRows :: [[(String, Value)]]
--         namedRows = [zip [columnName | (Column columnName _) <- columns] row | row <- rows]
--         -- Apply where filter to rows
--         filteredNamedRows :: [[(String, Value)]]
--         filteredNamedRows = [ namedValues | namedValues <- namedRows,
--             nullOrAny
--                 (\(whereOperand1, whereOperand2, operator) -> operator
--                     (getStringValue namedValues whereOperand1)
--                     (getStringValue namedValues whereOperand2))
--                 whereArgs']
--             where
--                 getStringValue :: [(String, Value)] -> WhereOperand -> String
--                 getStringValue _ (Constant string) = string
--                 getStringValue namedValues (ColumnName name) = case lookup name namedValues of
--                     Just (StringValue string) -> string
--                     _ -> ""
--         -- Finally remove column names from row values
--         filteredRows :: [Row]
--         filteredRows = [snd $ unzip namedValues | namedValues <- filteredNamedRows]


-- -- Culls a list of Either from Right elements
-- getOnlyLefts :: [Either a b] -> [a]
-- getOnlyLefts list = [val | Left val <- list]

-- -- Culls a list of Either from Left elements
-- getOnlyRights :: [Either a b] -> [b]
-- getOnlyRights list = [val | Right val <- list]

-- -- Zips row values by column
-- combineColumnsWithValues :: [a] -> [[b]] -> [(a, [b])]
-- combineColumnsWithValues columns rows = zip columns $ transpose rows

-- -- Unzips row values by column
-- uncombineColumnsFromValues :: [(a, [b])] -> ([a], [[b]])
-- uncombineColumnsFromValues mapped = (fst $ unzip mapped, transpose $ snd $ unzip mapped)

-- instance Eq ([Value] -> Value) where
--   a == b = True
--   a /= b = False

-- instance Eq (String -> String -> Bool) where
--   a == b = True
--   a /= b = False

-- instance Eq WhereOperand where
--   Constant a == Constant b = a == b
--   ColumnName a == ColumnName b = a == b
--   Constant _ == ColumnName _ = False
--   ColumnName _ == Constant _ = False
--   a /= b = not (a == b)