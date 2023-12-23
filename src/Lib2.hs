{-# LANGUAGE FlexibleInstances #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    WhereOperand (..),
    Function(..),
    now',
    sum',
    max',
    listCartesianProduct
  )
where

import Data.Either
import Data.List
import DataFrame
import InMemoryTables
import Lib1
import EitherT
import Data.Either (Either(Right))
import Data.Time (UTCTime)
import Control.Monad.Trans.State.Strict

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
        whereArgs :: [(WhereOperand, WhereOperand, WhereOperator)],
        -- String stores column name while Boolean is true if it shall be sorted in ascending order
        orderByArgs :: [((Maybe String, String), Bool)]
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
    | CreateTableStatement {
      tablename :: String,
      columns :: [Column]
    }
    | DropTableStatement {
      tablename :: String
    }


data WhereOperand = Constant String | ColumnName (Maybe String, String) deriving Show

data Function = Func0 {
    function0::String
  }
  | Func1{
    function1::[Value] -> Value
  }

-- Functions to implement
max :: [Value] -> Value
max [] = NullValue
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
sum [] = NullValue
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
parseStatement :: String -> EitherT ErrorMessage (State String) ParsedStatement
parseStatement a = do
  lift $ put $ normaliseString $ parseEndSemicolon a
  parseStatement' 
  where 
    parseStatement' :: EitherT ErrorMessage (State String) ParsedStatement
    parseStatement' = do
      input <- lift $ get
      case (input) of
        [] -> throwE "No statement found" 
        stmt -> do
          x <- parseKeyword
          result <- if(parseCompare x "select") then parseSelect
            else if(parseCompare x "show") then parseShow
            else if(parseCompare x "insert") then parseInsert
            else if(parseCompare x "update") then parseUpdate
            else if(parseCompare x "delete") then parseDelete
            else if(parseCompare x "create") then parseCreate
            else if(parseCompare x "drop")   then parseDrop
            else throwE "Keyword unrecognised"
          return(result)

normaliseString :: String -> String
normaliseString [] = []
normaliseString (x:xs) = ((normaliseChar x):(normaliseString xs))
  where 
    normaliseChar :: Char -> Char
    normaliseChar x | elem x "\n\t" = ' '
    normaliseChar x = x

parseKeyword :: EitherT ErrorMessage (State String) String
parseKeyword = do
  input <- lift get
  case (input) of
    [] -> throwE "Missing input for parsing the keyword"
    stmt -> do
      let parseResult = parseWord stmt
      if (isTerminating (trpl2 parseResult))
      then if (trpl2 parseResult /= '\'')
        then throwE $ "Unexpected " ++ [(trpl2 parseResult)] ++ " after " ++ (trpl1 parseResult)
        else do
          lift $ put $ '\'':(trpl3 parseResult)
          return $ trpl1 parseResult  
      else do 
        lift $ put $ trpl3 parseResult
        return $ trpl1 parseResult

isTerminating :: Char -> Bool
isTerminating a | elem a "(),=<>'" = True
isTerminating _ = False

parseSelect :: EitherT ErrorMessage (State String) ParsedStatement
parseSelect = do
  input <- lift get
  case (input) of
    [] -> throwE "Incomplete select statement"
    x  ->  do
      x1 <- parseSelectArgs
      parsedStatement <- parseFromArgs x1
      parsedWhereArgs <- parseWhereArgs
      parsedOrderArgs <- parseOrderByArgs
      return (SelectStatement { selectArgs = (selectArgs parsedStatement), fromArgs = (fromArgs parsedStatement), whereArgs = parsedWhereArgs, orderByArgs = parsedOrderArgs})
      where
        parseSelectArgs :: EitherT ErrorMessage (State String) ParsedStatement
        parseSelectArgs = do
          input <- lift get
          case (input) of
            [] -> throwE "Reached unknown state"
            a -> case (parseWord a) of
              (x, sym, _) | x == "" -> throwE $ "Unexpected " ++ [sym]
                          | (elem sym "=<>)'") -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ x
              (_, ';', _) -> throwE "Missing from statememnt"
              (x, ' ', xs) -> do
                fullName <- addTtoEither $ parseFullCollumnName x
                lift $ put xs
                return $ SelectStatement { selectArgs = [Right fullName]}
              (x, ',', xs) -> do
                lift $ put xs
                parseRes <- parseSelectArgs
                fullName <- addTtoEither $ parseFullCollumnName x
                return $ SelectStatement { selectArgs = (((Right fullName) : (selectArgs parseRes)))}
              (x, '(', xs) -> do
                lift $ put xs
                bracketStuff <- parseBrackets
                func <- addTtoEither $ getFunction x
                args <- if(parseCompare x "now")
                          then addTtoEither $ parseFuncArgs bracketStuff "empty"
                          else addTtoEither $ parseFuncArgs bracketStuff "arg"
                fullNames <- addTtoEither $ parseAllFullCollumnNames args
                rem <- lift get
                result <- if ((getTermination rem) == ' ' && (head' rem) == ' ') then 
                              return $ SelectStatement { selectArgs = [Left (fullNames, func)]}
                          else if((getTermination rem) == ',') then do
                            withRemovedTermination <- pure $ trpl3 $ parseWord rem
                            lift $ put withRemovedTermination
                            parsedStmt <- parseSelectArgs
                            return $ SelectStatement { selectArgs = (Left (fullNames, func)) : (selectArgs parsedStmt)}
                          else throwE $ "Unexpected " ++ [getTermination rem] ++ " after (" ++ (head' args) ++ ")"
                return (result)

        parseFromArgs :: ParsedStatement -> EitherT ErrorMessage (State String) ParsedStatement
        parseFromArgs parsedStmt = do
          input <- lift get
          case (input) of
            [] -> throwE "Missing from statement"
            remainder -> do 
              keyword <- parseKeyword
              if (parseCompare keyword "from")
                then parseFromArgs' parsedStmt
                else throwE "Missing from statement"
                where
                  parseFromArgs' :: ParsedStatement -> EitherT ErrorMessage (State String) ParsedStatement
                  parseFromArgs' b = do
                    input <- lift get
                    case (input) of
                       [] -> throwE "Missing from arguments"
                       _ -> case (parseWord input) of
                        (x, sym, xs) | elem sym "; " -> do
                                        lift $ put xs
                                        return $ SelectStatement { selectArgs = (selectArgs b), fromArgs = [x]}
                                     | elem sym ","  -> do
                                       lift $ put xs
                                       parsedStmt <- parseFromArgs' b
                                       return $ SelectStatement { selectArgs = (selectArgs parsedStmt), fromArgs = (x : (fromArgs parsedStmt))}
                                     | x == ""   -> throwE $ "Unexpected " ++ [sym]
                                     | otherwise -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ x

        parseOrderByArgs :: EitherT ErrorMessage (State String) [((Maybe String, String), Bool)]
        parseOrderByArgs = do
          input <- lift get
          case (input) of
            [] -> return [] 
            _  -> do
              keyword <- parseKeyword
              if(parseCompare keyword "order") 
                then do
                  keywordBy <- parseKeyword
                  _ <- if(parseCompare keywordBy "by") then return Nothing else throwE "Missing BY keyword after ORDER"
                  parseOrderColumnList  
                else throwE $ "Unrecognised statement: " ++ keyword
          where
            parseOrderColumnList :: EitherT ErrorMessage (State String) [((Maybe String, String), Bool)]
            parseOrderColumnList = do
              (sym, orderColumn) <- parseOrderColumn
              case (sym) of
                ',' -> do
                  parsedOrderList <- parseOrderColumnList
                  return (orderColumn : parsedOrderList)
                ';' -> return [orderColumn]
                _   -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ (snd $ fst orderColumn)

            parseOrderColumn :: EitherT ErrorMessage (State String) (Char, ((Maybe String, String), Bool))
            parseOrderColumn = do
              input <- lift get
              case (input) of
                [] -> throwE "Missing column name in ORDER BY statement"
                _  -> do
                  (columnName, sym, rem) <- return $ parseWord input
                  _ <- if (columnName /= "") then return Nothing else throwE $ "Missing column name"
                  fullColumnName <- addTtoEither $ parseFullCollumnName columnName
                  case (sym) of
                    ' ' -> do
                      (dirKey, sym2, rem2) <- return $ parseWord rem
                      lift $ put rem2
                      case (dirKey) of
                        word | parseCompare dirKey "asc"  -> return (sym2, (fullColumnName, True))
                             | parseCompare dirKey "desc" -> return (sym2, (fullColumnName, False))
                             | otherwise -> throwE $ "Unrecognised keyword: " ++ dirKey
                    _   -> do
                      lift $ put rem
                      return (sym, (fullColumnName, True))


parseWhereArgs :: EitherT ErrorMessage (State String) [(WhereOperand, WhereOperand, WhereOperator)]
parseWhereArgs = do
  input <- lift get
  case (input) of
    [] -> return []
    a -> do 
     x <- parseKeyword
     xs <- lift get
     if (parseCompare x "where")
      then if (xs /= "") then parseWhereArgs' else throwE "Missing where statement"
      else do
        lift $ put input
        return []
  where
    parseWhereArgs' :: EitherT ErrorMessage (State String) [(WhereOperand, WhereOperand, WhereOperator)]
    parseWhereArgs' = do
      a <- lift get
      res <- case (parseWord a) of
        (x, '\'', xs) | x == "" -> case (parseConstant xs) of
                        ("", _) -> throwE "Constant can not be emtpy"
                        (x1, xs1) -> case (parseWord xs1) of
                          (_, '\'', _) -> throwE $ "Unexpected \' after " ++ x
                          ("", sym, xs2) -> do
                            lift $ put xs2
                            parseOperator (Constant x1, sym)
                          (x2, _, _) -> throwE $ "Unexpected " ++ x2 ++ " after " ++ x1 
                      | otherwise -> throwE $ "Unexpected \' after " ++ x  
        (x, sym, xs) ->  do
                          fullName <- addTtoEither $ parseFullCollumnName x 
                          lift $ put xs
                          parseOperator (ColumnName fullName, sym)
      return res
        where
          parseOperator :: (WhereOperand, Char) -> EitherT ErrorMessage (State String) [(WhereOperand, WhereOperand, WhereOperator)]
          parseOperator (x1, sym) | elem sym ",()" = throwE $ "Unexpected " ++ [sym] ++ " after " ++ show x1
                                  | show x1 == "" = throwE $ "Unexpected " ++ [sym]
                                  | elem sym "; " = throwE $ "Missing predicate after " ++ show x1
          parseOperator (x1, '=') = parseSecondOperand equal x1
          parseOperator (x1, '>') = do
            xs1 <- lift get
            lift $ put $ tail xs1
            if (xs1 /= "" && (xs1 !! 0) == '=')
              then parseSecondOperand moreOrEqual x1
              else throwE "Unexpected symbol after >"
          parseOperator (x1, '<') = do
            xs1 <- lift get
            lift $ put $ tail xs1
            if (xs1 /= "" && (xs1 !! 0) == '=')
              then parseSecondOperand lessOrEqual x1
              else if (xs1 /= "" && (xs1 !! 0) == '>')
                then parseSecondOperand unequal x1
                else throwE "Unexpected symbol after <"

    parseSecondOperand :: (String -> String -> Bool) -> WhereOperand -> EitherT ErrorMessage (State String) [(WhereOperand, WhereOperand, WhereOperator)]
    parseSecondOperand func a = do
      ab <- lift get
      case (parseWord ab) of
        (x, '\'', xs) 
          | x == "" -> case (parseConstant xs) of
            ("", _) -> throwE "Constant can not be emtpy"
            (x1, xs1) -> case (parseWord xs1) of
              (x2, ';', xs2)| x2 == "" -> do
                                lift $ put xs2
                                return [(a, Constant x1, func)]
                            | parseCompare x2 "or" -> throwE $ "Missing statement after " ++ x2 
                            | otherwise -> throwE $ "Unexpected " ++ x2 ++ " after " ++ x1  
              (x2, ' ', xs2) | parseCompare x2 "or" -> if (xs2 /= "")
                                then do
                                  lift $ put xs2
                                  parseRes <- parseWhereArgs'
                                  return $ (a, Constant x1, func) : parseRes
                                else throwE $ "Missing statement after " ++ x2
                              | otherwise -> do
                                lift $ put xs1
                                return $ [(a, Constant x1, func)]
              (x2, '\'', xs2) | parseCompare x2 "or" -> if (xs2 /= "")
                                then do
                                  lift $ put $ '\'':xs2 
                                  parseRes <- parseWhereArgs'
                                  return $ (a, Constant x1, func) : parseRes
                                else throwE $ "Missing statement after " ++ x2
                              | otherwise -> do
                                lift $ put $ xs1
                                return $ [(a, Constant x1, func)]
              (x2, sym, _) -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ x2
          | otherwise -> throwE $ "Unexpected \' after " ++ x  
        (x, ';', xs) | x /= "" ->  do
                      lift $ put xs
                      fullName <- addTtoEither $ parseFullCollumnName x 
                      return $ [(a, ColumnName fullName, func)]
                    | otherwise -> throwE "Missing second operand"
        (x, ' ', xs) -> do
          lift $ put xs
          x1 <- parseKeyword
          xs1 <- lift get
          if (parseCompare x1 "or")
            then if (xs1 /= "")
              then do 
                parseRes <- parseWhereArgs'
                fullName <- addTtoEither $ parseFullCollumnName x
                return $ (a, ColumnName fullName, func) : parseRes
              else throwE $ "Missing statement after " ++ x1 
            else do
              fullName <- addTtoEither $ parseFullCollumnName x
              lift $ put xs
              return $ [(a, ColumnName fullName, func)]
        (x, sym, _) -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ x

parseInsert :: EitherT ErrorMessage (State String) ParsedStatement
parseInsert = do
  input <- lift get
  case (input) of
    [] -> throwE "Insert statement incomplete"
    a  -> do
      (x, sym, xs) <- return $ parseWord a
      result <- if(parseCompare x "into" && (elem sym " ("))
        then do
          (parsedTablename, nextSym) <- do
            (parsedWord, symbol, parseRem) <- return $ parseWord xs
            lift $ put parseRem
            tablenameresult <- case (symbol) of
              termSym | elem termSym " (" -> return (parsedWord, symbol)
                      | termSym == ';' -> throwE "Missing values in INSERT statement"
              otherwise -> throwE $ "Unexpected " ++ [symbol] ++ " after " ++ parsedWord
            return (tablenameresult)
          parsedValuesOrder <- if(nextSym == '(') then parseCollumnnameList else return []
          rem <- lift get
          parsedValues <- do
            (keyword, symbol, parseRem) <- return $ parseWord rem
            lift $ put parseRem
            if(parseCompare keyword "values" && symbol == '(')
              then parseConstantList
              else if(symbol /= '(') 
                then if(symbol == ';') then throwE "Missing value list" else throwE $ "Unexpected " ++ [symbol] ++ " after " ++ parseRem 
                else throwE $ "Unrecognised keyword: " ++ keyword
          return (InsertIntoStatement { tablename = parsedTablename, valuesOrder = ifEmptyReturnNothing parsedValuesOrder, values = parsedValues})
        else throwE "Unrecognised keyword after INSERT"
      return (result)
      where
        ifEmptyReturnNothing :: [String] -> Maybe [String]
        ifEmptyReturnNothing [] = Nothing
        ifEmptyReturnNothing a = Just a

parseUpdate :: EitherT ErrorMessage (State String) ParsedStatement
parseUpdate = do
  input <- lift get
  case (input) of
    [] -> throwE "Update statement incomplete"
    _  -> do
      (parsedTablename, sym, rem) <- return $ parseWord input
      _ <- if (sym == ';') then throwE "Missing list of updated values" else if(sym /= ' ') then throwE $ "Unexpected " ++ [sym] ++ " after " ++ parsedTablename else return Nothing
      lift $ put rem
      (newAssignedValues, termChar) <- parseValueAssignment
      case (termChar) of
        ' ' -> do
          parseRes <- parseWhereArgs
          leftover <- lift get
          _ <- if (leftover /= "") then throwE "Unrecognised keyword at the end of the statement" else return Nothing
          return $ UpdateStatement {tablename = parsedTablename, assignedValues = newAssignedValues, whereArgs = parseRes}
        ';' -> return $ UpdateStatement {tablename = parsedTablename, assignedValues = newAssignedValues, whereArgs = []}
        otherwise -> throwE $ "Unexpected " ++ [termChar] ++ " after values assignment"

parseValueAssignment :: EitherT ErrorMessage (State String) ([(String, Value)], Char)
parseValueAssignment = do
  keyword <- parseKeyword
  if (parseCompare keyword "set")
    then parseValueAssignment'
    else throwE $ "Unrecognised keyword: " ++ keyword
  where
    parseValueAssignment' :: EitherT ErrorMessage (State String) ([(String, Value)], Char)
    parseValueAssignment' = do
      input <- lift get
      case (input) of
        [] -> throwE "Missing values to be assigned"
        _  -> do
          (collumnName, value, sym) <- parseAssignmentExpr
          rem <- lift get
          result <- case (sym) of
            ',' -> do
              (laterAssignments, termSym) <- parseValueAssignment' 
              return ((collumnName, value) : laterAssignments, termSym)
            tempSym | elem tempSym "; " -> return ([(collumnName, value)], tempSym)
            otherwise -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ rem
          return (result)

parseAssignmentExpr :: EitherT ErrorMessage (State String) (String, Value, Char)
parseAssignmentExpr = do
  input <- lift get
  case (input) of
    [] -> throwE "Unexpected error when trying to parse assignment"
    _  -> do
      (collumnName, sym, rem) <- return $ parseWord input
      _ <- if(sym == ';') then throwE $ "Missing = sign after " ++ collumnName else if (sym /= '=') then throwE $ "Unexpected " ++ [sym] ++ " after " ++ rem else return Nothing
      lift $ put rem
      (value, endingSym) <- parseValue
      return (collumnName, value, endingSym)

parseValue :: EitherT ErrorMessage (State String) (Value, Char)
parseValue = do
  input <- lift get
  case (input) of
    [] -> throwE "Missing constant after ="
    _  -> do
      (parsedWord, sym, remainder) <- return $ parseWord input
      case (sym) of
        '\'' -> if(parsedWord /= "")
          then throwE $ "Unexpected \' after " ++ parsedWord
          else case (parseConstant remainder) of
            ("", "") -> throwE "Missing \' character"
            ("", _) -> throwE "Value inside \'\' can not be empty"
            (constant, constantRemainder) -> do
              lift $ put $ removeCharIfTerminating constantRemainder
              return (StringValue constant, getTermination constantRemainder)
        _ -> case (parsedWord) of
          word | parseCompare word "null" -> do
                  lift $ put remainder
                  return (NullValue, sym)
               | parseCompare word "true" -> do
                  lift $ put remainder
                  return (BoolValue True, sym)
               | parseCompare word "false" -> do
                  lift $ put remainder
                  return (BoolValue False, sym)
               | isNumber word -> do
                  lift $ put remainder
                  return (IntegerValue $ getNumber word, sym)
               | otherwise -> throwE $ "Unexpected value: " ++ word

parseDelete :: EitherT ErrorMessage (State String) ParsedStatement
parseDelete = do
  input <- lift get
  case (input) of
    [] -> throwE "Missing delete statement"
    _  -> do
      keyword <- parseKeyword
      _ <- if(parseCompare keyword "from") then return Nothing else throwE "Missing FROM keyword"
      rem <- lift get
      (parsedTablename, sym, rem2) <- return $ parseWord rem
      lift $ put rem2
      case (sym) of
        ' ' -> do
          parsedWhereArgs <- parseWhereArgs
          leftover <- lift get
          _ <- if (leftover /= "") then throwE "Unrecognised keyword at the end of the statement" else return Nothing
          return $ DeleteStatement {tablename = parsedTablename, whereArgs = parsedWhereArgs}
        ';' -> return $ DeleteStatement {tablename = parsedTablename, whereArgs = []}
        otherwise -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ parsedTablename

parseCreate :: EitherT ErrorMessage (State String) ParsedStatement
parseCreate = do
  input <- lift get
  case (input) of
    [] -> throwE "Missing create statement"
    _  -> do
      keyword <- parseKeyword
      _ <- if(parseCompare keyword "table") then return Nothing else throwE "Missing TABLE keyword"
      rem <- lift get
      (parsedTablename, sym, rem2) <- return $ parseWord rem
      _ <- if (parsedTablename == "") then throwE $ "Missing tablename" else return Nothing
      case (sym) of
        '(' -> do
          lift $ put rem2
          parsedColumns <- parseColumnDeclarationList
          return $ CreateTableStatement {tablename = parsedTablename, columns = parsedColumns}
        ';' -> throwE $ "Missing columns in CREATE TABLE statement"
        otherwise -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ parsedTablename

parseColumnDeclarationList :: EitherT ErrorMessage (State String) [Column]
parseColumnDeclarationList = do
  (sym, column) <- parseColumnDeclaration
  case (sym) of
    ',' -> do
      parsedDeclarationList <- parseColumnDeclarationList
      return (column : parsedDeclarationList)
    ')' -> do
      rem <- lift get
      _ <- if (rem /= "") then throwE $ "Unexpected " ++ rem ++ " after column declaration list" else return Nothing
      return [column]
    _   -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ (show column)

parseColumnDeclaration :: EitherT ErrorMessage (State String) (Char, Column)
parseColumnDeclaration = do
  input <- lift get
  case (input) of
    [] -> throwE "Missing closing bracket of collumn declarations"
    _ -> do
      (columnName, sym, rem) <- return $ parseWord input
      case (sym) of
        ' ' -> do
          (typeName, sym2, rem2) <- return $ parseWord rem
          lift $ put rem2
          case (typeName) of
            word | parseCompare typeName "string" -> return (sym2, Column columnName StringType)
                 | parseCompare typeName "bool" -> return (sym2, Column columnName BoolType)
                 | parseCompare typeName "int" -> return (sym2, Column columnName IntegerType)
                 | otherwise -> throwE $ "Unrecognised type name: " ++ typeName
        _   -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ columnName

parseDrop :: EitherT ErrorMessage (State String) ParsedStatement
parseDrop = do
  input <- lift get
  case (input) of
    [] -> throwE "Missing drop statement"
    _  -> do
      keyword <- parseKeyword
      _ <- if(parseCompare keyword "table") then return Nothing else throwE "Missing TABLE keyword"
      rem <- lift get
      (parsedTablename, sym, rem2) <- return $ parseWord rem
      _ <- if (parsedTablename == "") then throwE $ "Missing tablename" else return Nothing
      case (sym) of
        ';' -> do
          lift $ put rem2
          return $ DropTableStatement {tablename = parsedTablename}
        ' ' -> throwE $ "Unexpected " ++ rem2 ++ " after " ++ parsedTablename
        otherwise -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ parsedTablename

getTermination :: String -> Char
getTermination [] = ';'
getTermination (' ':xs) = getTermination xs
getTermination (x:xs) | isTerminating x = x
getTermination (_:xs) = ' '

parseCollumnnameList :: EitherT ErrorMessage (State String) [String]
parseCollumnnameList = do
  input <- lift get
  case (input) of
    [] -> throwE "Missing statement after ("
    _  -> case (parseWord input) of
      (x, ',', xs) -> do
        lift $ put xs
        parseRes <- parseCollumnnameList
        return $ x : parseRes
      (x, ')', xs) -> do
        lift $ put xs
        return [x]
      (x, sym, _) -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ x

parseConstantList :: EitherT ErrorMessage (State String) [Value]
parseConstantList = do
  input <- lift get
  case (input) of
    [] -> throwE "Missing statement after ("
    _  -> do 
      (parsedWord, sym, remainder) <- return $ parseWord input
      (manipulatedWord, newsym, newrem) <- case (sym) of
        '\'' -> if(parsedWord /= "")
          then throwE $ "Unexpected \' after " ++ parsedWord
          else case (parseConstant remainder) of
            ("", "") -> throwE "Missing \' character"
            ("", _) -> throwE "Value inside \'\' can not be empty"
            (constant, constantRemainder) -> case (getTermination constantRemainder) of
              symbol | elem symbol ",)" -> return (StringValue constant, symbol, trpl3(parseWord constantRemainder))
                     | symbol == ';' -> throwE $ "Missing closing ) bracket in element list"
                     | otherwise -> throwE $ "Unexpected " ++ [symbol] ++ " after " ++ constant
        tempSym | elem tempSym ",)"  -> case (parsedWord) of
                  word | parseCompare word "null" -> return (NullValue, sym, remainder)
                       | parseCompare word "true" -> return (BoolValue True, sym, remainder)
                       | parseCompare word "false" -> return (BoolValue False, sym, remainder)
                       | isNumber word -> return (IntegerValue $ getNumber word, sym, remainder)
                       | otherwise -> throwE $ "Unrecognised value: " ++ word
        otherwise -> throwE $ "Unexpected " ++ [sym] ++ " after " ++ parsedWord
      case (newsym) of
        ')' -> return [manipulatedWord]
        ',' -> do
          lift $ put newrem
          parseRes <- parseConstantList
          return $ manipulatedWord : parseRes

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

parseBrackets :: EitherT ErrorMessage (State String) String
parseBrackets = do
  input <- lift get
  case (input) of
    [] -> throwE "Missing closing bracket of a function"
    (x:xs) -> if (x == ')') 
      then do
        lift $ put xs
        return "" 
      else do
        lift $ put xs
        parseResult <- parseBrackets
        return $ x : parseResult

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

parseShow :: EitherT ErrorMessage (State String) ParsedStatement
parseShow = do
  input <- lift get
  case (input) of
    [] -> throwE "Show statement incomplete"
    _  -> do 
      keyword <- parseKeyword
      xs <- lift get
      if (parseCompare keyword "tables")
        then if (xs == "") 
          then return $ ShowTableStatement { showTableArgs = Nothing } 
          else throwE "Too many arguments in show tables command"
        else if(parseCompare keyword "table")
          then if (xs /= "") then parseTableName else throwE "Missing table arguments"
          else throwE "Unrecognised show command"

parseTableName :: EitherT ErrorMessage (State String) ParsedStatement
parseTableName = do
  input <- lift get
  case (parseWord input) of
    (result, ';', _) -> do
      lift $ put result
      return $ ShowTableStatement { showTableArgs = Just result}
    _ -> throwE "Too many arguments in show table statement"

parseEndSemicolon :: String -> String
parseEndSemicolon [] = ""
parseEndSemicolon (x:xs) = if (x == ';') then "" else x : parseEndSemicolon xs

parseCompare :: String -> String -> Bool
parseCompare [] [] = True
parseCompare (x:xs) (a:ab) = if (toLower(x) == toLower(a)) then parseCompare xs ab else False
parseCompare _ _ = False

-- Executes a parsed statemet with the given database. Produces a DataFrame.
executeStatement :: ParsedStatement -> [(TableName, DataFrame)] -> Either ErrorMessage DataFrame
executeStatement statement@(SelectStatement selectArgs' tableNames' whereArgs' orderByArgs') database' = do
    _ <- guardCheck (null selectArgs')
        $ "Zero columns in 'select'"
    _ <- guardCheck (any (isLeft) selectArgs' && anyRight selectArgs')
        $ "Cannot use both functions and select columns in 'select'"
    usedTables <- getUsedTables tableNames' database'
    _ <- traverse validateDataFrame [table | (_, table) <- usedTables]
    _ <- guardCheck (null usedTables)
        $ "No tables in 'from'"
    let allNamedCols = [((Just name, colName), col) | (name, DataFrame cols _) <- usedTables, col@(Column colName _) <- cols]
    let namedHell = map (createNamedRows) usedTables
    let cartesianHell = listCartesianProduct namedHell
    filteredCartesianHell <- filterByWhere cartesianHell whereArgs' allNamedCols
    orderedCartesianHell <- orderBy filteredCartesianHell orderByArgs' allNamedCols
    case selectArgs' of
        Right (Nothing, "*"):[] -> Right $ DataFrame
            (case usedTables of
                (_, DataFrame cols _):[] -> cols
                _ -> [Column (tableName ++ "." ++ colName) colType
                    | (tableName, DataFrame cols _) <- usedTables, Column colName colType <- cols])
            (map (map snd) orderedCartesianHell)
        Right (Just _, "*"):_ -> Left $ "Wildcard selector must be used without table name in 'select'"
        Right (_, "*"):xs -> Left $ "Wildcard selector must be used by itself in 'select'"
        _ -> do
            colValues <- Right $ if (null orderedCartesianHell)
                then [(((tableName, colName), colType), [])
                        | (tableName, DataFrame cols _) <- usedTables,
                            Column colName colType <- cols]
                else (zip
                    [((tableName, colName), colType)
                        | (tableName, DataFrame cols _) <- usedTables,
                            Column colName colType <- cols]
                    (transpose $ map (map snd) orderedCartesianHell))
            selectedColumnValues <- applySelectArgs colValues selectArgs'
            Right $ DataFrame
                [Column ((maybe "" (++ ".") tableName) ++ colName) colType
                    | (((tableName, colName), colType), _) <- selectedColumnValues]
                (transpose [values | (_, values) <- selectedColumnValues])
    where
        isStringColumn :: Column -> Bool
        isStringColumn (Column _ StringType) = True
        isStringColumn _ = False
        getUsedTables :: [String] -> [(TableName, DataFrame)] -> Either ErrorMessage [(TableName, DataFrame)]
        getUsedTables names db = getUsedTables' names db []
            where
                getUsedTables' :: [String] -> [(TableName, DataFrame)] -> [(TableName, DataFrame)]
                    -> Either ErrorMessage [(TableName, DataFrame)]
                getUsedTables' (x:xs) db acc = case lookup x db of
                    Just dataframe -> getUsedTables' xs db ((x, dataframe):acc)
                    _ -> Left $ "Could not find table " ++ x ++ " in db"
                getUsedTables' [] _ acc = Right $ reverse acc
        createNamedRows :: (TableName, DataFrame) -> [[((Maybe String, String), Value)]]
        createNamedRows (tableName, DataFrame cols rows) = [[((Just tableName, colName), row)
                | (colName, row) <- innerList]
            | innerList <- [zip colNames row | row <- rows]]
            where
                colNames :: [String]
                colNames = getColumnNames cols
        checkTableColsWithWhereColNames :: [((Maybe TableName, String), Column)]
            -> [(WhereOperand, WhereOperand, WhereOperator)]
            -> Either ErrorMessage ()
        checkTableColsWithWhereColNames tableNamedCols whereOps =
            forEach (\whereColName acc -> if null acc
                then acc
                else (case findAllBy
                        (\(tableColName, colName) -> case whereColName of
                            (Nothing, name) -> colName == name
                            (tableName, name) -> tableColName == tableName && colName == name)
                        tableNamedCols of
                        [] -> Left $ "Column " ++ show whereColName ++ " does not exist"
                        x:[] -> guardCheck (not $ isStringColumn x)
                            $ "Column " ++ show whereColName ++ "is not of string type"
                        x:xs -> Left $ "Column " ++ show whereColName ++ " matched in more than one table"))
                allWhereColNames (Right ())
            where
                allWhereColNames = [operand | (_, ColumnName operand, _) <- whereOps]
                    ++ [operand | (ColumnName operand, _, _) <- whereOps]
        filterByWhere :: [[((Maybe String, String), Value)]]
            -> [(WhereOperand, WhereOperand, WhereOperator)]
            -> [((Maybe TableName, String), Column)]
            -> Either ErrorMessage [[((Maybe String, String), Value)]]
        filterByWhere namedRows ops allNamedCols = do
            _ <- checkTableColsWithWhereColNames allNamedCols whereArgs'
            Right $ [namedRow | namedRow <- namedRows, executeWhere namedRow ops]
            where
                executeWhere :: [((Maybe String, String), Value)]
                    -> [(WhereOperand, WhereOperand, WhereOperator)]
                    -> Bool
                executeWhere namedRow [] = True
                executeWhere namedRow ops = forEach (\(lVal, rVal, operator) acc -> if acc
                    then acc
                    else (operator (getValue lVal) (getValue rVal)))
                    ops False
                    where
                        getValue :: WhereOperand -> String
                        getValue x = case x of
                            Constant str -> str
                            ColumnName (maybeOpTableName, opColName) -> case findAllBy
                                (\(maybeTableName, colName) -> case maybeOpTableName of
                                    Nothing -> opColName == colName
                                    Just _ -> maybeOpTableName == maybeTableName && opColName == colName)
                                namedRow of
                                (StringValue value):[] -> value
                                _ -> ""
        orderBy :: [[((Maybe String, String), Value)]]
            -> [((Maybe String, String), Bool)]
            -> [((Maybe TableName, String), Column)]
            -> Either ErrorMessage [[((Maybe String, String), Value)]]
        orderBy cartHell [] _ = Right $ cartHell
        orderBy cartHell orderings tableNamedCols = do
            _ <- forEach (\orderByColName acc -> if null acc
                    then acc
                    else (case findAllBy
                            (\(tableColName, colName) -> case orderByColName of
                                (Nothing, name) -> colName == name
                                (tableName, name) -> tableColName == tableName && colName == name)
                            tableNamedCols of
                            [] -> Left $ "Column " ++ show orderByColName ++ " does not exist"
                            x:[] -> Right ()
                            x:xs -> Left $ "Column " ++ show orderByColName ++ " matched in more than one table"))
                    allOrderByColNames (Right ())
            Right $ sortBy executeOrdering cartHell
            where
                allOrderByColNames :: [(Maybe String, String)]
                allOrderByColNames = [x | (x, _) <- orderings]
                executeOrdering :: [((Maybe String, String), Value)]
                    -> [((Maybe String, String), Value)]
                    -> Ordering
                executeOrdering x y = forEach (\((tableName, colName), isAsc) acc -> case acc of
                    EQ -> case findBy (findValue (tableName, colName)) x of
                        Nothing -> EQ
                        Just xValue -> case findBy (findValue (tableName, colName)) y of
                            Nothing -> EQ
                            Just yValue -> if isAsc
                                then compare xValue yValue
                                else compare yValue xValue
                    _ -> acc) orderings EQ
                    where
                        findValue :: (Maybe String, String)
                            -> (Maybe String, String)
                            -> Bool
                        findValue needle@(needleTableName, needleColName)
                            (hayTableName, hayColName) = case needle of
                                (Nothing, needleColName)
                                    -> needleColName == hayColName
                                (Just _, needleColName)
                                    -> needleTableName == hayTableName && needleColName == hayColName
        applySelectArgs :: [(((TableName, String), ColumnType), [Value])]
            -> [Either ([(Maybe TableName, String)], Function) (Maybe TableName, String)]
            -> Either ErrorMessage [(((Maybe TableName, String), ColumnType), [Value])]
        applySelectArgs columnsWithValues selects = case [val | (Right val) <- selects] of
            [] -> do
                let selectWithFuncs = [val | (Left val) <- selects]
                let selectColumnNames = (map fst selectWithFuncs)
                selectionOfColumns <- forEach
                    (\x acc -> do
                        trueAcc <- acc
                        selection <- getSelectionOfColumns x
                        Right $ selection:trueAcc)
                    (reverse selectColumnNames)
                    (Right [])
                funcAppliedColumns <- Right $ forEach
                    (\((colInfo, values):_, f) acc -> case f of
                        Func0 f0 -> ([(((Nothing, "CurrentTime"), StringType), [StringValue f0])]:acc)
                        Func1 f1 -> [(colInfo, [f1 values])]:acc)
                    (reverse $ zip selectionOfColumns $ map snd selectWithFuncs)
                    ([])
                Right $ decomposeListList funcAppliedColumns
            selectColumnNames -> getSelectionOfColumns selectColumnNames
            where
                getSelectionOfColumns columnNames = forEach
                    (\selectTableColName acc -> case acc of
                        Left _ -> acc
                        Right trueAcc -> case filterAllBy
                            (\((tableName, colName), _) -> case selectTableColName of
                                (Nothing, selectColName) -> selectColName == colName
                                (Just selectTableName, selectColName) -> selectTableName == tableName && selectColName == colName)
                            columnsWithValues of
                            [] -> Left $ "Could not find column " ++ show selectTableColName ++ " in 'select'"
                            match@((_, colType), value):[] -> Right $ ((selectTableColName, colType), value):trueAcc
                            match:matches -> Left $ "Matched column " ++ show selectTableColName ++ " more than once in 'select'")
                    (reverse columnNames)
                    (Right [])
executeStatement (ShowTableStatement tableToShow') database' = do
    case tableToShow' of
        Nothing -> Right $ DataFrame
            [ Column "table_name" StringType ]
            [ [ StringValue (fst table) ] | table <- database' ]
        Just tableName' -> do
            DataFrame cols _ <- maybe (Left $ "Could not find table " ++ tableName')
                (Right)
                (findTableByName database' tableName')
            Right $ DataFrame
                [ Column "column_name" StringType, Column "column_type" StringType]
                [ [StringValue name, StringValue $ show colType] | Column name colType <- cols ]
executeStatement (UpdateStatement tableName' assignedValues' whereArgs') database'  = do
    table@(DataFrame cols rows) <- maybe (Left $ "Could not find table " ++ tableName')
        (Right)
        (lookup tableName' database')
    let colNames = getColumnNames cols
    let colLookupList = zip colNames cols
    _ <- guardCheck (any (\(name, _) -> notElem name colNames) assignedValues')
        $ "Non-existant columns referenced in 'set'"
    _ <- guardCheck (not $ all (\(name, value) -> let Just col = lookup name colLookupList in valueCompatibleWithColumn value col) assignedValues')
        $ "At least one value is incompatible with column in 'set'"
    _ <- guardCheck (any (\name -> notElem name colNames) whereColNames)
        $ "Non-existant columns referenced in 'where'"
    let (updateColNames, updateColValues) = unzip assignedValues'
    Right $ DataFrame
        cols
        [ifElse (executeWhere (zip colNames row) whereArgs')
            (applyChanges (zip colNames row) assignedValues')
            row
            | row <- rows]
    where
        whereColNames :: [String]
        whereColNames = [colName | ((ColumnName (_, colName)), _, _) <- whereArgs']
            ++ [colName | (_, (ColumnName (_, colName)), _) <- whereArgs']
        applyChanges :: [(String, Value)] -> [(String, Value)] -> [Value]
        applyChanges xs ys = [maybe value (id) (lookup name ys) | x@(name, value) <- xs]
            where
        executeWhere :: [(String, Value)] -> [(WhereOperand, WhereOperand, WhereOperator)] -> Bool
        executeWhere namedRows whereOperations = nullOrAny (\(lVal, rVal, op) -> do
            let lVal' = getValue lVal
            let rVal' = getValue rVal
            op lVal' rVal')
            whereOperations
            where
                getValue :: WhereOperand -> String
                getValue x = case x of
                    Constant str -> str
                    ColumnName (_, str) -> case lookup str namedRows of
                        Just (StringValue val) -> val
                        _ -> ""
executeStatement (InsertIntoStatement tableName' valuesOrder' values') database' = do
    table@(DataFrame cols rows) <- maybe (Left $ "Could not find table " ++ tableName')
        (Right)
        (lookup tableName' database')
    let colNames = getColumnNames cols
    _ <- guardCheck (length values' /= length cols)
        $ "All values must be explicitly written in 'values'"
    _ <- guardCheck (maybe False (\x -> length values' /= length x) valuesOrder')
        $ "Length mismatch between column names and column values"
    let assignedValues' = maybe (zip (getColumnNames cols) values') (\x -> zip x values') valuesOrder'
    _ <- guardCheck (not $ valuesCompatibleWithColumns assignedValues' cols)
        $ "At least one value is incompatible with column in 'values'"
    _ <- guardCheck (maybe False (\x -> any (\y -> notElem y colNames) x) valuesOrder')
        $ "At least one value column name is not a valid column in table"
    let newRow = (snd $ unzip (alignValues assignedValues' colNames))
    Right $ DataFrame
        cols
        (reverse (newRow:(reverse rows)))
executeStatement (DeleteStatement tableName' whereArgs') database' = do
    table@(DataFrame cols rows) <- maybe (Left $ "Could not find table " ++ tableName')
        (Right)
        (lookup tableName' database')
    let colNames = getColumnNames cols
    _ <- guardCheck (any (\colName -> notElem colName (getColumnNames cols)) whereColNames)
        $ "Non-existant columns referenced in 'where'"
    _ <- guardCheck (any (\colName -> let Just x = lookup colName (zip colNames cols) in not $ isStringColumn x) whereColNames)
        $ "Only string columns allowed in 'where'"
    Right $ DataFrame
        cols
        [row | row <- rows, not $ executeWhere (zip colNames row) whereArgs']
    where
        isStringColumn :: Column -> Bool
        isStringColumn (Column _ StringType) = True
        isStringColumn _ = False
        whereColNames :: [String]
        whereColNames = [colName | ((ColumnName (_, colName)), _, _) <- whereArgs']
            ++ [colName | (_, (ColumnName (_, colName)), _) <- whereArgs']
        executeWhere :: [(String, Value)] -> [(WhereOperand, WhereOperand, WhereOperator)] -> Bool
        executeWhere namedRows whereOperations = nullOrAny (\(lVal, rVal, op) -> do
            let lVal' = getValue lVal
            let rVal' = getValue rVal
            op lVal' rVal')
            whereOperations
            where
                getValue :: WhereOperand -> String
                getValue x = case x of
                    Constant str -> str
                    ColumnName (_, str) -> case lookup str namedRows of
                        Just (StringValue val) -> val
executeStatement (CreateTableStatement tablename' columns') database' = do
    _ <- guardCheck (not $ null $ lookup tablename' database')
        $ "Table by name '" ++ tablename' ++ "' already exists"
    _ <- guardCheck (null columns') $ "Cannot create table with no columns"
    Right $ DataFrame columns' []
executeStatement (DropTableStatement tablename') database' = do
    _ <- guardCheck (null $ lookup tablename' database')
        $ "Table by name '" ++ tablename' ++ "' does not exist"
    executeStatement
        (ShowTableStatement Nothing)
        [table | table@(tableName'', _) <- database', tableName'' /= tablename']
executeStatement _ _ = Left $ "Unknown unsupported statement"

nullOrAny :: (Foldable t) => (a -> Bool) -> t a -> Bool
nullOrAny f x = null x || any f x

-- True - first, False - second
ifElse :: Bool -> a -> a  -> a
ifElse b x y
    | b = x
    | otherwise = y

guardCheck :: Bool -> ErrorMessage -> Either ErrorMessage ()
guardCheck b failMessage = ifElse b (Left failMessage) (Right ())

getColumnNames :: [Column] -> [String]
getColumnNames columns = [colName | (Column colName _) <- columns]

showParsedStatementType :: Either ErrorMessage ParsedStatement -> String
showParsedStatementType x = case x of
    Right (SelectStatement _ _ _ _) -> "SelectStatement"
    Right (ShowTableStatement _) -> "ShowTableStatement"
    Right (UpdateStatement _ _ _) -> "UpdateStatement"
    Right (InsertIntoStatement _ _ _) -> "InsertIntoStatement"
    Right (DeleteStatement _ _) -> "DeleteStatement"
    _ -> "ErrorMessage"

valueCompatibleWithColumn :: Value -> Column -> Bool
valueCompatibleWithColumn (IntegerValue _) (Column _ IntegerType) = True
valueCompatibleWithColumn (StringValue _) (Column _ StringType) = True
valueCompatibleWithColumn (BoolValue _) (Column _ BoolType) = True
valueCompatibleWithColumn (NullValue) _ = True
valueCompatibleWithColumn _ _ = False

valuesCompatibleWithColumns :: [(String, Value)] -> [Column] -> Bool
valuesCompatibleWithColumns xs ys = all (\(name, value) -> case lookup name (zip (getColumnNames ys) ys) of
    Nothing -> False
    Just y -> valueCompatibleWithColumn value y)
    xs

-- First argument - what to align, second argument - with what to align
alignValues :: Eq a => [(a, b)] -> [a] -> [(a, b)]
alignValues whats withWhats = [what | withWhat <- withWhats, what@(whatArg, _) <- whats, withWhat == whatArg]


cartesianProduct :: [[a]] -> [[a]] -> [[a]]
cartesianProduct xs ys = [x ++ y | x <- xs, y <- ys]
listCartesianProduct :: [[[a]]] -> [[a]]
listCartesianProduct [] = []
listCartesianProduct (x:xs) = listCartesianProduct' xs x
    where
        listCartesianProduct' :: [[[a]]] -> [[a]] -> [[a]]
        listCartesianProduct' [] acc = acc
        listCartesianProduct' (y:ys) acc = listCartesianProduct' ys (cartesianProduct acc y)


decomposeListList :: [[a]] -> [a]
decomposeListList xss = [x | xs <- xss, x <- xs]

findAllBy :: (a -> Bool) -> [(a, b)] -> [b]
findAllBy needleFunc haystack = [value | (key, value) <- haystack, needleFunc key]

findBy :: (a -> Bool) -> [(a, b)] -> Maybe b
findBy needleFunc haystack = case findAllBy needleFunc haystack of
    [] -> Nothing
    x:_ -> Just x

filterAllBy :: (a -> Bool) -> [(a, b)] -> [(a, b)]
filterAllBy needleFunc haystack = [(key, value) | (key, value) <- haystack, needleFunc key]

forEach :: (a -> b -> b) -> [a] -> b -> b
forEach func (x:xs) acc = forEach func xs (func x acc)
forEach _ [] acc = acc

anyRight :: [Either ([(Maybe String, String)], Function) (Maybe String, String)] -> Bool
anyRight [] = False
anyRight (x:xs) = case x of
  Right (Just "datetime", "datetime") -> anyRight xs
  Right _ -> True
  Left _ -> anyRight xs

class NewHead a where
  head' :: [a] -> a

instance NewHead Char where
  head' [] = toEnum 0
  head' (x:xs) = x

instance NewHead String where
  head' [] = ""
  head' (x:xs) = x

------------ For testing ------------

instance Eq ParsedStatement where
  SelectStatement sa1 fa1 wa1 oba1 == SelectStatement sa2 fa2 wa2 oba2 =
    sa1 == sa2 && fa1 == fa2 && wa1 == wa2 && oba1 == oba2
  ShowTableStatement sa1 == ShowTableStatement sa2 =
    sa1 == sa2
  UpdateStatement tn1 av1 wa1 == UpdateStatement tn2 av2 wa2 =
    tn1 == tn2 && av1 == av2 && wa1 == wa2
  InsertIntoStatement tn1 vo1 v1 == InsertIntoStatement tn2 vo2 v2 =
    tn1 == tn2 && vo1 == vo2 && v1 == v2
  DeleteStatement tn1 wa1 == DeleteStatement tn2 wa2 =
    tn1 == tn2 && wa1 == wa2
  _ == _ = False

instance Show Function where
  show (Func0 _) = " NOW() "
  show (Func1 _) = " MAX(something) or SUM(something) "

instance Show WhereOperator where
  show _ = "(= | <> | <= | >=, OR)"

instance Show ParsedStatement where
  show (SelectStatement sa fa wa oba) = "SelectStatement: "
    ++ show sa ++ " "
    ++ show fa ++ " "
    ++ show wa ++ " "
    ++ show oba
  show (ShowTableStatement sta) = "ShowTableStatement: " ++ show sta
  show (UpdateStatement tn av wa) = "UpdateStatement: " ++ show tn ++ " " ++ show av ++ " " ++ show wa
  show (InsertIntoStatement tn vo v) = "InsertIntoStatement: "  ++ show tn ++ " " ++ show vo ++ " " ++ show v
  show (DeleteStatement tn wa) = "DeleteStatement: " ++ show tn ++ " " ++ show wa

instance Eq Function where 
  (Func0 _) == (Func0 _) = True
  (Func1 _) == (Func1 _) = True
  _ == _ = False

instance Eq WhereOperator where -- WhereOperator
  _ == _ = True

instance Eq WhereOperand where
  Constant a == Constant b = a == b
  ColumnName a == ColumnName b = a == b
  Constant _ == ColumnName _ = False
  ColumnName _ == Constant _ = False

now' = now
max' = Lib2.max
sum' = Lib2.sum

