{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

-- Note(Almantas Mecele): Yes, this is modifying code outside of the allowed area.
-- I don't see why importing more from the DataFrame module would be a bad idea;
--import DataFrame (DataFrame)
import DataFrame (DataFrame (..), Column (..), Row, ColumnType (..), Value (..))
import InMemoryTables (TableName)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

toLower :: Char -> Char
toLower ch
    | elem ch ['A'..'Z'] = toEnum $ fromEnum ch + 32
    | otherwise = ch

toLowerStr :: [Char] -> [Char]
toLowerStr str = [toLower ch | ch <- str]

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName database tableName = lookup (toLowerStr tableName) database

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement inputStr
    | length truncSplitStr < 4 = Left "too short of a query"
    | truncSplitStr !! 0 /= "select" = Left "first word not 'select'"
    | truncSplitStr !! 1 /= "*" = Left "column selection not a wild card"
    | truncSplitStr !! 2 /= "from" = Left "third word not 'from'"
    | otherwise = Right $ unwords $ drop 3 truncSplitStr
    where
        truncSplitStr = words $ toLowerStr $ takeWhile (/= ';') inputStr

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame cols rows)
    | any (\row -> length row /= colLength) rows = Left "column count and row length mismatch"
    | any (\row -> any (\zipped -> not $ compatibleType zipped) (zip cols row)) rows = Left "column type and row value mismatch"
    | otherwise = Right ()
    where
        colLength :: Int
        colLength = length cols
        compatibleType :: (Column, Value) -> Bool
        compatibleType ((Column _ IntegerType), (IntegerValue _)) = True
        compatibleType ((Column _ StringType), (StringValue _)) = True
        compatibleType ((Column _ BoolType), (BoolValue _)) = True
        compatibleType (_, (NullValue)) = True
        compatibleType (_, _) = False

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable terminalWidth (DataFrame columns rows)
  = unlines $ headerRow : separatorRow : dataRows
  where
    columnWidths = map (maximum . map valueWidth) (transposeValueRows columns rows)
    separatorRow = separatorLine columnWidths
    headerRow = renderRow (map columnName columns) columnWidths
    dataRows = map (renderRowWithValues columnWidths) rows

    transposeValueRows :: [Column] -> [Row] -> [[Value]]
    transposeValueRows columns' rows' =
      [ [row !! i | row <- rows'] | i <- [0..length columns' - 1] ]

    valueWidth :: Value -> Int
    valueWidth NullValue = 4
    valueWidth (IntegerValue x) = length (show x)
    valueWidth (StringValue s) = length s
    valueWidth (BoolValue True) = 4
    valueWidth (BoolValue False) = 5

    separatorLine :: [Int] -> String
    separatorLine widths = "+-" ++ concatMap (`replicate` '-') widths' ++ "-+"
      where
        widths' = map (\w -> w + 2) widths

    renderRow :: [String] -> [Int] -> String
    renderRow columns' widths =
      "| " ++ renderRowValues columns' widths ++ " |"

    renderRowWithValues :: [Int] -> Row -> String
    renderRowWithValues widths row =
      "| " ++ renderRowValues (map showValue row) widths ++ " |"

    renderRowValues :: [String] -> [Int] -> String
    renderRowValues [] [] = ""
    renderRowValues (col:cols) (w:ws) =
      padValue col w ++ " | " ++ renderRowValues cols ws

    columnName :: Column -> String
    columnName (Column name _) = name

    showValue :: Value -> String
    showValue NullValue = "null"
    showValue (IntegerValue x) = show x
    showValue (StringValue s) = s
    showValue (BoolValue True) = "True"
    showValue (BoolValue False) = "False"

    padValue :: String -> Int -> String
    padValue value width = take width (value ++ repeat ' ')
