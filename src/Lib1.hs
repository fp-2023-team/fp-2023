{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
    toLower,
    toLowerStr,
  )
where

-- Note(Almantas Mecele): Yes, this is modifying code outside of the allowed area.
-- I don't see why importing more from the DataFrame module would be a bad idea;
--import DataFrame (DataFrame)
import DataFrame
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
-- Credit: Almantas Mecele
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName database tableName = lookup tableName database

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
-- Credit: Almantas Mecele
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement inputStr
    | length truncSplitStr < 4 = Left "Too short of a query"
    | truncSplitStr !! 0 /= "select" = Left "First word not 'select'"
    | truncSplitStr !! 1 /= "*" = Left "Column selection not a wild card"
    | truncSplitStr !! 2 /= "from" = Left "Third word not 'from'"
    | otherwise = Right $ unwords $ drop 3 truncSplitStr
    where
        truncSplitStr = words $ toLowerStr $ takeWhile (/= ';') inputStr

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
-- Credit: Almantas Mecele
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame cols rows)
    | any (\row -> length row /= colLength) rows = Left "Column count and row length mismatch"
    | any (\row -> any (\zipped -> not $ compatibleType zipped) (zip cols row)) rows = Left "Column type and row value mismatch"
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
renderDataFrameAsTable givenTerminalWidth (DataFrame columns rows)
  = unlines $ headerRow : separatorRow : dataRows
  where
    minimumTerminalWidth = 20
    maximumTerminalWidth = 120
    terminalWidth :: Int
    terminalWidth = min maximumTerminalWidth $ max minimumTerminalWidth (fromInteger givenTerminalWidth)  -- tructates and adds ... if sum of column widths exeeds it.
    -- Column represented by [Value] maps into widths and takes it maximum. Maps [[Value]] Calumns into [Int] widthMaximums.
    columnWidths = reduceWidths unboundColumnWidths
      where 
        unboundColumnWidths = map (\((Column name _), width) -> max (length name) width ) $ zip columns (map (maximum . map valueWidth) (getValues columns rows))
        reduceWidths :: [Int] -> [Int]
        reduceWidths widths
            | sum widths <= terminalWidth = widths
            | otherwise = map (\width -> if width > average then average else width) widths
        average = terminalWidth `div` (length columns)
    

    headerRow = renderRow $ map (\(column, width) -> renderColumnValue column width) (zip columns columnWidths)
    separatorRow = separatorLine columnWidths 
    dataRows = map renderRow $ map (\row -> map (\(value, width) -> renderRowValue value width) (zip row columnWidths) ) rows

    showValue :: Value -> String
    showValue (IntegerValue i) = show i
    showValue (StringValue str) = str
    showValue (BoolValue True) = "True"
    showValue (BoolValue False) = "False"
    showValue (NullValue) = "null"
    
    valueWidth :: Value -> Int
    valueWidth val = length $ showValue val
    
    getValues :: [Column] -> [Row] -> [[Value]]
    getValues columns' rows' = map (\i -> map (\row -> row !! i) rows') [0..(length columns' - 1)] -- transpose matrix, so [Value] is column of values
    
    separatorLine :: [Int] -> String
    separatorLine widths = (foldl (\acc str -> acc ++ str) "" $ map (\width -> (widthToLine $ width + 2) ++ "+") $ init widths) ++ (widthToLine $ (last widths) + 2)
      where widthToLine width = take width $ repeat '-'
    

    renderRow :: [String] -> String
    renderRow rowValues = (foldl (\acc str -> acc ++ " " ++ str ++ " |" ) "" $ init rowValues) ++ " " ++ last rowValues
    

    renderRowValue :: Value -> Int -> String
    renderRowValue val width = padAlignLeft (reduceValue (showValue val) width) width

    renderColumnValue :: Column -> Int -> String
    renderColumnValue (Column name _) width = padAlignCenter (reduceValue name width) width

    reduceValue :: String -> Int -> String
    reduceValue str width
      | length str > width = (take (width-2) str) ++ ".."
      | otherwise = str
    

    padAlignLeft :: String -> Int -> String
    padAlignLeft str width = str ++ (take (width - (length str)) $ repeat ' ') 

    padAlignCenter :: String -> Int -> String
    padAlignCenter str width =
      let padding = width - length str
          leftPadding = padding `div` 2
          rightPadding = padding - leftPadding
      in replicate leftPadding ' ' ++ str ++ replicate rightPadding ' '
