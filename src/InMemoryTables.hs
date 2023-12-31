module InMemoryTables
  ( tableEmployees,
    tableInvalid1,
    tableInvalid2,
    tableLongStrings,
    tableWithNulls,
    database,
    TableName,
  )
where

import DataFrame (Column (..), ColumnType (..), DataFrame (..), Value (..))

type TableName = String

tableEmployees :: (TableName, DataFrame)
tableEmployees =
  ( "employees",
    DataFrame
      [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
      [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
        [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
      ]
  )

tableJobs :: (TableName, DataFrame)
tableJobs =
  ( "jobs",
    DataFrame
      [Column "id" IntegerType, Column "title" StringType, Column "employeeId" IntegerType]
      [ [IntegerValue 8, StringValue "Assistant", IntegerValue 2],
        [IntegerValue 9, StringValue "Lecturer", IntegerValue 1]
      ]
  )

tableInvalid1 :: (TableName, DataFrame)
tableInvalid1 =
  ( "invalid1",
    DataFrame
      [Column "id" IntegerType]
      [ [StringValue "1"]
      ]
  )

tableInvalid2 :: (TableName, DataFrame)
tableInvalid2 =
  ( "invalid2",
    DataFrame
      [Column "id" IntegerType, Column "text" StringType]
      [ [IntegerValue 1, NullValue],
        [IntegerValue 1]
      ]
  )

longString :: Value
longString =
  StringValue $
    unlines
      [ "Lorem ipsum dolor sit amet, mei cu vidisse pertinax repudiandae, pri in velit postulant vituperatoribus.",
        "Est aperiri dolores phaedrum cu, sea dicit evertitur no. No mei euismod dolorem conceptam, ius ne paulo suavitate.",
        "Vim no feugait erroribus neglegentur, cu sed causae aeterno liberavisse,",
        "his facer tantas neglegentur in. Soleat phaedrum pri ad, te velit maiestatis has, sumo erat iriure in mea.",
        "Numquam civibus qui ei, eu has molestiae voluptatibus."
      ]

tableLongStrings :: (TableName, DataFrame)
tableLongStrings =
  ( "long_strings",
    DataFrame
      [Column "text1" StringType, Column "text2" StringType]
      [ [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString]
      ]
  )

tableWithNulls :: (TableName, DataFrame)
tableWithNulls =
  ( "flags",
    DataFrame
      [Column "flag" StringType, Column "value" BoolType]
      [ [StringValue "a", BoolValue True],
        [StringValue "b", BoolValue True],
        [StringValue "b", NullValue],
        [StringValue "b", BoolValue False]
      ]
  )

tableWithDuplicateColumns :: (TableName, DataFrame)
tableWithDuplicateColumns = (
    "duplicates",
    DataFrame
        [Column "x" StringType, Column "y" StringType]
        [
            [StringValue "a", StringValue "a"],
            [StringValue "a", StringValue "b"],
            [StringValue "b", StringValue "a"],
            [StringValue "b", StringValue "b"]
        ]
    )

cartesianProductTestOne :: (TableName, DataFrame)
cartesianProductTestOne = (
    "cartProdTestOne",
    DataFrame
        [Column "a" StringType, Column "b" StringType]
        [
            [StringValue "a", StringValue "a"],
            [StringValue "a", StringValue "b"]
        ]
    )

cartesianProductTestTwo :: (TableName, DataFrame)
cartesianProductTestTwo = (
    "cartProdTestTwo",
    DataFrame
        [Column "a" StringType, Column "b" StringType]
        [
            [StringValue "x", StringValue "x"],
            [StringValue "x", StringValue "y"]
        ]
    )

tableNoRows :: (TableName, DataFrame)
tableNoRows = (
    "noRows",
    DataFrame
        [Column "x" StringType, Column "y" StringType]
        []
    )

database :: [(TableName, DataFrame)]
database = [tableEmployees, tableInvalid1, tableInvalid2,
        tableLongStrings, tableWithNulls, tableWithDuplicateColumns,
        tableNoRows, cartesianProductTestOne, cartesianProductTestTwo, tableJobs]
