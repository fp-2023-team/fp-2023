{-# LANGUAGE QuasiQuotes #-}
import Text.RawString.QQ
import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import Lib3
import DataFrame
import Test.Hspec
import Data.Maybe
import Test.Hspec (shouldBe)
import Data.Either (Either(Right))
import DataFrame (DataFrame(DataFrame), ColumnType (BoolType, IntegerType), Value (IntegerValue))


main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
      -- Commented out because of the changed requirements: names are now case sensitive
--    it "can find by case-insensitive name" $ do
--      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()
  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null

  describe "Lib2.parseStatement" $ do
    it "parses a show tables statement" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` (parseTest 0)
    it "parses a show tables statement case insensitively" $ do
      Lib2.parseStatement "ShoW TAbLeS;" `shouldBe` (parseTest 0)
    it "parses a show table statement" $ do
      Lib2.parseStatement "SHOW TABLE employees;" `shouldBe` (parseTest 1)
    it "parses a select statement with columns" $ do
      Lib2.parseStatement "SELECT id, surname FROM employees;" `shouldBe` (parseTest 2)
    it "does not parse an invalid select statement" $ do
      Lib2.parseStatement "SLECT id, birthday FROM employees;" `shouldSatisfy` isLeft
    it "parses a max function" $ do
      Lib2.parseStatement "SELECT MAX(id) FROM employees;" `shouldBe` (parseTest 3)
    it "does not parse an invalid max function" $ do
      Lib2.parseStatement "SELECT MAaX(id) FROM employees;" `shouldSatisfy` isLeft
    it "parses a sum function" $ do
      Lib2.parseStatement "SELECT SUM(id) FROM employees;" `shouldBe` (parseTest 3)
    it "does not parse an invalid sum function" $ do
      Lib2.parseStatement "SELECT SUMN(id) FROM employees;" `shouldSatisfy` isLeft
    it "parses a where or function with strings, = comparison" $ do
      Lib2.parseStatement "SELECT * FROM duplicates WHERE x = 'a' OR y = 'a';" `shouldBe` (parseTest 4)
    it "parses a where function with strings, <> comparison" $ do
      Lib2.parseStatement "SELECT * FROM duplicates WHERE x <> y;" `shouldBe` (parseTest 5)
    it "parses executes a where or function with strings, >= comparison" $ do
      Lib2.parseStatement "SELECT id FROM employees WHERE 'a' >= 'b' OR name >= 'Z';" `shouldBe` (parseTest 6)
    it "parses a where or function with strings, <= comparison, combined with sum" $ do
      Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= 'E' or surname <= 'E';" `shouldBe` (parseTest 7)
  describe "Lib2.executeStatement" $ do
    it "executes a show tables statement" $ do
      case Lib2.parseStatement "SHOW TABLES;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right testRes1
    it "executes a show tables statement case insensitively" $ do
      case Lib2.parseStatement "ShoW TAbLeS;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right testRes1
    it "executes a show table statement" $ do
      case Lib2.parseStatement "SHOW TABLE employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right testRes2
    it "does not execute a show table statement with a case insensitive name" $ do
      case Lib2.parseStatement "SHOW TABLE emplOyEes;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps D.database `shouldSatisfy` isLeft
    it "executes a select statement with columns" $ do
      case Lib2.parseStatement "SELECT id, surname FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right testRes3
    it "does not execute a select statement with wrong columns" $ do
      case Lib2.parseStatement "SELECT id, birthday FROM employees;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps D.database `shouldSatisfy` isLeft
    it "executes a max function" $ do
      case Lib2.parseStatement "SELECT MAX(id) FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                          [[IntegerValue 2]])
    it "executes a sum function" $ do
      case Lib2.parseStatement "SELECT SUM(id) FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                          [[IntegerValue 3]])
    it "executes a where or function with strings, = comparison" $ do
      case Lib2.parseStatement "SELECT * FROM duplicates WHERE x = 'a' OR y = 'a';" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right testRes4
    it "executes a where function with strings, <> comparison" $ do
      case Lib2.parseStatement "SELECT * FROM duplicates WHERE x <> y;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right testRes5
    it "executes a where or function with strings, >= comparison" $ do
      case Lib2.parseStatement "SELECT id FROM employees WHERE 'a' >= 'b' OR name >= 'Z';" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                        [[NullValue]])
    it "executes a where or function with strings, <= comparison, combined with sum" $ do
      case Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= 'E' or surname <= 'E';" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                        [[IntegerValue 2]])
{-
  ---------------------------------------------------------------------------------------------------------------
  describe "Lib2.parseStatement Task 3" $ do
    it "parses a show tables statement" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` (parseTest 0)
  
  describe "Lib3.deserialize" $ do
    it "deserializes valid table" $ do
      Lib3.deserialize [r|
        [
          [
            ["id","IT"],
            ["name","ST"],
            ["surname","ST"],
            ["isActive","BT"]
          ],
          [
            [
              {"contents":1,"tag":"IV"},
              {"contents":"Vi","tag":"SV"},
              {"contents":"Po","tag":"SV"},
              {"contents":true,"tag":"BV"}
            ],
            [
              {"contents":2,"tag":"IV"},
              {"contents":"Ed","tag":"SV"},
              {"contents":"Dl","tag":"SV"},
              {"contents":false,"tag":"BV"}
            ],
            [
              {"tag":"NV"},
              {"tag":"NV"},
              {"tag":"NV"},
              {"tag":"NV"}
            ]
          ]
        ]
      |]
      `shouldBe` Just (DataFrame
        [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType, Column "isActive" BoolType]
        [
          [IntegerValue 1, StringValue "Vi", StringValue "Po", BoolValue True],
          [IntegerValue 2, StringValue "Ed", StringValue "Dl", BoolValue False],
          [NullValue, NullValue, NullValue, NullValue]
        ])
        
    it "deserializes empty table" $ do
      Lib3.deserialize [r|
        [
          [
            ["id","IT"],
            ["name","ST"],
            ["surname","ST"],
            ["isActive","BT"]
          ],
          []
        ]
      |]
      `shouldBe` Just (DataFrame
        [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType, Column "isActive" BoolType]
        [])

    it "parses minimal table" $ do
      Lib3.deserialize [r|
        [
          [
            ["id","IT"]
          ],
          [
            [
              {"contents":1,"tag":"IV"}
            ]
          ]
        ]
      |]
      `shouldBe` Just (DataFrame 
      [Column "id" IntegerType]
      [
        [IntegerValue 1]
      ])

    it "nothing if json is malformed" $ do
      Lib3.deserialize [r|
        [
          
            ["id","IT"]
          ],
          [
            [
              {"contents":1,"tag":"IV"}
            ]
          ]
        ]
      |]
      `shouldBe` (Nothing :: Maybe DataFrame)

    it "nothing if value doesn't match the type" $ do
      Lib3.deserialize [r|
        [
          [
            ["id","IT"]
          ],
          [
            [
              {"contents":"a string","tag":"IV"}
            ]
          ]
        ]
      |]
      `shouldBe` (Nothing :: Maybe DataFrame)

    it "nothing if missing required fields" $ do
      Lib3.deserialize [r|
        [
          [
            ["id","IT"]
          ],
          [
            [
              {"contents":"a string"}
            ]
          ]
        ]
      |]
      `shouldBe` (Nothing :: Maybe DataFrame)
    it "nothing if more fields than required" $ do
      Lib3.deserialize [r|
        [
          [
            ["id","IT"]
          ],
          [
            [
              {"contents":"a string"},
              {"contents":"a string"}
            ]
          ]
        ]
      |]
      `shouldBe` (Nothing :: Maybe DataFrame)
  describe "Lib3.serialize" $ do
    it "serializes valid table" $ do
      Lib3.serialize (DataFrame
        [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType, Column "isActive" BoolType]
        [
          [IntegerValue 1, StringValue "Vi", StringValue "Po", BoolValue True],
          [IntegerValue 2, StringValue "Ed", StringValue "Dl", BoolValue False],
          [NullValue, NullValue, NullValue, NullValue]
        ]) `shouldBe` 
       filter (\x -> x /= ' ' && x /= '\n') [r|
        [
          [
            ["id","IT"],
            ["name","ST"],
            ["surname","ST"],
            ["isActive","BT"]
          ],
          [
            [
              {"contents":1,"tag":"IV"},
              {"contents":"Vi","tag":"SV"},
              {"contents":"Po","tag":"SV"},
              {"contents":true,"tag":"BV"}
            ],
            [
              {"contents":2,"tag":"IV"},
              {"contents":"Ed","tag":"SV"},
              {"contents":"Dl","tag":"SV"},
              {"contents":false,"tag":"BV"}
            ],
            [
              {"tag":"NV"},
              {"tag":"NV"},
              {"tag":"NV"},
              {"tag":"NV"}
            ]
          ]
        ]
      |]
      
        
    it "deserializes empty table" $ do
      Lib3.serialize (DataFrame
        [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType, Column "isActive" BoolType]
        [])
      `shouldBe`
      filter (\x -> x /= ' ' && x /= '\n') [r|
        [
          [
            ["id","IT"],
            ["name","ST"],
            ["surname","ST"],
            ["isActive","BT"]
          ],
          []
        ]
      |]

    it "parses minimal table" $ do
      Lib3.serialize (DataFrame 
        [Column "id" IntegerType]
        [
          [IntegerValue 1]
        ])
      `shouldBe`
      filter (\x -> x /= ' ' && x /= '\n') [r|
        [
          [
            ["id","IT"]
          ],
          [
            [
              {"contents":1,"tag":"IV"}
            ]
          ]
        ]
      |]
  describe "Lib2.parseStatement updated (For task 3)" $ do
    it "selects columns from multiple tables" $ do
      Lib2.parseStatement "SELECT employees.name, jobs.title, departments.location FROM employees, jobs, departments;" 
      `shouldBe` 
      Right (SelectStatement {
        selectArgs = [Right (Just "employees", "name"), Right (Just "jobs", "title"), Right (Just "departments", "location")],
        fromArgs = ["employees", "jobs", "departments"],
        whereArgs = []})
    it "selects using now()" $ do
      Lib2.parseStatement "SELECT NOW();" 
      `shouldBe` 
      Right (SelectStatement {
        selectArgs = [Left ([], Func0 "")],
        fromArgs = [],
        whereArgs = []})
      

-}


testRes1 :: DataFrame
testRes1 = DataFrame
  [Column "table_name" StringType]
  [ 
    [StringValue "employees"],
    [StringValue "invalid1"],
    [StringValue "invalid2"],
    [StringValue "long_strings"],
    [StringValue "flags"],
    [StringValue "duplicates"]
  ]

testRes2 :: DataFrame
testRes2 = DataFrame
  [Column "column_name" StringType, Column "column_type" StringType]
  [
    [StringValue "id", StringValue "IntegerType"],
    [StringValue "name", StringValue "StringType"],
    [StringValue "surname", StringValue "StringType"]
  ]

testRes3 :: DataFrame
testRes3 = DataFrame
  [Column "id" IntegerType, Column "surname" StringType]
  [ [IntegerValue 1, StringValue "Po"],
    [IntegerValue 2, StringValue "Dl"]
  ]

testRes4 :: DataFrame
testRes4 = DataFrame
  [Column "x" StringType, Column "y" StringType]
    [
        [StringValue "a", StringValue "a"],
        [StringValue "a", StringValue "b"],
        [StringValue "b", StringValue "a"]
    ]

testRes5 :: DataFrame
testRes5 = DataFrame
  [Column "x" StringType, Column "y" StringType]
    [
        [StringValue "a", StringValue "b"],
        [StringValue "b", StringValue "a"]
    ]



type ErrorMessage = String

parseTest :: Int -> Either ErrorMessage ParsedStatement
parseTest 0 = Right (ShowTableStatement {showTableArgs = Nothing})
parseTest 1 = Right (ShowTableStatement {showTableArgs = Just "employees"})
parseTest 2 = Right (SelectStatement {selectArgs = [Right (Nothing, "id"), Right (Nothing, "surname")], fromArgs = ["employees"], whereArgs = []})
parseTest 3 = Right (SelectStatement {selectArgs = [Left ([(Nothing, "id")], Func1 dummy1)], fromArgs = ["employees"], whereArgs = []})
parseTest 4 = Right (SelectStatement {selectArgs = [Right (Nothing, "*")], fromArgs = ["duplicates"], whereArgs = [(ColumnName (Nothing, "x"), Constant "a", dummy2), (ColumnName (Nothing, "y"), Constant "a", dummy2)]})
parseTest 5 = Right (SelectStatement {selectArgs = [Right (Nothing, "*")], fromArgs = ["duplicates"], whereArgs = [(ColumnName (Nothing, "x"), ColumnName(Nothing, "y"), dummy2)]})
parseTest 6 = Right (SelectStatement {selectArgs = [Right (Nothing, "id")], fromArgs = ["employees"], whereArgs = [(Constant "a", Constant "b", dummy2), (ColumnName (Nothing, "name"), Constant "Z", dummy2)]})
parseTest 7 = Right (SelectStatement {selectArgs = [Left ([(Nothing, "id")], Func1 dummy1)], fromArgs = ["employees"], whereArgs = [(ColumnName (Nothing, "name"), Constant "E", dummy2), (ColumnName (Nothing, "surname"), Constant "E", dummy2)]})
parseTest _ = Left "error"

dummy1 :: [Value] -> Value
dummy1 _ = NullValue

dummy2 :: String -> String -> Bool
dummy2 a b = True


