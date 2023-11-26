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
import qualified Lib2
import qualified Lib2
import qualified Data.Ord as Lib2
import Text.Read (Lexeme(String))


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
----------------------------------------------------------------------------------------------------------------------------------------------
{-
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
        Right ps -> Lib2.executeStatement ps `shouldBe` Right testRes1
    it "executes a show tables statement case insensitively" $ do
      case Lib2.parseStatement "ShoW TAbLeS;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right testRes1
    it "executes a show table statement" $ do
      case Lib2.parseStatement "SHOW TABLE employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right testRes2
    it "does not execute a show table statement with a case insensitive name" $ do
      case Lib2.parseStatement "SHOW TABLE emplOyEes;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
    it "executes a select statement with columns" $ do
      case Lib2.parseStatement "SELECT id, surname FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right testRes3
    it "does not execute a select statement with wrong columns" $ do
      case Lib2.parseStatement "SELECT id, birthday FROM employees;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
    it "executes a max function" $ do
      case Lib2.parseStatement "SELECT MAX(id) FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                          [[IntegerValue 2]])
    it "executes a sum function" $ do
      case Lib2.parseStatement "SELECT SUM(id) FROM employees;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                          [[IntegerValue 3]])
    it "executes a where or function with strings, = comparison" $ do
      case Lib2.parseStatement "SELECT * FROM duplicates WHERE x = 'a' OR y = 'a';" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right testRes4
    it "executes a where function with strings, <> comparison" $ do
      case Lib2.parseStatement "SELECT * FROM duplicates WHERE x <> y;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right testRes5
    it "executes a where or function with strings, >= comparison" $ do
      case Lib2.parseStatement "SELECT id FROM employees WHERE 'a' >= 'b' OR name >= 'Z';" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                        [[NullValue]])
    it "executes a where or function with strings, <= comparison, combined with sum" $ do
      case Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= 'E' or surname <= 'E';" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                        [[IntegerValue 2]])

  ---------------------------------------------------------------------------------------------------------------
  describe "Lib2.parseStatement Task 3" $ do
    it "parses a show tables statement" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` (parseTest 0)
  -}
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

    it "deserializes minimal table" $ do
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
      
        
    it "serializes empty table" $ do
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

    it "serializes minimal table" $ do
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
    it "parses select with columns from multiple tables" $ do
      Lib2.parseStatement "SELECT employees.name, jobs.title, departments.location FROM employees, jobs, departments;" 
      `shouldBe` 
      Right (SelectStatement {
        selectArgs = [Right (Just "employees", "name"), Right (Just "jobs", "title"), Right (Just "departments", "location")],
        fromArgs = ["employees", "jobs", "departments"],
        whereArgs = []})
    it "parses select with columns from multiple tables without specifying table in select" $ do
      Lib2.parseStatement "SELECT name, title, location FROM employees, jobs, departments;" 
      `shouldBe` 
      Right (SelectStatement {
        selectArgs = [Right (Nothing, "name"), Right (Nothing, "title"), Right (Nothing, "location")],
        fromArgs = ["employees", "jobs", "departments"],
        whereArgs = []})
    it "parses select with columns from multiple tables without some specifying table in select" $ do
      Lib2.parseStatement "SELECT name, jobs.title, location FROM employees, jobs, departments;" 
      `shouldBe` 
      Right (SelectStatement {
        selectArgs = [Right (Nothing, "name"), Right (Just "jobs", "title"), Right (Nothing, "location")],
        fromArgs = ["employees", "jobs", "departments"],
        whereArgs = []})
    it "parses select with columns from multiple tables inside a function" $ do
      Lib2.parseStatement "SELECT MAX(employees.age), SUM(jobs.id) FROM employees, jobs;" 
      `shouldBe` 
      Right (SelectStatement {
        selectArgs = [Left ([(Just "employees", "age")], Func1 Lib2.max'), Left ([(Just "jobs", "id")], Func1 Lib2.sum')],
        fromArgs = ["employees", "jobs"],
        whereArgs = []})
    it "parses select with NOW() function" $ do
      Lib2.parseStatement "SELECT NOW();" 
      `shouldBe` 
      Right (SelectStatement {
        selectArgs = [Left ([], Func0 Lib2.now')],
        fromArgs = [],
        whereArgs = []})
    it "parses select with NOW() function and other mixed columns" $ do
      Lib2.parseStatement "SELECT NOW(), employees.name, jobs.title FROM employees, jobs;"
      `shouldBe`
      Right (SelectStatement {
        selectArgs = [Left ([], Func0 Lib2.now'), Right (Just "employees", "name"), Right(Just "jobs", "title")],
        fromArgs = ["employees", "jobs"],
        whereArgs = []})
    it "parses select with where clause" $ do
      Lib2.parseStatement "SELECT employees.name, jobs.title FROM employees, jobs WHERE employees.name = jobs.holder;"
      `shouldBe`
      Right (SelectStatement {
        selectArgs = [Right (Just "employees", "name"), Right (Just "jobs", "title")],
        fromArgs = ["employees", "jobs"],
        whereArgs = [( ColumnName (Just "employees", "name"), ColumnName (Just "jobs", "holder"),  whereEq)]})
    it "parses select with where OR clause" $ do
      Lib2.parseStatement "SELECT employees.name, jobs.title FROM employees, jobs WHERE employees.name = jobs.holder OR employees.jobId = jobs.id;"
      `shouldBe`
      Right (SelectStatement {
        selectArgs = [Right (Just "employees", "name"), Right (Just "jobs", "title")],
        fromArgs = ["employees", "jobs"],
        whereArgs = [
          (ColumnName (Just "employees", "name"), ColumnName (Just "jobs", "holder"), whereEq), 
          (ColumnName (Just "employees", "jobId"), ColumnName (Just "jobs", "id"),  whereEq)
        ]})
    it "parses select with where clause and no table names" $ do
      Lib2.parseStatement "SELECT employees.name, jobs.title FROM employees, jobs WHERE name = holder;"
      `shouldBe`
      Right (SelectStatement {
        selectArgs = [Right (Just "employees", "name"), Right (Just "jobs", "title")],
        fromArgs = ["employees", "jobs"],
        whereArgs = [( ColumnName (Nothing, "name"), ColumnName (Nothing, "holder"),  whereEq)]})


    it "parses update with string constant in where" $ do
      Lib2.parseStatement "UPDATE employees SET id = 5, name = 'New Name' WHERE name = 'Old Name';"
      `shouldBe`
      Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = [( ColumnName (Nothing, "name"), Constant "Old Name",  whereEq)]})
    it "parses update with number constant in where" $ do
      Lib2.parseStatement "UPDATE employees SET id = 5, name = 'New Name' WHERE id = 1;"
      `shouldBe`
      Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = [( ColumnName (Nothing, "id"), Constant "1",  whereEq)]})
    it "parses update with column name in where" $ do
      Lib2.parseStatement "UPDATE employees SET id = 5, name = 'New Name' WHERE id = otherId;"
      `shouldBe`
      Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = [( ColumnName (Nothing, "id"), ColumnName (Nothing, "otherId"),  whereEq)]})
    it "parses update keyword case insensitive" $ do
      Lib2.parseStatement "uPdAtE employees sET id = 5, name = 'New Name' WhErE id = otherId;"
      `shouldBe`
      Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = [( ColumnName (Nothing, "id"), ColumnName (Nothing, "otherId"),  whereEq)]})
    it "parses update with extra whitespace" $ do
      Lib2.parseStatement "UPDATE            employees    SET    id=  5,    name  ='New Name'  WHERE   id=otherId;"
      `shouldBe`
      Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = [( ColumnName (Nothing, "id"), ColumnName (Nothing, "otherId"),  whereEq)]})
    it "parses update without where" $ do
      Lib2.parseStatement "UPDATE employees SET id = 5, name = 'New Name';"
      `shouldBe`
      Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = []})
    it "error message on update without set" $ do
      Lib2.parseStatement "UPDATE employees WHERE id=otherId;"
      `shouldSatisfy`
      isLeft
    it "error message on update without table name specified" $ do
      Lib2.parseStatement "UPDATE SET id = 5 WHERE id=otherId;"
      `shouldSatisfy`
      isLeft
    it "error message on update without setting any fields" $ do
      Lib2.parseStatement "UPDATE employees SET WHERE id=otherId;"
      `shouldSatisfy`
      isLeft


    it "parses insert with column names provided" $ do
      Lib2.parseStatement "INSERT INTO employees (name, jobTitle) VALUES ('Inserted Name', 'Inserted Job Title');"
      `shouldBe`
      Right (InsertIntoStatement {
        tablename = "employees",
        valuesOrder = Just ["name", "jobTitle"],
        values = [StringValue "Inserted Name", StringValue "Inserted Job Title"]
      })
    it "parses insert without column names provided" $ do
      Lib2.parseStatement "INSERT INTO employees VALUES ('Inserted Name', 'Inserted Job Title');"
      `shouldBe`
      Right (InsertIntoStatement {
        tablename = "employees",
        valuesOrder = Nothing,
        values = [StringValue "Inserted Name", StringValue "Inserted Job Title"]
      })  
    it "parses with number constant as value" $ do
      Lib2.parseStatement "INSERT INTO employees (name, id) VALUES ('Inserted Name', 1);"
      `shouldBe`
      Right (InsertIntoStatement {
        tablename = "employees",
        valuesOrder = Just ["name", "id"],
        values = [StringValue "Inserted Name", IntegerValue 1]
      })
    it "parses insert with keywords case insensitive" $ do
      Lib2.parseStatement "iNSeRt inTo employees (name, jobTitle) vALueS ('Inserted Name', 'Inserted Job Title');"
      `shouldBe`
      Right (InsertIntoStatement {
        tablename = "employees",
        valuesOrder = Just ["name", "jobTitle"],
        values = [StringValue "Inserted Name", StringValue "Inserted Job Title"]
      })
    it "parses insert with extra whitespace" $ do
      Lib2.parseStatement "INSERT    INTO     employees  (  name,jobTitle  )  VALUES (  'Inserted Name'    ,    'Inserted Job Title'   ) ; "
      `shouldBe`
      Right (InsertIntoStatement {
        tablename = "employees",
        valuesOrder = Just ["name", "jobTitle"],
        values = [StringValue "Inserted Name", StringValue "Inserted Job Title"]
      })
    it "error message on insert when value missing" $ do
      Lib2.parseStatement "INSERT INTO employees (name, jobTitle) VALUES;"
      `shouldSatisfy`
      isLeft
    it "error message on insert when no table provided" $ do
      Lib2.parseStatement "INSERT INTO VALUES ('Inserted Name', 'Inserted Job Title');"
      `shouldSatisfy`
      isLeft
    it "error message on insert when no values provided" $ do
      Lib2.parseStatement "INSERT INTO employees (name, jobTitle) VALUES;"
      `shouldSatisfy`
      isLeft



    it "parses delete with string constant in where" $ do
      Lib2.parseStatement "DELETE FROM employees WHERE name = 'Employee Name';"
      `shouldBe`
      Right (DeleteStatement {
        tablename = "employees",
        whereArgs = [(ColumnName (Nothing, "name"), Constant "Employee Name", whereEq)]
      })
    it "parses delete with number constant in where" $ do
      Lib2.parseStatement "DELETE FROM employees WHERE id = 1;"
      `shouldBe`
      Right (DeleteStatement {
        tablename = "employees",
        whereArgs = [(ColumnName (Nothing, "id"), Constant "1", whereEq)]
      })
    it "parses delete with column name in where" $ do
      Lib2.parseStatement "DELETE FROM employees WHERE name = surname;"
      `shouldBe`
      Right (DeleteStatement {
        tablename = "employees",
        whereArgs = [(ColumnName (Nothing, "name"), ColumnName (Nothing, "surname"), whereEq)]
      })
    it "parses delete keywords case insensitive" $ do
      Lib2.parseStatement "dElEtE FrOm employees wHerE name = 'Employee Name';"
      `shouldBe`
      Right (DeleteStatement {
        tablename = "employees",
        whereArgs = [(ColumnName (Nothing, "name"), Constant "Employee Name", whereEq)]
      })
    it "parses delete keywords with extra whitespace" $ do
      Lib2.parseStatement "DELETE      FROM   employees  WHERE   name         ='Employee Name'  ;    "
      `shouldBe`
      Right (DeleteStatement {
        tablename = "employees",
        whereArgs = [(ColumnName (Nothing, "name"), Constant "Employee Name", whereEq)]
      })
    it "parses delete without where" $ do
      Lib2.parseStatement "DELETE FROM employees;"
      `shouldBe`
      Right (DeleteStatement {
        tablename = "employees",
        whereArgs = []
      })
    it "error message on update without table name specified" $ do
      Lib2.parseStatement "DELETE FROM WHERE name = 'Employee Name';"
      `shouldSatisfy`
      isLeft



      
whereEq :: String -> String -> Bool
whereEq _ _ = True


{-
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
parseTest 2 = Right (SelectStatement {selectArgs = [Right "id", Right "surname"], fromArgs = "employees", whereArgs = []})
parseTest 3 = Right (SelectStatement {selectArgs = [Left ("id", dummy1)], fromArgs = "employees", whereArgs = []})
parseTest 4 = Right (SelectStatement {selectArgs = [Right "*"], fromArgs = "duplicates", whereArgs = [(ColumnName "x", Constant "a", dummy2), (ColumnName "y", Constant "a", dummy2)]})
parseTest 5 = Right (SelectStatement {selectArgs = [Right "*"], fromArgs = "duplicates", whereArgs = [(ColumnName "x", ColumnName "y", dummy2)]})
parseTest 6 = Right (SelectStatement {selectArgs = [Right "id"], fromArgs = "employees", whereArgs = [(Constant "a", Constant "b", dummy2), (ColumnName "name", Constant "Z", dummy2)]})
parseTest 7 = Right (SelectStatement {selectArgs = [Left ("id", dummy1)], fromArgs = "employees", whereArgs = [(ColumnName "name", Constant "E", dummy2), (ColumnName "surname", Constant "E", dummy2)]})
parseTest _ = Left "error"

dummy1 :: [Value] -> Value
dummy1 _ = NullValue

dummy2 :: String -> String -> Bool
dummy2 a b = True
-}

