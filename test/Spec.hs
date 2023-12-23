{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE FlexibleInstances #-}

import Text.RawString.QQ
import Data.Either
import Data.Maybe (Maybe (Just))
import InMemoryTables qualified as D
import Lib1
import Lib2
import Lib3
import DataFrame
import Test.Hspec
import Data.Maybe
import Test.Hspec (shouldBe, describe)
import Data.Either (Either(Right))
import DataFrame (DataFrame(DataFrame), ColumnType (BoolType, IntegerType), Value (IntegerValue))
import qualified Lib2
import qualified Data.Ord as Lib2
import Text.Read (Lexeme(String))
import Data.IORef
import Control.Monad.Free (Free (..))
import Data.Time (Day (..), UTCTime (..), secondsToDiffTime)
import Data.Time.Calendar.OrdinalDate (fromOrdinalDate)
import EitherT
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class(lift, MonadTrans)
import EitherT (addTtoEither)
import Lib2 (ParsedStatement(orderByArgs))
main :: IO ()
main = hspec $ do
  {-
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
-}
  describe "Lib2.parseStatement" $ do
    it "parses a show tables statement" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` addTtoEither (Right (ShowTableStatement {showTableArgs = Nothing}))
    
    it "parses a show tables statement case insensitively" $ do
      Lib2.parseStatement "ShoW TAbLeS;" `shouldBe` addTtoEither (Right (ShowTableStatement {showTableArgs = Nothing}))
    it "parses a show table statement" $ do
      Lib2.parseStatement "SHOW TABLE employees;" `shouldBe` addTtoEither (Right (ShowTableStatement {showTableArgs = Just "employees"}))
    it "parses a select statement with columns" $ do
      Lib2.parseStatement "SELECT id, surname FROM employees;" `shouldBe` addTtoEither (Right (SelectStatement {selectArgs = [Right (Nothing, "id"), Right (Nothing, "surname")], fromArgs = ["employees"], whereArgs = [], orderByArgs = []}))
    it "does not parse an invalid select statement" $ do
      removeTFromEither (Lib2.parseStatement "SLECT id, birthday FROM employees;") `shouldSatisfy` isLeft
    it "parses a max function" $ do
      Lib2.parseStatement "SELECT MAX(id) FROM employees;" `shouldBe` addTtoEither (Right (SelectStatement {selectArgs = [Left ([(Nothing, "id")], Func1 dummy1)], fromArgs = ["employees"], whereArgs = [], orderByArgs = []}))
    it "does not parse an invalid max function" $ do
      removeTFromEither (Lib2.parseStatement "SELECT MAaX(id) FROM employees;") `shouldSatisfy` isLeft
    it "parses a sum function" $ do
      Lib2.parseStatement "SELECT SUM(id) FROM employees;" `shouldBe` addTtoEither (Right (SelectStatement {selectArgs = [Left ([(Nothing, "id")], Func1 dummy1)], fromArgs = ["employees"], whereArgs = [], orderByArgs = []}))
    it "does not parse an invalid sum function" $ do
      removeTFromEither (Lib2.parseStatement "SELECT SUMN(id) FROM employees;") `shouldSatisfy` isLeft
    it "parses a where or function with strings, = comparison" $ do
      Lib2.parseStatement "SELECT * FROM duplicates WHERE x = 'a' OR y = 'a';" `shouldBe` addTtoEither (Right (SelectStatement {selectArgs = [Right (Nothing, "*")], fromArgs = ["duplicates"], whereArgs = [(ColumnName (Nothing, "x"), Constant "a", dummy2), (ColumnName (Nothing, "y"), Constant "a", dummy2)], orderByArgs = []}))
    it "parses a where function with strings, <> comparison" $ do
      Lib2.parseStatement "SELECT * FROM duplicates WHERE x <> y;" `shouldBe` addTtoEither (Right (SelectStatement {selectArgs = [Right (Nothing, "*")], fromArgs = ["duplicates"], whereArgs = [(ColumnName (Nothing, "x"), ColumnName (Nothing, "y"), dummy2)], orderByArgs = []}))
    it "parses a where or function with strings, >= comparison" $ do
      Lib2.parseStatement "SELECT id FROM employees WHERE 'a' >= 'b' OR name >= 'Z';" `shouldBe` addTtoEither (Right (SelectStatement {selectArgs = [Right (Nothing, "id")], fromArgs = ["employees"], whereArgs = [(Constant "a", Constant "b", dummy2), (ColumnName (Nothing, "name"), Constant "Z", dummy2)], orderByArgs = []}))
    it "parses a where or function with strings, <= comparison, combined with sum" $ do
      Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= 'E' or surname <= 'E';" `shouldBe` addTtoEither (Right (SelectStatement {selectArgs = [Left ([(Nothing, "id")], Func1 dummy1)], fromArgs = ["employees"], whereArgs = [(ColumnName (Nothing, "name"), Constant "E", dummy2), (ColumnName (Nothing, "surname"), Constant "E", dummy2)], orderByArgs = []}))
      {-
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
                                                                        [])
    it "executes a where or function with strings, <= comparison, combined with sum" $ do
      case Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= 'E' or surname <= 'E';" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps D.database `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                        [[IntegerValue 2]])
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
    {-
    it "parses select with columns from multiple tables" $ do
      Lib2.parseStatement "SELECT employees.name, jobs.title, departments.location FROM employees, jobs, departments;" 
      `shouldBe` 
      addTtoEither (Right (SelectStatement {
        selectArgs = [Right (Just "employees", "name"), Right (Just "jobs", "title"), Right (Just "departments", "location")],
        fromArgs = ["employees", "jobs", "departments"],
        whereArgs = [],
        orderByArgs = []}))
      
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
      Lib2.parseStatement "SELECT NOW() FROM employees;" 
      `shouldBe` 
      Right (SelectStatement {
        selectArgs = [Left ([], Func0 Lib2.now')],
        fromArgs = ["employees"],
        whereArgs = []})
    it "parses select with NOW() function and other mixed columns" $ do
      Lib2.parseStatement "SELECT NOW(), employees.name, jobs.title FROM employees, jobs;"
      `shouldBe`
      Right (SelectStatement {
        selectArgs = [Left ([], Func0 Lib2.now'), Right (Just "employees", "name"), Right(Just "jobs", "title")],
        fromArgs = ["employees", "jobs"],
        whereArgs = []})
    it "parses select with where clause" $ do
      Lib2.parseStatement "SELECT employees.name, jobs.title FROM employees, jobs WHERE employees.name = jobs.title;"
      `shouldBe`
      Right (SelectStatement {
        selectArgs = [Right (Just "employees", "name"), Right (Just "jobs", "title")],
        fromArgs = ["employees", "jobs"],
        whereArgs = [( ColumnName (Just "employees", "name"), ColumnName (Just "jobs", "title"),  whereEq)]})
    it "parses select with where OR clause" $ do
      Lib2.parseStatement "SELECT employees.name, jobs.title FROM employees, jobs WHERE employees.name >= jobs.title OR employees.surname <= jobs.title;"
      `shouldBe`
      Right (SelectStatement {
        selectArgs = [Right (Just "employees", "name"), Right (Just "jobs", "title")],
        fromArgs = ["employees", "jobs"],
        whereArgs = [
          (ColumnName (Just "employees", "name"), ColumnName (Just "jobs", "title"), whereEq), 
          (ColumnName (Just "employees", "surname"), ColumnName (Just "jobs", "title"),  whereEq)
        ]})
    it "parses select with where clause and no table names" $ do
      Lib2.parseStatement "SELECT employees.name, jobs.title FROM employees, jobs WHERE name >= title;"
      `shouldBe`
      Right (SelectStatement {
        selectArgs = [Right (Just "employees", "name"), Right (Just "jobs", "title")],
        fromArgs = ["employees", "jobs"],
        whereArgs = [( ColumnName (Nothing, "name"), ColumnName (Nothing, "title"),  whereEq)]})
-}
    it "parses update with string constant in where" $ do
      Lib2.parseStatement "UPDATE employees SET id = 5, name = 'New Name' WHERE name = 'Old Name';"
      `shouldBe`
      addTtoEither (Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = [( ColumnName (Nothing, "name"), Constant "Old Name",  whereEq)]
      }))
-- Not supported
--    it "parses update with number constant in where" $ do
--      Lib2.parseStatement "UPDATE employees SET id = 5, name = 'New Name' WHERE id = 1;"
--      `shouldBe`
--      Right (UpdateStatement {
--        tablename = "employees",
--        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
--        whereArgs = [( ColumnName (Nothing, "id"), Constant "1",  whereEq)]})

    it "parses update with column name in where" $ do
      Lib2.parseStatement "UPDATE employees SET id = 5, name = 'New Name' WHERE id = otherId;"
      `shouldBe`
      addTtoEither (Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = [( ColumnName (Nothing, "id"), ColumnName (Nothing, "otherId"),  whereEq)]}))
    it "parses update keyword case insensitive" $ do
      Lib2.parseStatement "uPdAtE employees sET id = 5, name = 'New Name' WhErE id = otherId;"
      `shouldBe`
      addTtoEither (Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = [( ColumnName (Nothing, "id"), ColumnName (Nothing, "otherId"),  whereEq)]}))
    it "parses update with extra whitespace" $ do
      Lib2.parseStatement "UPDATE            employees    SET    id=  5,    name  ='New Name'  WHERE   id=otherId;"
      `shouldBe`
      addTtoEither (Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = [( ColumnName (Nothing, "id"), ColumnName (Nothing, "otherId"),  whereEq)]}))
    it "parses update without where" $ do
      Lib2.parseStatement "UPDATE employees SET id = 5, name = 'New Name';"
      `shouldBe`
      addTtoEither (Right (UpdateStatement {
        tablename = "employees",
        assignedValues = [("id", IntegerValue 5), ("name", StringValue "New Name")],
        whereArgs = []}))
    it "error message on update without set" $ do
      removeTFromEither (Lib2.parseStatement "UPDATE employees WHERE id=otherId;")
      `shouldSatisfy`
      isLeft
    it "error message on update without table name specified" $ do
      removeTFromEither (Lib2.parseStatement "UPDATE SET id = 5 WHERE id=otherId;")
      `shouldSatisfy`
      isLeft
    it "error message on update without setting any fields" $ do
      removeTFromEither (Lib2.parseStatement "UPDATE employees SET WHERE id=otherId;")
      `shouldSatisfy`
      isLeft


    it "parses insert with column names provided" $ do
      Lib2.parseStatement "INSERT INTO employees (name, jobTitle) VALUES ('Inserted Name', 'Inserted Job Title');"
      `shouldBe`
      addTtoEither (Right (InsertIntoStatement {
        tablename = "employees",
        valuesOrder = Just ["name", "jobTitle"],
        values = [StringValue "Inserted Name", StringValue "Inserted Job Title"]
      }))
    it "parses insert without column names provided" $ do
      Lib2.parseStatement "INSERT INTO employees VALUES ('Inserted Name', 'Inserted Job Title');"
      `shouldBe`
      addTtoEither (Right (InsertIntoStatement {
        tablename = "employees",
        valuesOrder = Nothing,
        values = [StringValue "Inserted Name", StringValue "Inserted Job Title"]
      }))  
    it "parses with number constant as value" $ do
      Lib2.parseStatement "INSERT INTO employees (name, id) VALUES ('Inserted Name', 1);"
      `shouldBe`
      addTtoEither (Right (InsertIntoStatement {
        tablename = "employees",
        valuesOrder = Just ["name", "id"],
        values = [StringValue "Inserted Name", IntegerValue 1]
      }))
    it "parses insert with keywords case insensitive" $ do
      Lib2.parseStatement "iNSeRt inTo employees (name, jobTitle) vALueS ('Inserted Name', 'Inserted Job Title');"
      `shouldBe`
      addTtoEither (Right (InsertIntoStatement {
        tablename = "employees",
        valuesOrder = Just ["name", "jobTitle"],
        values = [StringValue "Inserted Name", StringValue "Inserted Job Title"]
      }))
    it "parses insert with extra whitespace" $ do
      Lib2.parseStatement "INSERT    INTO     employees  (  name,jobTitle  )  VALUES (  'Inserted Name'    ,    'Inserted Job Title'   ) ; "
      `shouldBe`
      addTtoEither (Right (InsertIntoStatement {
        tablename = "employees",
        valuesOrder = Just ["name", "jobTitle"],
        values = [StringValue "Inserted Name", StringValue "Inserted Job Title"]
      }))
    it "error message on insert when value missing" $ do
      removeTFromEither (Lib2.parseStatement "INSERT INTO employees (name, jobTitle) VALUES;")
      `shouldSatisfy`
      isLeft
    it "error message on insert when no table provided" $ do
      removeTFromEither (Lib2.parseStatement "INSERT INTO VALUES ('Inserted Name', 'Inserted Job Title');")
      `shouldSatisfy`
      isLeft
    it "error message on insert when no values provided" $ do
      removeTFromEither (Lib2.parseStatement "INSERT INTO employees (name, jobTitle) VALUES;")
      `shouldSatisfy`
      isLeft
    it "parses delete with string constant in where" $ do
      Lib2.parseStatement "DELETE FROM employees WHERE name = 'Employee Name';"
      `shouldBe`
      addTtoEither (Right (DeleteStatement {
        tablename = "employees",
        whereArgs = [(ColumnName (Nothing, "name"), Constant "Employee Name", whereEq)]
      }))
-- Not supported
--    it "parses delete with number constant in where" $ do
--      Lib2.parseStatement "DELETE FROM employees WHERE id = 1;"
--      `shouldBe`
--      Right (DeleteStatement {
--        tablename = "employees",
--        whereArgs = [(ColumnName (Nothing, "id"), Constant "1", whereEq)]
--      })
    it "parses delete with column name in where" $ do
      Lib2.parseStatement "DELETE FROM employees WHERE name = surname;"
      `shouldBe`
      addTtoEither (Right (DeleteStatement {
        tablename = "employees",
        whereArgs = [(ColumnName (Nothing, "name"), ColumnName (Nothing, "surname"), whereEq)]
      }))
    it "parses delete keywords case insensitive" $ do
      Lib2.parseStatement "dElEtE FrOm employees wHerE name = 'Employee Name';"
      `shouldBe`
      addTtoEither (Right (DeleteStatement {
        tablename = "employees",
        whereArgs = [(ColumnName (Nothing, "name"), Constant "Employee Name", whereEq)]
      }))
    it "parses delete keywords with extra whitespace" $ do
      Lib2.parseStatement "DELETE      FROM   employees  WHERE   name         ='Employee Name'  ;    "
      `shouldBe`
      addTtoEither (Right (DeleteStatement {
        tablename = "employees",
        whereArgs = [(ColumnName (Nothing, "name"), Constant "Employee Name", whereEq)]
      }))
    it "parses delete without where" $ do
      Lib2.parseStatement "DELETE FROM employees;"
      `shouldBe`
      addTtoEither (Right (DeleteStatement {
        tablename = "employees",
        whereArgs = []
      }))
    it "error message on update without table name specified" $ do
      removeTFromEither (Lib2.parseStatement "DELETE FROM WHERE name = 'Employee Name';")
      `shouldSatisfy`
      isLeft
  
  describe "Lib3.executeSql" $ do
    {-
    it "executes select with columns from multiple tables" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "SELECT employees.name, jobs.title FROM employees, jobs;")
      df `shouldBe` Right (DataFrame [Column "employees.name" StringType,Column "jobs.title" StringType] [[StringValue "Vi",StringValue "Assistant"],[StringValue "Vi",StringValue "Lecturer"],[StringValue "Ed",StringValue "Assistant"],[StringValue "Ed",StringValue "Lecturer"]])
    it "executes select with columns from multiple tables without specifying table in select" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "SELECT name, title FROM employees, jobs;")
      df `shouldBe` Right  (DataFrame [Column "name" StringType,Column "title" StringType] [[StringValue "Vi",StringValue "Assistant"],[StringValue "Vi",StringValue "Lecturer"],[StringValue "Ed",StringValue "Assistant"],[StringValue "Ed",StringValue "Lecturer"]])
    it "executes select with columns from multiple tables without some specifying table in select" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "SELECT name, jobs.title FROM employees, jobs;")
      df `shouldBe` Right  (DataFrame [Column "name" StringType ,Column "jobs.title" StringType] [[StringValue "Vi",StringValue "Assistant"],[StringValue "Vi",StringValue "Lecturer"],[StringValue "Ed",StringValue "Assistant"],[StringValue "Ed",StringValue "Lecturer"]])
    it "executes select with columns from multiple tables inside functions" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "SELECT MAX(employees.id), SUM(jobs.id) FROM employees, jobs;")
      df `shouldBe` Right (DataFrame [Column "employees.id" IntegerType, Column "jobs.id" IntegerType] [[IntegerValue 2, IntegerValue 34]])
    it "executes select with where clause" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "SELECT employees.name, jobs.title FROM employees, jobs WHERE employees.name = jobs.title;")
      df `shouldBe` Right (DataFrame [Column "employees.name" StringType,Column "jobs.title" StringType] [])
    it "executes select with where OR clause" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "SELECT employees.name, jobs.title FROM employees, jobs WHERE employees.name >= jobs.title OR employees.surname <= jobs.title;")
      df `shouldBe` Right (DataFrame [Column "employees.name" StringType,Column "jobs.title" StringType] [[StringValue "Vi",  StringValue "Assistant"], [StringValue "Vi",  StringValue "Lecturer"], [StringValue "Ed",StringValue "Assistant"], [StringValue "Ed",StringValue "Lecturer"]])
    it "executes select with where clause and no table names" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "SELECT employees.name, jobs.title FROM employees, jobs WHERE name >= title;")
      df `shouldBe` Right (DataFrame [Column "employees.name" StringType,Column "jobs.title" StringType] [[StringValue "Vi",  StringValue "Assistant"], [StringValue "Vi",StringValue "Lecturer"], [StringValue "Ed",StringValue "Assistant"]])
    it "executes select with NOW() function" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "SELECT NOW() FROM employees;")
      df `shouldBe` Right (DataFrame [Column "datetime.datetime" StringType] [[StringValue "1984-11-28 00:00:00 UTC"], [StringValue "1984-11-28 00:00:00 UTC"]])
    it "executes select with NOW() function and other mixed columns" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "SELECT NOW(), employees.name, jobs.title FROM employees, jobs;")
      df `shouldBe` Right (DataFrame [Column "datetime.datetime" StringType, Column "employees.name" StringType, Column "jobs.title" StringType] [[StringValue "1984-11-28 00:00:00 UTC", StringValue "Vi",StringValue "Assistant"],[StringValue "1984-11-28 00:00:00 UTC", StringValue "Vi",StringValue "Lecturer"],[StringValue "1984-11-28 00:00:00 UTC", StringValue "Ed",StringValue "Assistant"],[StringValue "1984-11-28 00:00:00 UTC", StringValue "Ed",StringValue "Lecturer"]])
      -}
    it "executes update with string constant in where" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "UPDATE employees SET id = 42, name = 'New Vi' WHERE name = 'Vi';")
      dbdf <- getTableFromDB db "employees"
      dbdf `shouldBe` Just (DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 42,StringValue "New Vi",StringValue "Po"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"]])
    it "executes update with column name in where" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "UPDATE employees SET id = 42, name = 'New Vi' WHERE name = surname;")
      dbdf <- getTableFromDB db "employees"
      dbdf `shouldBe` Just (DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"]])
    it "executes update without where" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "UPDATE employees SET id = 42, name = 'New Vi';")
      dbdf <- getTableFromDB db "employees"
      dbdf `shouldBe` Just (DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 42,StringValue "New Vi",StringValue "Po"],[IntegerValue 42,StringValue "New Vi",StringValue "Dl"]])


    it "executes insert with column names provided" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "INSERT INTO employees (surname, id, name) VALUES ('Inserted Test Surname', 42, 'Inserted Test Name');")
      dbdf <- getTableFromDB db "employees"
      dbdf `shouldBe` Just (DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"],[IntegerValue 42,StringValue "Inserted Test Name",StringValue "Inserted Test Surname"]])
    it "executes insert without column names provided" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "INSERT INTO employees VALUES (42, 'Inserted Test Name', 'Inserted Test Surname');")
      dbdf <- getTableFromDB db "employees"
      dbdf `shouldBe` Just (DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"],[IntegerValue 42,StringValue "Inserted Test Name",StringValue "Inserted Test Surname"]])
      
    it "executes delete with string constant in where" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "DELETE FROM employees WHERE name = 'Vi';")
      dbdf <- getTableFromDB db "employees"
      dbdf `shouldBe` Just (DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 2,StringValue "Ed",StringValue "Dl"]])
    it "executes delete with column name in where" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "DELETE FROM employees WHERE name = surname;")
      dbdf <- getTableFromDB db "employees"
      dbdf `shouldBe` Just (DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [[IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"]])
    it "executes delete without where" $ do
      db <- testSetup
      df <- runExecuteIO db (Lib3.executeSql "DELETE FROM employees;")
      dbdf <- getTableFromDB db "employees"
      dbdf `shouldBe` Just (DataFrame [Column "id" IntegerType,Column "name" StringType,Column "surname" StringType] [])
  describe "Testing EitherT" $ do 
    it "parses delete without where" $ do
      Lib2.parseStatement "DELETE FROM employees;"
      `shouldBe`
       addTtoEither (Right (DeleteStatement {
        tablename = "employees",
        whereArgs = []
      }))
    it "does not parse an invalid select statement" $ do
      removeTFromEither (Lib2.parseStatement "SLECT id, birthday FROM employees;") `shouldSatisfy` isLeft

type MemoryDatabase = IORef [(String, String)]

testSetup :: IO MemoryDatabase
testSetup = newIORef $ map (\(a, b) -> (a, Lib3.serialize b)) D.database

--when calling this in tests you have to give it memoryDBForTests as the first argument
runExecuteIO :: MemoryDatabase -> Lib3.Execution r -> IO r
runExecuteIO _ (Pure r) = return r
runExecuteIO memoryDB (Free step) = do
    next <- runStep step
    runExecuteIO memoryDB next
    where
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.GetTime next) = return (UTCTime {utctDay = fromOrdinalDate 1984 333, utctDayTime = secondsToDiffTime 0})  >>= return . next
        runStep (Lib3.SaveTable name content next) = modifyIORef' memoryDB (modifyDatabase (name, content)) >>= return . next
        runStep (Lib3.LoadTable name next) = do
          table <- fmap (lookup name) (readIORef memoryDB)
          case table of
            Just a -> return a >>= return . next
            Nothing -> return "" >>= return . next
        runStep (Lib3.GetTableList next) = readIORef memoryDB >>= (return . next) . (fmap fst)

getTableFromDB :: MemoryDatabase -> String -> IO (Maybe DataFrame)
getTableFromDB ref name = do
  a <- readIORef ref
  case lookup name a of
    Just b -> return (deserialize b :: Maybe DataFrame)
    Nothing -> return Nothing

modifyDatabase :: (String, String) -> [(String, String)] -> [(String, String)]
modifyDatabase a [] = [a]
modifyDatabase (name, content) ((x, y):xs) = case x == name of
  True -> ((x, content):xs)
  False -> ((x, y) : modifyDatabase (name, content) xs)


whereEq :: String -> String -> Bool
whereEq _ _ = True


testRes1 :: DataFrame
testRes1 = DataFrame
  [Column "table_name" StringType]
  [
    [StringValue "employees"],
    [StringValue "invalid1"],
    [StringValue "invalid2"],
    [StringValue "long_strings"],
    [StringValue "flags"],
    [StringValue "duplicates"],
    [StringValue "noRows"],
    [StringValue "cartProdTestOne"],
    [StringValue "cartProdTestTwo"],
    [StringValue "jobs"]
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




dummy1 :: [Value] -> Value
dummy1 _ = NullValue

dummy2 :: String -> String -> Bool
dummy2 a b = True


instance (Show a) => Show (EitherT a (State String) ParsedStatement) where
    show eitherTState = case runState (runEitherT eitherTState) "" of
        (Left err, _) -> "Left " ++ show err
        (Right stmt, state) -> "Right " ++ show stmt ++ " (State: " ++ show state ++ ")"

instance (Eq a) => Eq (EitherT a (State String) ParsedStatement) where
    (==) eitherTState1 eitherTState2 =
        let (result1, state1) = runState (runEitherT eitherTState1) ""
            (result2, state2) = runState (runEitherT eitherTState2) ""
        in result1 == result2