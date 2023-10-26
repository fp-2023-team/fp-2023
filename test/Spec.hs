import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import DataFrame
import Test.Hspec

main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
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
      Lib2.parseStatement "SHOW TABLES;" `shouldSatisfy` isRight
    it "parses a show tables statement case insensitively" $ do
      Lib2.parseStatement "ShoW TAbLeS;" `shouldSatisfy` isRight
    it "parses a show table statement" $ do
      Lib2.parseStatement "SHOW TABLE employees;" `shouldSatisfy` isRight
    it "parses a select statement with columns" $ do
      Lib2.parseStatement "SELECT id, surname FROM employees;" `shouldSatisfy` isRight
    it "does not parse an invalid select statement" $ do
      Lib2.parseStatement "SLECT id, birthday FROM employees;" `shouldSatisfy` isLeft
    it "parses a max function" $ do
      Lib2.parseStatement "SELECT MAX(id) FROM employees;" `shouldSatisfy` isRight
    it "does not parse an invalid max function" $ do
      Lib2.parseStatement "SELECT MAaX(id) FROM employees;" `shouldSatisfy` isLeft
    it "parses a sum function" $ do
      Lib2.parseStatement "SELECT SUM(id) FROM employees;" `shouldSatisfy` isRight
    it "does not parse an invalid sum function" $ do
      Lib2.parseStatement "SELECT SUMN(id) FROM employees;" `shouldSatisfy` isLeft
    it "parses a where or function with strings, = comparison" $ do
      Lib2.parseStatement "SELECT * FROM duplicates WHERE x = 'a' OR y = 'a';" `shouldSatisfy` isRight
    it "parses a where function with strings, <> comparison" $ do
      Lib2.parseStatement "SELECT * FROM duplicates WHERE x <> y;" `shouldSatisfy` isRight
    it "parses a where or function with strings, >= comparison" $ do
      Lib2.parseStatement "SELECT * FROM employees WHERE id >= 2 OR name >= 'Va';" `shouldSatisfy` isRight
    it "parses a where or function with strings, <= comparison, combined with sum" $ do
      Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= 'E' or surname <= 'E';" `shouldSatisfy` isRight
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
    it "does not execute a sum function with incorrect columns" $ do
      case Lib2.parseStatement "SELECT SUM(name) FROM employees;" of
        Left err -> err `shouldBe` err
        Right ps -> Lib2.executeStatement ps `shouldSatisfy` isLeft
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
                                                                        [])
    it "executes a where or function with strings, <= comparison, combined with sum" $ do
      case Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= 'E' or surname <= 'E';" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                        [[IntegerValue 2]])

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

instance Show ParsedStatement where
  show _ = "parsed statement"