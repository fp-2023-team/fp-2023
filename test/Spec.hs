import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
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
    it "parses a show table statement with case sensitive name" $ do
      Lib2.parseStatement "SHOW TABLE emplOyEes;" `shouldSatisfy` isLeft
    it "parses a select statement with collumns" $ do
      Lib2.parseStatement "SELECT id, surname FROM employees;" `shouldSatisfy` isRight
    it "parses an invalid select statement" $ do
      Lib2.parseStatement "SELECT id, birthday FROM employees;" `shouldSatisfy` isLeft
    it "parses a max function" $ do
      Lib2.parseStatement "SELECT MAX(id) FROM employees;" `shouldSatisfy` isRight
    it "parses a sum function" $ do
      Lib2.parseStatement "SELECT SUM(id) FROM employees;" `shouldSatisfy` isRight
    it "parses an invalid sum function" $ do
      Lib2.parseStatement "SELECT SUM(name) FROM employees;" `shouldSatisfy` isLeft
    it "parses a where or function with strings, = comparison" $ do
      Lib2.parseStatement "SELECT * FROM duplicates WHERE x = 'a' OR y = 'a';" `shouldSatisfy` isRight
    it "parses a where function with strings, <> comparison" $ do
      Lib2.parseStatement "SELECT * FROM duplicates WHERE x <> y;" `shouldSatisfy` isRight
    it "parses a where or function with strings, >= comparison" $ do
      Lib2.parseStatement "SELECT * FROM employees WHERE id => 2 OR name >= 'Va';" `shouldSatisfy` isRight
    it "parses a where or function with strings, <= comparison, combined with sum" $ do
      Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= 'E' or surname <= 'E';" `shouldSatisfy` isRight
  describe "Lib2.executeStatement" $ do
    it "executes a show tables statement" $ do
      Lib2.executeStatement Lib2.parseStatement "SHOW TABLES;" `shouldBe` testRes1
    it "parses a show tables statement case insensitively" $ do
      Lib2.executeStatementv Lib2.parseStatement "ShoW TAbLeS;" `shouldBe` testRes1
    it "parses a show table statement" $ do
      Lib2.executeStatement Lib2.parseStatement "SHOW TABLE employees;" `shouldBe` testRes2
    it "parses a select statement with collumns" $ do
      Lib2.executeStatement Lib2.parseStatement "SELECT id, surname FROM employees;" `shouldBe` testRes3
    it "parses a max function" $ do
      Lib2.executeStatement Lib2.parseStatement "SELECT MAX(id) FROM employees;" `shouldBe` [Column "1" IntegerType] [[IntegerValue 2]]
    it "parses a sum function" $ do
      Lib2.executeStatement Lib2.parseStatement "SELECT SUM(id) FROM employees;" `shouldBe` [Column "1" IntegerType] [[IntegerValue 3]]
    it "parses a where or function with strings, = comparison" $ do
      Lib2.executeStatement Lib2.parseStatement "SELECT * FROM duplicates WHERE x = 'a' OR y = 'a';" `shouldBe` testRes4
    it "parses a where function with strings, <> comparison" $ do
      Lib2.executeStatement Lib2.parseStatement "SELECT * FROM duplicates WHERE x <> y;" `shouldBe` testRes5
    it "parses a where or function with strings, >= comparison" $ do
      Lib2.executeStatement Lib2.parseStatement "SELECT * FROM employees WHERE id => 2 OR name >= 'Va';" `shouldbe` (snd D.tableEmployees)
    it "parses a where or function with strings, <= comparison, combined with sum" $ do
      Lib2.executeStatement Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= 'E' or surname <= 'E';" [Column "1" IntegerType] [[IntegerValue 2]]

testRes1 :: DataFrame
testRes1 = 
  [Column "1" StringType]
  [
    [StringType "employees"],
    [StringType "invalid1"],
    [StringType "invalid2"],
    [StringType "long_strings"],
    [StringType "flags"],
    [StringType "duplicates"],
  ]

testRes2 :: DataFrame
testRes2 = 
  [Column "1" StringType]
  [
    [StringType "id"],
    [StringType "name"],
    [StringType "surname"],
  ]

testRes3 :: DataFrame
testRes3 = 
  [Column "id" IntegerType, Column "surname" StringType]
  [ [IntegerValue 1, StringValue "Po"],
    [IntegerValue 2, StringValue "Dl"]
  ]

testRes4 :: DataFrame
testRes4 = 
  [Column "x" StringType, Column "y" StringType]
    [
        [StringValue "a", StringValue "a"],
        [StringValue "a", StringValue "b"],
        [StringValue "b", StringValue "a"],
    ]

testRes5 :: DataFrame
testRes5 = 
  [Column "x" StringType, Column "y" StringType]
    [
        [StringValue "a", StringValue "b"],
        [StringValue "b", StringValue "a"],
    ]