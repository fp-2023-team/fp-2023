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
    it "parses a where or function with strings, = and >= comparison" $ do
      Lib2.parseStatement "SELECT * FROM duplicates WHERE x = y OR y >= x;" `shouldBe` (parseTest 4)
    it "parses a where function with strings, <> comparison" $ do
      Lib2.parseStatement "SELECT * FROM duplicates WHERE x <> y;" `shouldBe` (parseTest 5)
    it "parses a where function with strings, <= comparison, combined with sum" $ do
      Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= surname;" `shouldBe` (parseTest 6)
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
    it "executes a where or function with strings, = and >= comparison" $ do
      case Lib2.parseStatement "SELECT * FROM duplicates WHERE x = y OR y >= x;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right testRes4
    it "executes a where function with strings, <> comparison" $ do
      case Lib2.parseStatement "SELECT * FROM duplicates WHERE x <> y;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right testRes5
    it "executes a where function with strings, <= comparison, combined with sum" $ do
      case Lib2.parseStatement "SElecT SuM(id) FRoM employees wHerE name <= surname;" of 
        Left err -> err `shouldBe` "should have successfully parsed"
        Right ps -> Lib2.executeStatement ps `shouldBe` Right (DataFrame [Column "id" IntegerType] 
                                                                        [[NullValue]])

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
        [StringValue "b", StringValue "b"]
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

type ErrorMessage = String

parseTest :: Int -> Either ErrorMessage ParsedStatement
parseTest 0 = Right (ShowTableStatement {showTableArgs = Nothing})
parseTest 1 = Right (ShowTableStatement {showTableArgs = Just "employees"})
parseTest 2 = Right (SelectStatement {selectArgs = [Right "id", Right "surname"], fromArgs = "employees", whereArgs = []})
parseTest 3 = Right (SelectStatement {selectArgs = [Left ("id", dummy1)], fromArgs = "employees", whereArgs = []})
parseTest 4 = Right (SelectStatement {selectArgs = [Right "*"], fromArgs = "duplicates", whereArgs = [("x", "y", dummy2), ("y", "x", dummy2)]})
parseTest 5 = Right (SelectStatement {selectArgs = [Right "*"], fromArgs = "duplicates", whereArgs = [("x", "y", dummy2)]})
parseTest 6 = Right (SelectStatement {selectArgs = [Left ("id", dummy1)], fromArgs = "employees", whereArgs = [("name", "surname", dummy2)]})
parseTest _ = Left "error"

dummy1 :: [Value] -> Value
dummy1 _ = NullValue

dummy2 :: String -> String -> Bool
dummy2 a b = True