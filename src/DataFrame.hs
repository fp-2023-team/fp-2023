module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  deriving (Show, Eq)

data Column = Column String ColumnType
  deriving (Show, Eq)

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  deriving (Show, Eq)

instance Ord Value where
    compare (IntegerValue x) (IntegerValue y) = compare x y
    compare (StringValue x) (StringValue y) = compare x y
    compare (BoolValue x) (BoolValue y) = compare x y
    compare NullValue NullValue = EQ
    compare NullValue _ = LT
    compare _ NullValue = GT
    compare _ _ = EQ

type Row = [Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Show, Eq)
