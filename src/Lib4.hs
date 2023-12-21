module Lib4 (encodeYAML, decodeYAML) where

class YAMLencodable a where
    encodeYAML :: a -> String
    decodeYAML :: String -> Either String a

