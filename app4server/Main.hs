module Main(main) where

import Happstack.Server

main :: IO ()
main = simpleHTTP nullConf $ ok $ "Hello Happstack!\n"
