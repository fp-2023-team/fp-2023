module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Functor((<&>))
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified (unconvertDF)
import InMemoryTables (database)
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)
import Control.Applicative (liftA)
import DataFrame
import Network.HTTP
import qualified Data.Yaml as Yaml
import Data.ByteString.Char8 (pack, unpack)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to the sql client! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      df <- makeHttpRequest c 
      return $ Lib1.renderDataFrameAsTable s <$> df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

makeHttpRequest :: String -> IO (Either String DataFrame)
makeHttpRequest a = do
  response <- simpleHTTP (getRequest $ "http://localhost:8000/?statement=" ++ urlEncode a)
  case response of
    Left err -> return $ Left "Connection error"
    Right resp -> case resp of
      Response  (2, _, _) _ _ body -> return $ case (Yaml.decodeEither $ pack body) of
        Left s -> Left s
        Right df -> Right $ Lib3.unconvertDF df
      Response (4, _, _) _ _ body -> return $ Left body
      Response code _ _ _-> return $ Left $ "Response code: " ++ show code
