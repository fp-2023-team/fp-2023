module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.List qualified as L
import InMemoryTables (database)
import Lib1 qualified
import Lib2 qualified
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-more database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = ["select", "*", "from", "show", "table", "tables"] ++ map fst database
  return $ Prelude.filter (L.isPrefixOf n) names

main :: IO ()
main = putStrLn "compatibility issues"