module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))

import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
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
import qualified Lib2

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

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
      df <- runExecuteIO $ Lib3.executeSql c 
      --df <- runExecuteLocal $ Lib3.executeSql c 
      return $ Lib1.renderDataFrameAsTable s <$> df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        -- probably you will want to extend the interpreter
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
        runStep (Lib3.SaveTable name content next) = writeFile ("./db/" ++ name ++ ".json") content >>= return . next
        runStep (Lib3.LoadTable name next) = readFile ("./db/" ++ name ++ ".json") >>= return . next
        runStep (Lib3.SerializeTable dataframe next) = pure (Lib3.serialize dataframe) >>= return . next
        runStep (Lib3.DeserializeTable tableContent next) = pure (Lib3.deserialize tableContent) >>= return . next
        runStep (Lib3.GetParsedStatement statement next) = pure (Lib2.parseStatement statement) >>= return . next
        runStep (Lib3.GetExecutionResult statement database next) = pure (Lib2.executeStatement statement database) >>= return . next