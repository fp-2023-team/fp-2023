module Main(main) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM.TVar

import Control.Exception

import Control.Monad.STM
import Control.Monad.Free
import Control.Monad.Trans.Class

import Data.Time ( UTCTime, getCurrentTime )
import qualified Data.ByteString.Internal as ByteString.Internal

import System.Directory
import System.IO

import Happstack.Server

import Lib1
import Lib2
import Lib3

import DataFrame


import qualified Data.Yaml as Yaml

saveLoop :: TVar [(String, String)] -> IO ()
saveLoop state = do
    threadDelay $ 4 * 1000 * 1000
    _ <- putStrLn $ "Saving..."
    curState <- readTVarIO state
    _ <- save curState
    saveLoop state
    where
        save :: [(String, String)] -> IO ()
        save [] = return ()
        save ((name, content):xs) = do
            catch (writeFile ("./db/" ++ name ++ ".json") content)
                ((\ex -> putStrLn $ "Caught error: " ++ show ex) :: SomeException -> IO ())
            save xs

loadTablesFromDirectory :: FilePath -> IO [(String, String)]
loadTablesFromDirectory dirPath = do
    fileNames <- listDirectory dirPath
    loadedTables <- loadTablesFromList fileNames (return [])
    --putStrLn $ show $ fileNames
    return $ loadedTables
    where
        loadTablesFromList :: [FilePath] -> IO [(String, String)] -> IO [(String, String)]
        loadTablesFromList [] acc = acc
        loadTablesFromList (fileName:fileNames) acc = do
            fileContents <- readFile' $ dirPath ++ "/" ++ fileName
            let _:_:_:_:_:reversedTableName = reverse fileName
            let tableName = reverse reversedTableName
            loadTablesFromList fileNames (fmap ((:) (tableName, fileContents)) acc)

handleRequest :: TVar [(String, String)] -> ServerPart String
handleRequest state = do
    statementStr <- look "statement"
    executionResult <- lift $ runExecuteIO $ Lib3.executeSql statementStr
    case executionResult of
        Left err -> do
            _ <- lift $ putStrLn $ "Error: " ++ err
            badRequest err
        Right dataframe -> do
            _ <- lift $ putStrLn $ "Info: successfully parsed '" ++ statementStr ++ "'"
            ok $ ByteString.Internal.unpackChars $ Yaml.encode $ convertDF dataframe
    where
        runExecuteIO :: Lib3.Execution r -> IO r
        runExecuteIO (Pure r) = return r
        runExecuteIO (Free step) = do
            next <- runStep step
            runExecuteIO next
            where
                runStep :: Lib3.ExecutionAlgebra a -> IO a
                runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
                runStep (Lib3.SaveTable name content next) = (atomically $ updateState state (name, content))
                    >>= return . next
                    where
                        updateState :: TVar [(String, b)] -> (String, b) -> STM ()
                        updateState state' newValue@(newName, _) = do
                            curState <- readTVar state'
                            writeTVar state'
                                $ newValue:[value | value@(name', _) <- curState, name' /= newName]
                runStep (Lib3.LoadTable name next) = (atomically $ lookupState state name) >>= return . next
                    where
                        lookupState :: TVar [(String, String)] -> String -> STM String
                        lookupState state' needle = do
                            curState <- readTVar state'
                            case lookup needle curState of
                                Nothing -> return ""
                                Just x -> return x
                runStep (Lib3.GetTableList next) = (atomically $ getTables state) >>= return . next
                    where
                        getTables :: TVar [(String, b)] -> STM [String]
                        getTables state' = do
                            curState <- readTVar state'
                            return $ fmap fst curState
                runStep (Lib3.DeleteTable name next) = (deleteTable state name) >>= return . next
                    where
                        deleteTable :: TVar [(String, b)] -> String -> IO ()
                        deleteTable state' tablename = do
                            fileExist <- doesFileExist $ "./db/" ++ tablename ++ ".json"
                            if not fileExist
                                then putStrLn $ "File " ++ "./db/" ++ tablename ++ ".json" ++ " does not exist"
                                else do
                                    removeFile $ "./db/" ++ tablename ++ ".json"
                                    database <- loadTablesFromDirectory "./db/"
                                    _ <- atomically $ writeTVar state database
                                    return ()

main :: IO ()
main = do
    putStrLn $ "Starting SQL server..."
    database <- loadTablesFromDirectory "./db/"
    state <- newTVarIO database
    withAsync (saveLoop state) $ \asyncSave -> do
        link asyncSave
        putStrLn $ "SQL server started"
        simpleHTTP nullConf $ handleRequest state

