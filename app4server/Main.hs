module Main(main) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM.TVar

import Control.Monad.STM
import Control.Monad.Free
import Control.Monad.Trans.Class

import Data.Time ( UTCTime, getCurrentTime )
import qualified Data.ByteString.Internal as ByteString.Internal

import System.Directory

import Happstack.Server

import Lib1
import Lib2
import Lib3

import DataFrame


import qualified Data.Yaml as Yaml

modifyTVarIO a f = atomically (modifyTVar a f)

save :: TVar [(String, String)] -> STM ()
save state = do
    curState <- readTVar state
    return ()

saveLoop :: TVar [(String, String)] -> IO ()
saveLoop state = do
    threadDelay $ 1000 * 1000
    atomically $ save state
    curState <- readTVarIO state
    --putStrLn $ show curState
    saveLoop state

loadTablesFromDirectory :: FilePath -> IO [(String, String)]
loadTablesFromDirectory path = do
    fileNames <- listDirectory path
    loadedTables <- loadTablesFromList fileNames (return [])
    --putStrLn $ show $ fileNames
    return $ loadedTables
    where
        loadTablesFromList :: [FilePath] -> IO [(String, String)] -> IO [(String, String)]
        loadTablesFromList [] acc = acc
        loadTablesFromList (fileName:fileNames) acc = do
            fileContents <- readFile $ path ++ "/" ++ fileName
            let _:_:_:_:_:reversedTableName = reverse fileName
            let tableName = reverse reversedTableName
            loadTablesFromList fileNames (fmap ((:) (tableName, fileContents)) acc)

handleRequest :: TVar [(String, String)] -> ServerPart String
handleRequest state = do
    statementStr <- look "statement"
    executionResult <- lift $ runExecuteIO $ Lib3.executeSql statementStr
    case executionResult of
        Left err -> badRequest $ "Error: " ++ err
        Right dataframe -> ok $ ByteString.Internal.unpackChars $ Yaml.encode $ convertDF dataframe
    where
        runExecuteIO :: Lib3.Execution r -> IO r
        runExecuteIO (Pure r) = return r
        runExecuteIO (Free step) = do
            next <- runStep step
            runExecuteIO next
            where
                runStep :: Lib3.ExecutionAlgebra a -> IO a
                runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
                runStep (Lib3.SaveTable name content next) = modifyState >>= return . next
                    where
                        modifyState = modifyTVarIO state ((:) (name, content))
                runStep (Lib3.LoadTable name next) = fmappedResult >>= return . next
                    where
                        fmapLookup = fmap (lookup name)
                        fmapMaybe = fmap (maybe (error $ "Could not find table " ++ name) (id))
                        fmappedResult = fmapMaybe $ fmapLookup $ readTVarIO state

main :: IO ()
main = do
    database <- loadTablesFromDirectory "./db"
    state <- newTVarIO database
    withAsync (saveLoop state) $ \asyncSave -> do
        link asyncSave
        simpleHTTP nullConf $ handleRequest state

