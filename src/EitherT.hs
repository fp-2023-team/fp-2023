{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}

module EitherT
  ( 
    EitherT,
    throwE,
    lift,
    pure,
    ErrorMessage,
    addTtoEither,
    fmap,
    runEitherT
  )
where

import Control.Monad.Trans.Class(lift, MonadTrans)
import DataFrame
import Control.Monad.Trans.State.Strict

type ErrorMessage = String

newtype EitherT e m a = EitherT {
    runEitherT :: m (Either e a)
}

instance MonadTrans (EitherT e) where
    lift :: Monad m => m a -> EitherT e m a
    lift ma = EitherT $ Prelude.fmap Right ma

instance Monad m => Functor (EitherT e m) where
    fmap :: (a -> b) -> EitherT e m a -> EitherT e m b
    fmap f ta = EitherT $ do
        eit <- runEitherT ta
        case eit of
            Left e -> return $ Left e
            Right a -> return $ Right (f a)

instance Monad m => Applicative (EitherT e m) where
    pure :: a -> EitherT e m a
    pure a = EitherT $ return $ Right a
    (<*>) :: EitherT e m (a -> b) -> EitherT e m a -> EitherT e m b
    af <*> aa = EitherT $ do
        f <- runEitherT af
        case f of
            Left e1 -> return $ Left e1
            Right r1 -> do
                a <- runEitherT aa
                case a of
                    Left e2 -> return $ Left e2
                    Right r2 -> return $ Right (r1 r2)

instance Monad m => Monad (EitherT e m) where
    (>>=) :: EitherT e m a -> (a -> EitherT e m b) -> EitherT e m b
    m >>= k = EitherT $ do
        eit <- runEitherT m
        case eit of
            Left e -> return $ Left e
            Right r -> runEitherT (k r)

throwE :: Monad m => e -> EitherT e m a
throwE err = EitherT $ return $ Left err

addTtoEither :: Either ErrorMessage a -> EitherT ErrorMessage (State String) a
addTtoEither (Left e) = throwE e
addTtoEither (Right a) = pure a