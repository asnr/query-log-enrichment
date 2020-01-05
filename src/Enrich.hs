{-# LANGUAGE OverloadedStrings #-}

module Enrich where

import Control.Applicative ((<|>), empty)
import Data.Functor (void)
import Data.Either (lefts)
import qualified Data.HashMap.Strict as HMS
import Data.Proxy
import Data.Text.Lazy.Encoding (decodeUtf8)
import qualified Data.Text.Lazy as TL
import Data.Time (NominalDiffTime, UTCTime(..), diffUTCTime)
import Data.Time.Calendar (Day, fromGregorian)
import Data.Time.Format (defaultTimeLocale, parseTimeOrError)
import Database.Sql.Util.Scope (runResolverWarn)
import Database.Sql.Type (Query(..), Statement(..))
import Database.Sql.Type.Query (Constant (..),
                                Expr (..),
                                Operator (..),
                                Select (..),
                                SelectWhere (..))
import Database.Sql.Type.Names ( DatabaseName (..)
                               , fqcnToFQCN
                               , mkNormalSchema
                               , No (None)
                               , UQSchemaName
                               , QTableName (..), UQTableName
                               , RColumnRef (..)
                               , FQCN
                               , QColumnName(..))
import Database.Sql.Type.Schema (makeDefaultingCatalog)
import Database.Sql.Type.Scope (Catalog
                               , CatalogMap
                               , persistentTable
                               , RawNames
                               , ResolvedNames
                               , ResolutionError
                               , SchemaMember (..))
import qualified Database.Sql.Vertica.Parser as VerticaParser
import Database.Sql.Vertica.Type (VerticaStatement (..),
                                  resolveVerticaStatement,
                                  Vertica)

enrich :: IO ()
enrich = putStr . show  $ extractPartitions queryTime
  "select _time from foo where _time > '2019-12-10'"
  where queryTime = UTCTime (fromGregorian 2019 12 30) 0

extractPartitions :: UTCTime -> TL.Text -> [Partition]
extractPartitions queryTime = getQueriedPartitions queryTime . fst . parseAndResolve

parseAndResolve :: TL.Text -> (VerticaStatement ResolvedNames (), [ResolutionError ()])
parseAndResolve sql =
  case runResolverWarn (resolveVerticaStatement $ parse sql)
                       (Proxy :: Proxy Vertica) catalog of
    (Right queryResolved, resolutions) -> (queryResolved, lefts resolutions)
    (Left err, _) -> error $ show err

parse :: TL.Text -> VerticaStatement RawNames ()
parse sql = case void <$> VerticaParser.parse sql of
    Right q -> q
    Left err -> error $ show err

catalog :: Catalog
catalog = makeDefaultingCatalog catalogMap [defaultSchema] defaultDatabase
  where
    defaultDatabase :: DatabaseName ()
    defaultDatabase = DatabaseName () "defaultDatabase"

    defaultSchema :: UQSchemaName ()
    defaultSchema = mkNormalSchema "public" ()

    foo :: (UQTableName (), SchemaMember)
    foo = (QTableName () None "foo", persistentTable [QColumnName () None "_time"])

    catalogMap :: CatalogMap
    catalogMap = HMS.singleton defaultDatabase $
                     HMS.fromList [ ( defaultSchema, HMS.fromList [ foo ] ) ]

data Partition = Partition { partitionColumn :: FQCN, earliestDate :: NominalDiffTime }
  deriving Show

getQueriedPartitions :: UTCTime -> VerticaStatement ResolvedNames () -> [Partition]
getQueriedPartitions queryTime (VerticaStandardSqlStatement s) = getPartitionsFromStatement queryTime s
getQueriedPartitions _ (VerticaCreateProjectionStatement _) = []
getQueriedPartitions _ (VerticaMultipleRenameStatement _) = []
getQueriedPartitions _ (VerticaSetSchemaStatement _) = []
getQueriedPartitions _ (VerticaMergeStatement _) = []
getQueriedPartitions _ (VerticaUnhandledStatement _) = []

getPartitionsFromStatement :: UTCTime -> Statement Vertica ResolvedNames () -> [Partition]
getPartitionsFromStatement qTime (QueryStmt q) = getPartitionsFromQuery qTime q
getPartitionsFromStatement _ (InsertStmt _) = []
getPartitionsFromStatement _ (UpdateStmt _) = []
getPartitionsFromStatement _ (DeleteStmt _) = []
getPartitionsFromStatement _ (TruncateStmt _) = []
getPartitionsFromStatement _ (CreateTableStmt _) = []
getPartitionsFromStatement _ (AlterTableStmt _) = []
getPartitionsFromStatement _ (DropTableStmt _) = []
getPartitionsFromStatement _ (CreateViewStmt _) = []
getPartitionsFromStatement _ (DropViewStmt _) = []
getPartitionsFromStatement _ (CreateSchemaStmt _) = []
getPartitionsFromStatement _ (GrantStmt _) = []
getPartitionsFromStatement _ (RevokeStmt _) = []
getPartitionsFromStatement _ (BeginStmt _) = []
getPartitionsFromStatement _ (CommitStmt _) = []
getPartitionsFromStatement _ (RollbackStmt _) = []
getPartitionsFromStatement _ (ExplainStmt _ _) = []
getPartitionsFromStatement _ (EmptyStmt _) = []

getPartitionsFromQuery :: UTCTime -> Query ResolvedNames () -> [Partition]
getPartitionsFromQuery qTime (QuerySelect _ s) = getPartitionsFromSelect qTime s
getPartitionsFromQuery _ (QueryExcept _ _ _ _) = []
getPartitionsFromQuery qTime (QueryUnion _ isDistinct _ fstQuery sndQuery) =
  unionPartitions (getPartitionsFromQuery qTime fstQuery) (getPartitionsFromQuery qTime sndQuery)
getPartitionsFromQuery _ (QueryIntersect _ _ _ _) = []
getPartitionsFromQuery _ (QueryWith _ _ _) = []  -- this is gonna be hard
getPartitionsFromQuery qTime (QueryOrder _ _ q) = getPartitionsFromQuery qTime q
getPartitionsFromQuery qTime (QueryLimit _ _ q) = getPartitionsFromQuery qTime q
getPartitionsFromQuery qTime (QueryOffset _ _ q) = getPartitionsFromQuery qTime q

getPartitionsFromSelect :: UTCTime -> Select ResolvedNames () -> [Partition]
getPartitionsFromSelect qTime select = case selectWhere select of
  Nothing -> []
  Just whereClause -> getPartitionsFromWhere qTime whereClause

getPartitionsFromWhere :: UTCTime -> SelectWhere ResolvedNames () -> [Partition]
getPartitionsFromWhere qTime (SelectWhere _ expr) = getPartitionsFromExpr qTime expr

getPartitionsFromExpr :: UTCTime -> Expr ResolvedNames () -> [Partition]
getPartitionsFromExpr qTime (BinOpExpr _ (Operator ">") left right) =
  case [getPartitionExpr left, getPartitionExpr right] of
    [Nothing, Just fqcn] ->
      [Partition fqcn (evalExpr qTime left)]
    [Just fqcn, Nothing] ->
      [Partition fqcn (evalExpr qTime right)]

unionPartitions :: [Partition] -> [Partition] -> [Partition]
unionPartitions a b = a

getPartitionExpr :: Expr ResolvedNames () -> Maybe FQCN
getPartitionExpr expr = case expr of
  (ColumnExpr _ (RColumnRef fqcn)) -> Just $ fqcnToFQCN fqcn
  (FunctionExpr _ funcName _ args _ _ _) ->
    -- For some reason I couldn't get asum to compile here
    foldr (<|>) empty $ fmap getPartitionExpr args
  _ -> Nothing

evalExpr :: UTCTime -> Expr ResolvedNames () -> NominalDiffTime
evalExpr qTime (ConstantExpr _ (StringConstant _ const)) =
  diffUTCTime qTime $
    parseTimeOrError True defaultTimeLocale "%Y-%m-%d" $ TL.unpack $ decodeUtf8 const
