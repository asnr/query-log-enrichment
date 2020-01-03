{-# LANGUAGE OverloadedStrings #-}

module Enrich where

import Data.Functor (void)
import Data.Either (lefts)
import qualified Data.HashMap.Strict as HMS
import Data.Proxy
import Data.Text.Lazy.Encoding (decodeUtf8)
import qualified Data.Text.Lazy as TL
import Data.Time.Calendar (Day)
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
enrich = putStr . show  $ extractPartitions
  "select _time from foo where _time > '2019-12-10'"

extractPartitions :: TL.Text -> [Partition]
extractPartitions = getQueriedPartitions . fst . parseAndResolve

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
    foo = ( QTableName () None "foo", persistentTable [ QColumnName () None "_time"
                                                      ] )

    catalogMap :: CatalogMap
    catalogMap = HMS.singleton defaultDatabase $
                     HMS.fromList [ ( defaultSchema, HMS.fromList [ foo ] ) ]

data Partition = Partition { partitionColumn :: FQCN, earliestDate :: Day }
  deriving Show

getQueriedPartitions :: VerticaStatement ResolvedNames () -> [Partition]
getQueriedPartitions (VerticaStandardSqlStatement s) = getPartitionsFromStatement s
getQueriedPartitions (VerticaCreateProjectionStatement _) = []
getQueriedPartitions (VerticaMultipleRenameStatement _) = []
getQueriedPartitions (VerticaSetSchemaStatement _) = []
getQueriedPartitions (VerticaMergeStatement _) = []
getQueriedPartitions (VerticaUnhandledStatement _) = []

getPartitionsFromStatement :: Statement Vertica ResolvedNames () -> [Partition]
getPartitionsFromStatement (QueryStmt q) = getPartitionsFromQuery q
getPartitionsFromStatement (InsertStmt _) = []
getPartitionsFromStatement (UpdateStmt _) = []
getPartitionsFromStatement (DeleteStmt _) = []
getPartitionsFromStatement (TruncateStmt _) = []
getPartitionsFromStatement (CreateTableStmt _) = []
getPartitionsFromStatement (AlterTableStmt _) = []
getPartitionsFromStatement (DropTableStmt _) = []
getPartitionsFromStatement (CreateViewStmt _) = []
getPartitionsFromStatement (DropViewStmt _) = []
getPartitionsFromStatement (CreateSchemaStmt _) = []
getPartitionsFromStatement (GrantStmt _) = []
getPartitionsFromStatement (RevokeStmt _) = []
getPartitionsFromStatement (BeginStmt _) = []
getPartitionsFromStatement (CommitStmt _) = []
getPartitionsFromStatement (RollbackStmt _) = []
getPartitionsFromStatement (ExplainStmt _ _) = []
getPartitionsFromStatement (EmptyStmt _) = []

getPartitionsFromQuery :: Query ResolvedNames () -> [Partition]
getPartitionsFromQuery (QuerySelect _ s) = getPartitionsFromSelect s
getPartitionsFromQuery (QueryExcept _ _ _ _) = []
getPartitionsFromQuery (QueryUnion _ isDistinct _ fstQuery sndQuery) =
  unionPartitions (getPartitionsFromQuery fstQuery) (getPartitionsFromQuery sndQuery)
getPartitionsFromQuery (QueryIntersect _ _ _ _) = []
getPartitionsFromQuery (QueryWith _ _ _) = []  -- this is gonna be hard
getPartitionsFromQuery (QueryOrder _ _ q) = getPartitionsFromQuery q
getPartitionsFromQuery (QueryLimit _ _ q) = getPartitionsFromQuery q
getPartitionsFromQuery (QueryOffset _ _ q) = getPartitionsFromQuery q

getPartitionsFromSelect :: Select ResolvedNames () -> [Partition]
getPartitionsFromSelect select = case selectWhere select of
  Nothing -> []
  Just whereClause -> getPartitionsFromWhere whereClause

getPartitionsFromWhere :: SelectWhere ResolvedNames () -> [Partition]
getPartitionsFromWhere (SelectWhere _ expr) = getPartitionsFromExpr expr

getPartitionsFromExpr :: Expr ResolvedNames () -> [Partition]
getPartitionsFromExpr (BinOpExpr _ (Operator ">") left right) =
  case [getPartitionExpr left, getPartitionExpr right] of
    [PartitionValue valExpr, PartitionVar var varExpr] ->
      [Partition var (evalExpr valExpr)]
    [PartitionVar var varExpr, PartitionValue valExpr] ->
      [Partition var (evalExpr valExpr)]

unionPartitions :: [Partition] -> [Partition] -> [Partition]
unionPartitions a b = a

data PartitionExpr = PartitionValue (Expr ResolvedNames ())
                   | PartitionVar FQCN (Expr ResolvedNames ())

getPartitionExpr :: Expr ResolvedNames () -> PartitionExpr
getPartitionExpr expr = case expr of
  (ColumnExpr _ (RColumnRef fqcn)) -> PartitionVar (fqcnToFQCN fqcn) expr
  _ -> PartitionValue expr

evalExpr :: Expr ResolvedNames () -> Day
evalExpr (ConstantExpr _ (StringConstant _ const)) =
  parseTimeOrError True defaultTimeLocale "%Y-%m-%d" $ TL.unpack $ decodeUtf8 const
