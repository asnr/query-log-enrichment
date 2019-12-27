{-# LANGUAGE OverloadedStrings #-}

module Enrich
    ( enrich
    ) where

import qualified Database.Sql.Vertica.Parser as VP

enrich :: IO ()
enrich = putStr . show $ VP.parse "SELECT 1;"
