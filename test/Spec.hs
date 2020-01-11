{-# LANGUAGE OverloadedStrings #-}

module Main where

import Enrich

import Data.Time.Calendar (fromGregorian)
import Data.Time (UTCTime(..), nominalDay)
import Test.Tasty (defaultMain, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase)

timeOfQuery = UTCTime (fromGregorian 2019 12 30) 0

nominalHour = 60 * 60

main :: IO ()
main = defaultMain $ testGroup "Extract partitions"
  [
    testCase "Simple query" $
      do let query = "select _time from foo where _time > '2019-12-29'"
         partitions <- extractPartitions timeOfQuery query
         assertEqual "" nominalDay $ earliestDate $ head partitions,

    testCase "Query comparing date" $
      do let query = "select _time from foo where date(_time) > '2019-12-28'"
         partitions <- extractPartitions timeOfQuery query
         assertEqual "" (2 * nominalDay) $ earliestDate $ head partitions,

    testCase "Query comparing CURRENT_TIMESTAMP" $
      do let query = "select _time from foo where _time > CURRENT_TIMESTAMP(1)"
         partitions <- extractPartitions timeOfQuery query
         assertEqual "" 0 $ earliestDate $ head partitions,

    testCase "Query comparing CURRENT_TIMESTAMP shifted by a timezone offset" $
      do let query = "select _time from foo where _time > TIMESTAMP(FORMAT_TIMESTAMP('%F %T', CURRENT_TIMESTAMP(), 'America/Los_Angeles'))"
         partitions <- extractPartitions timeOfQuery query
         assertEqual "" (8 * nominalHour)  $ earliestDate $ head partitions
  ]
