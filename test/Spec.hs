{-# LANGUAGE OverloadedStrings #-}

module Main where

import Enrich

import Data.Time.Calendar (fromGregorian)
import Data.Time (UTCTime(..), nominalDay)
import Test.Tasty (defaultMain, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase)

timeOfQuery = UTCTime (fromGregorian 2019 12 30) 0

main :: IO ()
main = defaultMain $ testGroup "Extract partitions"
  [
    testCase "Simple query" $
      assertEqual "" nominalDay $ earliestDate $ head $
        extractPartitions timeOfQuery "select _time from foo where _time > '2019-12-29'",

    testCase "Query comparing date" $
      assertEqual "" (2 * nominalDay) $ earliestDate $ head $
        extractPartitions timeOfQuery "select _time from foo where date(_time) > '2019-12-28'",

    testCase "Query comparing CURRENT_TIMESTAMP" $
      assertEqual "" 0 $ earliestDate $ head $
        extractPartitions timeOfQuery "select _time from foo where _time > CURRENT_TIMESTAMP(1)"
  ]
