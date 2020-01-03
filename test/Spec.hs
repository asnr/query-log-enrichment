{-# LANGUAGE OverloadedStrings #-}

module Main where

import Enrich

import Data.Time.Calendar (fromGregorian)
import Test.Tasty (defaultMain, testGroup)
import Test.Tasty.HUnit (assertEqual, testCase)

main :: IO ()
main = defaultMain $ testGroup "Extract partitions"
  [
    testCase "Simple query" $
      assertEqual "" (fromGregorian 2019 12 10) $ earliestDate $ head $
        extractPartitions "select _time from foo where _time > '2019-12-10'",

    testCase "Query comparing date" $
      assertEqual "" (fromGregorian 2020 1 1) $ earliestDate $ head $
        extractPartitions "select _time from foo where date(_time) > '2020-01-01'"
  ]
