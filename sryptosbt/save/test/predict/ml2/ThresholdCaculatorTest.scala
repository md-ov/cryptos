package com.minhdd.cryptos.scryptosbt.predict.ml2

import com.minhdd.cryptos.scryptosbt.tools.FileHelper
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ThresholdCaculatorTest extends FunSuite {
    
    ignore("testGetThreshold") {
        val ss: SparkSession = SparkSession.builder().appName("minh").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df = ss.read.parquet(FileHelper.getPathForSpark("parquets/regressor-result"))
        val threshold: Double = ThresholdCalculator.getThreshold(ss, df)._1
        assert(threshold > 1.01897137 && threshold < 1.021)
    }
    
    ignore("adjust threshold") {
        val ss: SparkSession = SparkSession.builder().appName("minh").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df = ss.read.parquet(FileHelper.getPathForSpark("parquets/regressor-result"))
        val centeredThreshold: Double = 1.0465104442389215
        val adjustedThreshold = ThresholdCalculator.getAdjustedThreshold(ss, df, centeredThreshold, 0.82)._1
        assert(adjustedThreshold > 1.01897137 && adjustedThreshold < 1.021)
    }
    
}
