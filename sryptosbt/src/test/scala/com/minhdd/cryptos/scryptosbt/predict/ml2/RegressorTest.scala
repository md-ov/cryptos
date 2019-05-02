package com.minhdd.cryptos.scryptosbt.predict.ml2

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class RegressorTest extends FunSuite {
    
    test("testGetThreshold") {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df = ss.read.parquet("file://" + getClass.getResource("/parquets/regressor-result").getPath)
//        df.show(false)
        def threshold: Double = Regressor.getThreshold(ss, df)
        assert(threshold > 1.01897137 && threshold < 1.021)
    }
    
}
