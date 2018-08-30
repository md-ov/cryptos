package com.minhdd.cryptos.scryptosbt.parquet

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ExtractorTest extends FunSuite {
    
    val ss: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    
    test("testGetOneDayCryptoValue") {
        
    }
    
}
