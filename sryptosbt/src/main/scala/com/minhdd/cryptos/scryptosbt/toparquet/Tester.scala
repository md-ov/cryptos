package com.minhdd.cryptos.scryptosbt.toparquet

import org.apache.spark.sql.SparkSession

object Tester {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        val ds = ss.read.parquet("file:///D:\\ws\\cryptos\\data\\parquets\\bchv2")
        ds.toDF().show(false)
    }
    
}
