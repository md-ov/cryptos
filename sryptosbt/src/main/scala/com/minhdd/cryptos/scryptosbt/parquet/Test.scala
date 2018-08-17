package com.minhdd.cryptos.scryptosbt.parquet

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object Test {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        import ss.implicits._
    
        val ds1: Dataset[Crypto] = ss.read.parquet("file:///D:\\ws\\cryptos\\data\\parquets\\parquet1").as[Crypto]
        val ds2: Dataset[Crypto] = ss.read.parquet("file:///D:\\ws\\cryptos\\data\\parquets\\parquet2").as[Crypto]
        
        
//        val ds11: RDD[(CryptoPartitionKey, Crypto)] = ds1.toJavaRDD.rdd.map(c => ((c.partitionKey), c))
        
        
        
        println(ds1.count())
        println(ds2.count())
        val ds = ds1.union(ds2)
        println(ds.count())
        ds.toDF().show(false)
    }
}
