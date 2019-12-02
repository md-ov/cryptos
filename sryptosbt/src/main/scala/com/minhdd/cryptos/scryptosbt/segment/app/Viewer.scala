package com.minhdd.cryptos.scryptosbt.segment.app

import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import org.apache.spark.sql.{Dataset, SparkSession}

object Viewer {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("big segments")
          .master("local[*]").getOrCreate()
    
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._
        
        val smalls_20191113: Dataset[Seq[BeforeSplit]] = 
            spark.read.parquet(s"$dataDirectory\\segments\\small\\20191113").as[Seq[BeforeSplit]]
        smalls_20191113.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).sort("_2").show(false)
        println(smalls_20191113.count())
    
        val smalls_20191115: Dataset[Seq[BeforeSplit]] = 
            spark.read.parquet(s"$dataDirectory\\segments\\small\\20191115").as[Seq[BeforeSplit]]
        smalls_20191115.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).sort("_2").show(false)
        println(smalls_20191115.count())
    }
}
