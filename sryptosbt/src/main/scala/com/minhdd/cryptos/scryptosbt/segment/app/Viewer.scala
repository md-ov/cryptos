package com.minhdd.cryptos.scryptosbt.segment.app

import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.constants._
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
        
        val smalls: Dataset[Seq[BeforeSplit]] =
            spark.read.parquet(s"$dataDirectory/segments/small/$smallSegmentsFolder").as[Seq[BeforeSplit]]
        smalls.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).sort("_2").show(false)
        println(smalls.count())
    }
}
