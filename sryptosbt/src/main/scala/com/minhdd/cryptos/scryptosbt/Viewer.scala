package com.minhdd.cryptos.scryptosbt

import com.minhdd.cryptos.scryptosbt.constants.dataDirectory
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.service.segment.Splitter
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
        
        val bb: Dataset[Seq[BeforeSplit]] = spark.read.parquet(s"$dataDirectory\\segments\\small\\20191113").as[Seq[BeforeSplit]]
        
        bb.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).sort("_2").show(false)
        println(bb.count())
    }
}
//        ds.map(seq => (seq.size, seq.head.datetime, seq.last.datetime))
//          .sort("_2").show(1000, true)