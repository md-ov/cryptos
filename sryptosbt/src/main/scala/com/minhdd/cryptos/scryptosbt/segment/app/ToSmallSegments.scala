package com.minhdd.cryptos.scryptosbt.segment.app

import com.minhdd.cryptos.scryptosbt.constants.dataDirectory
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.segment.service.Splitter
import org.apache.spark.sql.{Dataset, SparkSession}

//after ToBigSegments
object ToSmallSegments {
    
    def cut(seq: Seq[Seq[BeforeSplit]]): Seq[Seq[BeforeSplit]] = {
        val length = seq.length
        val smallers: Seq[Seq[BeforeSplit]] = seq.flatMap(Splitter.toSmallSegments)
        if (smallers.length > length) {
            cut(smallers)
        } else {
            smallers
        }
    }
    
    def cut(ds: Dataset[Seq[BeforeSplit]]): Dataset[Seq[BeforeSplit]] = {
        import ds.sparkSession.implicits._
        val count = ds.count
        val smallers: Dataset[Seq[BeforeSplit]] = ds.flatMap(Splitter.toSmallSegments)
        if (smallers.count > count) {
            cut(smallers)
        } else {
            println
            smallers
        }
    }
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("big segments")
          .master("local[*]").getOrCreate()
    
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._
        
        val bb: Dataset[Seq[BeforeSplit]] = Seq("201316", "2017", "2018", "2019").map(year => {
            val bigs: Dataset[Seq[BeforeSplit]] = 
                spark.read.parquet(s"$dataDirectory\\segments\\big\\big15\\$year").as[Seq[BeforeSplit]]
            cut(bigs)
        }).reduce(_.union(_))
        
        bb.write.parquet(s"$dataDirectory\\segments\\small\\15\\20191116")
    }
}
