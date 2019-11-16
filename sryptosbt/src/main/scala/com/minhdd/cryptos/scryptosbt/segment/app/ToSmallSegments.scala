package com.minhdd.cryptos.scryptosbt.segment.app

import com.minhdd.cryptos.scryptosbt.constants.dataDirectory
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.segment.service.Splitter
import org.apache.spark.sql.{Dataset, SparkSession}

//after ToBigSegments
object ToSmallSegments {
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
        
        val aa: Seq[(String, String)] = 
            Seq("1", "5", "15").flatMap(x => Seq("201316", "2017", "2018", "2019").map(y => (x, y)))
    
        val bb: Dataset[Seq[BeforeSplit]] = aa.map(c => {
            println(c)
            
            val level = c._1
            val year = c._2
            val bigs: Dataset[Seq[BeforeSplit]] = spark.read.parquet(s"$dataDirectory\\segments\\big$level\\$year").as[Seq[BeforeSplit]]
            print(bigs.count + " big segments ")
            cut(bigs)
        }).reduce(_.union(_))
        
        bb.write.parquet(s"$dataDirectory\\segments\\small\\20191113")
    }
}
