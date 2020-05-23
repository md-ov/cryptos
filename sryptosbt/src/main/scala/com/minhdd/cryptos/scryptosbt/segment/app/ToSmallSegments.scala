package com.minhdd.cryptos.scryptosbt.segment.app

import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.constants.numberOfMinutesBetweenTwoElement
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.segment.service.Splitter
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper
import org.apache.spark.sql.{Dataset, SparkSession}

//2
//after ToBigSegments
//update seq of year and run
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
        val count: Long = ds.count
        val smallers: Dataset[Seq[BeforeSplit]] = ds.flatMap(Splitter.toSmallSegments)
        if (smallers.count > count) {
            cut(smallers)
        } else {
            smallers
        }
    }
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .config("spark.driver.maxResultSize", "3g")
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "60s")
            .appName("small segments")
            .master("local[*]").getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._
        
        val bb: Dataset[Seq[BeforeSplit]] = Seq("1316", "2017", "2018", "2019", "2020").map(year => {
            val bigs: Dataset[Seq[BeforeSplit]] =
                spark.read.parquet(s"$dataDirectory/segments/big/big$numberOfMinutesBetweenTwoElement/$year").as[Seq[BeforeSplit]]
//            bigs.filter(_.head.isEndOfSegment == true).show(5, false)
            cut(bigs)
        }).reduce(_.union(_))
        
//        bb.filter(_.last.isEndOfSegment == false).show(5, false)

        val ts = DateTimeHelper.now
        println(ts)
        bb.write.parquet(s"$dataDirectory/segments/small/$numberOfMinutesBetweenTwoElement/$ts")
    }
}
