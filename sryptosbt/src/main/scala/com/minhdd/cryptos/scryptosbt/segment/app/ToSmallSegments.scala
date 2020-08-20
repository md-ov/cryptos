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
//            bigs.map(_.size).filter(_ < 10).show(1000, false)
//            val completedBigs =
            Splitter.generalCutDsTs(bigs.map(s => (s.head.datetime, s.last.datetime)))
        }).reduce(_.union(_))
        
//        bb.filter(_.last.isEndOfSegment == false).show(5, false)

        val ts = DateTimeHelper.now
        println(ts)
        bb.write.parquet(s"$dataDirectory/segments/small/$numberOfMinutesBetweenTwoElement/$ts")
    }
}
