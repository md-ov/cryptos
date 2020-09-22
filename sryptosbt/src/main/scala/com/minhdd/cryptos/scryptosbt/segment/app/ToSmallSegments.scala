package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.constants.numberOfMinutesBetweenTwoElement
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto}
import com.minhdd.cryptos.scryptosbt.parquet.ParquetHelper
import com.minhdd.cryptos.scryptosbt.segment.service.{SegmentHelper, Splitter}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, TimestampHelper}
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper.TimestampImplicit
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

        val ohlcDs: Dataset[Crypto] = ParquetHelper().ohlcCryptoDs(spark).persist

        val sss: Seq[Seq[BeforeSplit]] = Seq("1316", "2017", "2018", "2019", "2020").flatMap(year => {
            val bigs: Array[Seq[BeforeSplit]] =
                spark.read.parquet(s"$dataDirectory/segments/big/big$numberOfMinutesBetweenTwoElement/$year").as[Seq[BeforeSplit]].collect
//            bigs.map(_.size).filter(_ < 10).show(1000, false)
            Splitter.generalCut(bigs.map(seq => {
                val start: String = seq.head.datetime.getString
                val end: String = seq.last.datetime.getString
                val beforeSplits: Seq[BeforeSplit] = SegmentHelper.getBeforeSplits(spark, start, end, ohlcDs)
                if (beforeSplits.nonEmpty) {
                    val last = beforeSplits.last.copy(isEndOfSegment = true)
                    beforeSplits.dropRight(1) :+ last
                } else {
                    beforeSplits
                }
            }))
        }).filter(_.nonEmpty)
        
//        bb.filter(_.last.isEndOfSegment == false).show(5, false)

        val ts = DateTimeHelper.now
        println(ts)
        spark.createDataset(sss).write.parquet(s"$dataDirectory/segments/small/$numberOfMinutesBetweenTwoElement/$ts")
    }
}
