package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.parquet.ParquetHelper
import com.minhdd.cryptos.scryptosbt.segment.service.{SegmentHelper, Splitter}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, TimestampHelper}
import org.apache.spark.sql.{Dataset, SparkSession}

//3
//after ToSmallSegments
object CompleteSmallSegments {
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    def main(args: Array[String]): Unit = {
        val smallSegmentsPath = s"$dataDirectory/segments/small/$smallSegmentsFolder"
        val outputSegmentsPath = s"$dataDirectory/segments/small/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"

        completeSmallSegments(smallSegmentsPath, outputSegmentsPath)
    }

    def completeSmallSegments(smallSegmentsPath: String, outputPath: String) = {
        val smallSegments: Dataset[Seq[BeforeSplit]] = spark.read.parquet(smallSegmentsPath).as[Seq[BeforeSplit]]

        val lastSegment: Seq[BeforeSplit] = smallSegments.collect().sortWith { case (x, y) => x.last.datetime.before(y.last.datetime) }.last
        val lastTimestamp: Timestamp = lastSegment.last.datetime
        println(lastSegment.size)
        println(lastSegment.head.datetime)
        println(lastTimestamp)

        val lastTsHelper: TimestampHelper = TimestampHelper(lastTimestamp.getTime)
        val lastCryptoPartitionKey = CryptoPartitionKey(
            asset = "XBT",
            currency = "EUR",
            provider = "KRAKEN",
            api = "TRADES",
            year = lastTsHelper.getYear,
            month = lastTsHelper.getMonth,
            day = lastTsHelper.getDay)
        val newTrades: Dataset[Crypto] = ParquetHelper().tradesFromLastSegment(spark, lastTimestamp, lastCryptoPartitionKey)
        val newOHLCs: Dataset[Crypto] = ParquetHelper().ohlcCryptoDs(spark).filter(x => !x.cryptoValue.datetime.before(lastTimestamp))

        val newBigs: Dataset[Seq[BeforeSplit]] = SegmentHelper.toBigSegments(spark, newTrades, newOHLCs)._2
        val newSmalls: Dataset[Seq[BeforeSplit]] = Splitter.generalCut(newBigs)

        newSmalls.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).sort("_2").show(false)

        val allSmalls: Dataset[Seq[BeforeSplit]] = newSmalls.union(smallSegments)

        allSmalls.write.parquet(outputPath)
    }
}
