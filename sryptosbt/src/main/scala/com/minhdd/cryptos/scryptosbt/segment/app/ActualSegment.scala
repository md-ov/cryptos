package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.constants.{directoryNow, numberOfMinutesBetweenTwoElement}
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.segment.app.ToBigSegments.ohlcCryptoDs
import com.minhdd.cryptos.scryptosbt.segment.service.{SegmentHelper, Splitter}
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper
import org.apache.spark.sql.{Dataset, SparkSession}

//after CompleteSmallSegments
object ActualSegment {
    
    def tradesFromLastSegment(ss: SparkSession, lastTimestamps: Timestamp,
                              lastCryptoPartitionKey: CryptoPartitionKey): Dataset[Crypto] = {
        val parquetPath = s"$dataDirectoryy//parquets"
        val todayPath = s"$dataDirectoryy//parquets//XBT//EUR//TRADES//today//parquet"
        
        Crypto.getPartitionsUniFromPathFromLastTimestamp(
            spark = ss, prefix = "file:///",
            path1 = parquetPath, path2 = parquetPath, todayPath = todayPath,
            ts = lastTimestamps, lastCryptoPartitionKey = lastCryptoPartitionKey).get
    }
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    
    def getActualSegment: Seq[Seq[BeforeSplit]] = {
        val smallSegments: Dataset[Seq[BeforeSplit]] =
            spark.read.parquet(s"$dataDirectory\\segments\\small\\$numberOfMinutesBetweenTwoElement\\$directoryNow").as[Seq[BeforeSplit]]
    
        val lastSegment: Seq[BeforeSplit] = smallSegments.collect().sortWith { case (x, y) => x.last.datetime.before(y.last.datetime) }.last
        val lastTimestamp: Timestamp = lastSegment.last.datetime
    
        val lastTsHelper: TimestampHelper = TimestampHelper(lastTimestamp.getTime)
        val lastCryptoPartitionKey = CryptoPartitionKey(
            asset = "XBT",
            currency = "EUR",
            provider = "KRAKEN",
            api = "TRADES",
            year = lastTsHelper.getYear,
            month = lastTsHelper.getMonth,
            day = lastTsHelper.getDay)
        val newTrades: Dataset[Crypto] = tradesFromLastSegment(spark, lastTimestamp, lastCryptoPartitionKey)
        val newOHLCs: Dataset[Crypto] = ohlcCryptoDs(spark).filter(x => !x.cryptoValue.datetime.before(lastTimestamp))
    
        val actualSegment: Seq[BeforeSplit] = SegmentHelper.toBeforeSplits(spark, newTrades, newOHLCs)
        ToSmallSegments.cut(Seq(actualSegment))
    }
    
    def main(args: Array[String]): Unit = {
        def actualSegments: Seq[Seq[BeforeSplit]] = getActualSegment
    
        println(actualSegments.size)
        println(actualSegments.last.size)
        println(actualSegments.last.head.datetime)
        println(actualSegments.last.last.datetime)
    }
}
