package com.minhdd.cryptos.scryptosbt

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.ToBigSegments.ohlcCryptoDs
import com.minhdd.cryptos.scryptosbt.constants.dataDirectory
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.service.segment.SegmentsCalculator
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper
import org.apache.spark.sql.{Dataset, SparkSession}

//after CompleteSmallSegments
object ActualSegment {
    
    def tradesFromLastSegment(ss: SparkSession, lastTimestamps: Timestamp,
                              lastCryptoPartitionKey: CryptoPartitionKey): Dataset[Crypto] = {
        val parquetPath = "D://ws//cryptos//data//parquets"
        val todayPath = "D://ws//cryptos//data//parquets//XBT//EUR//TRADES//today//parquet"
        Crypto.getPartitionsUniFromPathFromLastTimestamp(
            ss = ss, prefix = "file:///",
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
    
    def main(args: Array[String]): Unit = {
        val smallSegments: Dataset[Seq[BeforeSplit]] = 
            spark.read.parquet(s"$dataDirectory\\segments\\small\\20191115").as[Seq[BeforeSplit]] 
        
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
        val newTrades: Dataset[Crypto] = tradesFromLastSegment(spark, lastTimestamp, lastCryptoPartitionKey)
        val newOHLCs: Dataset[Crypto] = ohlcCryptoDs(spark).filter(x => !x.cryptoValue.datetime.before(lastTimestamp))
        
        val actualSegment: Seq[BeforeSplit] = SegmentsCalculator.toBeforeSplits(spark, newTrades, newOHLCs)
    
        println(actualSegment.size)
        println(actualSegment.head.datetime)
        println(actualSegment.last.datetime)
    }
}
