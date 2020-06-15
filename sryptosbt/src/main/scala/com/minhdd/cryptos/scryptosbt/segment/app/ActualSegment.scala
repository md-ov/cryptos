package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.env
import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.constants.smallSegmentsFolder
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.parquet.ParquetHelper
import com.minhdd.cryptos.scryptosbt.segment.service.SegmentHelper
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper
import org.apache.spark.sql.{Dataset, SparkSession}

//4
//after CompleteSmallSegments
object ActualSegment {

    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    
    def tradesFromLastSegment(spark: SparkSession, lastTimestamps: Timestamp,
                              lastCryptoPartitionKey: CryptoPartitionKey): Dataset[Crypto] = {

        
        Crypto.getPartitionsUniFromPathFromLastTimestamp(
            spark = spark, prefix = env.prefixPath,
            path1 = env.parquetsPath, path2 = env.parquetsPath, todayPath = env.todayPath,
            ts = lastTimestamps, lastCryptoPartitionKey = lastCryptoPartitionKey).get
    }

    def getBeforeSplits(beginTimestamp: Timestamp, endTimestamp: Timestamp): Seq[BeforeSplit] = {
        val beginTsHelper: TimestampHelper = TimestampHelper(beginTimestamp.getTime)
        val beginCryptoPartitionKey = CryptoPartitionKey(
            asset = "XBT",
            currency = "EUR",
            provider = "KRAKEN",
            api = "TRADES",
            year = beginTsHelper.getYear,
            month = beginTsHelper.getMonth,
            day = beginTsHelper.getDay)

        val trades: Dataset[Crypto] = tradesFromLastSegment(spark, beginTimestamp, beginCryptoPartitionKey)
          .filter(x => !x.cryptoValue.datetime.after(endTimestamp))

        val ohlcs: Dataset[Crypto] = ParquetHelper().ohlcCryptoDs(spark)
          .filter(x => !x.cryptoValue.datetime.before(beginTimestamp))
          .filter(x => !x.cryptoValue.datetime.after(endTimestamp))

        SegmentHelper.toBeforeSplits(spark, trades, ohlcs)
    }

    def getActualSegments(lastTimestamp: Timestamp): Seq[Seq[BeforeSplit]] = {
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
        val newOHLCs: Dataset[Crypto] = ParquetHelper().ohlcCryptoDs(spark).filter(x => !x.cryptoValue.datetime.before(lastTimestamp))
    
        val actualSegment: Seq[BeforeSplit] = SegmentHelper.toBeforeSplits(spark, newTrades, newOHLCs)
        ToSmallSegments.cut(Seq(actualSegment))
    }
    
    def getActualSegments: Seq[Seq[BeforeSplit]] = {
        val smallSegments: Dataset[Seq[BeforeSplit]] =
            spark.read.parquet(s"$dataDirectory${pathDelimiter}segments${pathDelimiter}small${pathDelimiter}$smallSegmentsFolder").as[Seq[BeforeSplit]]

        val lastSegment: Seq[BeforeSplit] = smallSegments.collect().sortWith { case (x, y) => x.last.datetime.before(y.last.datetime) }.last
        val lastTimestamp: Timestamp = lastSegment.last.datetime
        println("last ts of small segments : " + lastTimestamp)

        getActualSegments(lastTimestamp)
    }
}
