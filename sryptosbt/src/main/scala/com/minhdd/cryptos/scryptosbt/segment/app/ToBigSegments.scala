package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.env.{dataDirectory, todayPath}
import com.minhdd.cryptos.scryptosbt.constants.numberOfMinutesBetweenTwoElement
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.parquet.ParquetHelper
import com.minhdd.cryptos.scryptosbt.segment.service.SegmentHelper
import org.apache.spark.sql.{Dataset, SparkSession}

//1
//run first
//in 2021 change thisYear and run

//some statistics
//trades count 2016 : 0
//ohlc count 2016 :   342
//last ts : 2016-12-31 01:00:00.0
//----
//trades count 2017 : 9696710
//ohlc count 2017 :   442
//last ts  2017-12-30 17:45:00.0
//----
//trades count 2018 : 10828556
//ohlc count 2018 :   33701
//lat ts 2018-12-31 23:00:00.0
//----
//trades count 2019 : 8294262
//ohlc count 2019 :   142115
//last ts : 2019-12-31 19:00:00.0
//----
//trades count 2020 : 4172864
//ohlc count 2020 :   32493

object ToBigSegments {

  val thisYear = "2020"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val (lastTimestamp2016, notTaken2016): (Timestamp, Array[Crypto]) = toBigSegmentsBetween2013and2016(spark)
    val (lastTimestamp2017, notTaken2017): (Timestamp, Array[Crypto]) = toBigSegmentsAfter2016(spark, notTaken2016, lastTimestamp2016, "2017", "2016")
    val (lastTimestamp2018, notTaken2018): (Timestamp, Array[Crypto]) = toBigSegmentsAfter2016(spark, notTaken2017, lastTimestamp2017, "2018", "2017")
    val (lastTimestamp2019, notTaken2019): (Timestamp, Array[Crypto]) = toBigSegmentsAfter2016(spark, notTaken2018, lastTimestamp2018, "2019", "2018")
    toBigSegmentsAfter2016(spark, notTaken2019, lastTimestamp2019, thisYear, "2019")
  }

  def toBigSegmentsAfter2016(spark: SparkSession, notTakenFromLastYear: Array[Crypto], lastTimestamp: Timestamp, year: String, lastYear: String): (Timestamp, Array[Crypto]) = {
    import spark.implicits._
    val thisYearTrades: Dataset[Crypto] = ParquetHelper().tradesCryptoDs(year, spark)

    val trades: Dataset[Crypto] = if (lastYear != "2016") {
        spark.createDataset(notTakenFromLastYear).union(thisYearTrades)
    } else {
        thisYearTrades
    }

    val todayPartitionKey: CryptoPartitionKey = spark.read.parquet(todayPath).as[Crypto].head().partitionKey
    val todayMonth: String = todayPartitionKey.month
    val todayDay: String = todayPartitionKey.day

    val ohlcs: Dataset[Crypto] = ParquetHelper().ohlcCryptoDs(spark).filter(x => {
      (year != thisYear || x.partitionKey.month != todayMonth || x.partitionKey.day != todayDay) &&
        (x.partitionKey.year == year || (x.partitionKey.year == lastYear && !x.cryptoValue.datetime.before(lastTimestamp)))
    })

    val (nextLastTimestamp: Timestamp, ds: Dataset[Seq[BeforeSplit]]) =
      SegmentHelper.toBigSegments(spark, trades, ohlcs)

    ds.write.parquet(s"$dataDirectory/segments/big/big$numberOfMinutesBetweenTwoElement/$year")
    //        ds.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).limit(1000).show()
    val notTaken = trades.filter(x => !x.cryptoValue.datetime.before(nextLastTimestamp)).collect()
    (nextLastTimestamp, notTaken)
  }


  def toBigSegmentsBetween2013and2016(spark: SparkSession): (Timestamp, Array[Crypto]) = {
    import spark.implicits._

    val trades: Dataset[Crypto] = spark.createDataset(Seq[Crypto]())

    val ohlcs: Dataset[Crypto] = ParquetHelper().ohlcCryptoDs(spark).filter(x => {
      x.partitionKey.year == "2013" ||
        x.partitionKey.year == "2014" ||
        x.partitionKey.year == "2015" ||
        x.partitionKey.year == "2016"
    })

    val (lastTimestamp: Timestamp, ds: Dataset[Seq[BeforeSplit]]) =
      SegmentHelper.toBigSegments(spark, trades, ohlcs)

    val notTaken: Array[Crypto] = trades.filter(x => !x.cryptoValue.datetime.before(lastTimestamp)).collect()

    ds.write.parquet(s"$dataDirectory/segments/big/big$numberOfMinutesBetweenTwoElement/1316")

    //        ds.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).show(1000, true)
    (lastTimestamp, notTaken)
  }
}
