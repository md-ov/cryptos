package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.env.{dataDirectory, todayPath}
import com.minhdd.cryptos.scryptosbt.constants.{numberOfMinutesBetweenTwoElement, thisYear}
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.parquet.ParquetHelper
import com.minhdd.cryptos.scryptosbt.segment.service.SegmentHelper
import org.apache.spark.sql.{Dataset, SparkSession}

//run first
object ToBigSegments {
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("big segments")
          .master("local[*]").getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        val (lastTimestamp2016, ds2013to2016): (Timestamp, Dataset[Seq[BeforeSplit]]) = toBigSegmentsBetween2013and2016(spark)
        
        val (lastTimestamp2017, ds2017): (Timestamp, Dataset[Seq[BeforeSplit]]) = toBigSegmentsAfter2016(spark,
            lastTimestamp2016, "2017", "2016")
        
        val (lastTimestamp2018, ds2018): (Timestamp, Dataset[Seq[BeforeSplit]]) = toBigSegmentsAfter2016(spark,
            lastTimestamp2017, "2018", "2017")
        
        val (lastTimestamp2019, ds2019): (Timestamp, Dataset[Seq[BeforeSplit]]) = toBigSegmentsAfter2016(spark,
            lastTimestamp2018, "2019", "2018")

        toBigSegmentsAfter2016(spark,
            lastTimestamp2019, "2020", "2019")
        
    }
    
    def toBigSegmentsAfter2016(spark: SparkSession, lastTimestamp: Timestamp, year: String, lastYear: String): (Timestamp,
      Dataset[Seq[BeforeSplit]]) = {
        import spark.implicits._
        val trades: Dataset[Crypto] = ParquetHelper.tradesCryptoDs(year, spark)
        val todayPartitionKey: CryptoPartitionKey = spark.read.parquet(todayPath).as[Crypto].head().partitionKey
        val todayMonth = todayPartitionKey.month
        val todayDay = todayPartitionKey.day
        
        val ohlcs: Dataset[Crypto] = ParquetHelper.ohlcCryptoDs(spark).filter(x => {
            (year != thisYear || x.partitionKey.month != todayMonth || x.partitionKey.day != todayDay) &&
                (x.partitionKey.year == year || (x.partitionKey.year == lastYear && x.cryptoValue.datetime.after(lastTimestamp)))
        })
        
        val (nextLastTimestamp: Timestamp, ds: Dataset[Seq[BeforeSplit]]) =
            SegmentHelper.toBigSegments(spark, trades, ohlcs)
        
        ds.write.parquet(s"$dataDirectory/segments/big/big$numberOfMinutesBetweenTwoElement/$year")
        
        ds.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).show(1000, true)
        
        (nextLastTimestamp, ds)
    }
    
    
    def toBigSegmentsBetween2013and2016(spark: SparkSession): (Timestamp, Dataset[Seq[BeforeSplit]]) = {
        import spark.implicits._
        val trades: Dataset[Crypto] = spark.createDataset(Seq[Crypto]())(Crypto.encoder(spark))
        
        val ohlcs: Dataset[Crypto] = ParquetHelper.ohlcCryptoDs(spark).filter(x => {
            x.partitionKey.year == "2013" ||
              x.partitionKey.year == "2014" ||
              x.partitionKey.year == "2015" ||
              x.partitionKey.year == "2016"
        })
        
        val (lastTimestamp: Timestamp, ds: Dataset[Seq[BeforeSplit]]) =
            SegmentHelper.toBigSegments(spark, trades, ohlcs)
        
        ds.write.parquet(s"$dataDirectory/segments/big/big$numberOfMinutesBetweenTwoElement/201316")
        
        ds.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).show(1000, true)
        
        (lastTimestamp, ds)
    }
}
