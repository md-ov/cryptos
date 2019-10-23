package com.minhdd.cryptos.scryptosbt

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants.dataDirectory
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto, CryptoPartitionKey, KrakenCrypto}
import com.minhdd.cryptos.scryptosbt.service.segment.{SegmentsCalculator, SpacingSpreadingJoiner, Splitter}
import org.apache.spark.sql.{Dataset, SparkSession}

object ToBigSegments {
    
    def tradesCryptoDs(year: String, ss: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getTRADESParquetPath(
            parquetsDir = s"$dataDirectory\\parquets", asset = "XBT", currency = "EUR", year = year)
        Crypto.getPartitionsUniFromPath(ss, "file:///", parquetPath).get
    }
    
    def allTradesCryptoDs(ss: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getTRADESParquetPath(
            parquetsDir = s"$dataDirectory\\parquets", asset = "XBT", currency = "EUR")
        Crypto.getPartitionsUniFromPath(ss, "file:///", parquetPath).get
    }
    
    def ohlcCryptoDs(ss: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath(
            parquetsDir = s"file:///$dataDirectory\\parquets", asset = "XBT", currency = "EUR")
        Crypto.getPartitionFromPath(ss, parquetPath).get
    }
    
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
            lastTimestamp2016, "2018", "2017")
        
        val (lastTimestamp2019, ds2019): (Timestamp, Dataset[Seq[BeforeSplit]]) = toBigSegmentsAfter2016(spark,
            lastTimestamp2016, "2019", "2018")
        
        //        val smallSegments: Dataset[Seq[BeforeSplit]] = ds.flatMap(BigSegmentToSegment.get)(BeforeSplit.encoderSeq(spark))
    }
    
    def toBigSegmentsAfter2016(spark: SparkSession, lastTimestamp: Timestamp, year: String, lastYear: String): (Timestamp,
      Dataset[Seq[BeforeSplit]]) = {
        import spark.implicits._
        val trades: Dataset[Crypto] = tradesCryptoDs(year, spark)
        val ohlcs: Dataset[Crypto] = ohlcCryptoDs(spark).filter(x => {
            x.partitionKey.year == year || (x.partitionKey.year == lastYear && x.cryptoValue.datetime.after(lastTimestamp))
        })
        
        val (nextLastTimestamp: Timestamp, ds: Dataset[Seq[BeforeSplit]]) = toSmallSegments(spark, trades, ohlcs)
        
        ds.write.parquet(s"$dataDirectory\\segments\\big\\$year")
        
        ds.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).show(1000, true)
        
        (nextLastTimestamp, ds)
    }
    
    
    def toBigSegmentsBetween2013and2016(spark: SparkSession): (Timestamp, Dataset[Seq[BeforeSplit]]) = {
        import spark.implicits._
        val trades: Dataset[Crypto] = spark.createDataset(Seq[Crypto]())(Crypto.encoder(spark))
        
        val ohlcs: Dataset[Crypto] = ohlcCryptoDs(spark).filter(x => {
            x.partitionKey.year == "2013" ||
              x.partitionKey.year == "2014" ||
              x.partitionKey.year == "2015" ||
              x.partitionKey.year == "2016"
        })
        
        val (lastTimestamp: Timestamp, ds: Dataset[Seq[BeforeSplit]]) = toSmallSegments(spark, trades, ohlcs)
        
        ds.write.parquet(s"$dataDirectory\\segments\\big\\201316")
        
        ds.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).show(1000, true)
        
        (lastTimestamp, ds)
    }
    
    private def toSmallSegments(spark: SparkSession, trades: Dataset[Crypto], ohlcs: Dataset[Crypto]): (Timestamp, Dataset[Seq[BeforeSplit]]) = {
        val joined: Dataset[KrakenCrypto] = SpacingSpreadingJoiner.join(spark, trades, ohlcs)
        val collected: Seq[KrakenCrypto] = joined.collect().toSeq
        val beforeSplits: Seq[BeforeSplit] = SegmentsCalculator.toBeforeSplits(collected)
        val (bigSegments, lastTimestamp): (Seq[Seq[BeforeSplit]], Timestamp) = Splitter.toBigSegmentsAndLastTimestamp(beforeSplits)
        val ds: Dataset[Seq[BeforeSplit]] = spark.createDataset(bigSegments)(BeforeSplit.encoderSeq(spark))
        
        (lastTimestamp, ds)
    }
}
