package com.minhdd.cryptos.scryptosbt.service

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants.dataDirectory
import com.minhdd.cryptos.scryptosbt.domain.KrakenCrypto
import com.minhdd.cryptos.scryptosbt.exploration.BeforeSplit
import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
import org.apache.spark.sql.{Dataset, SparkSession}

object ToBigSegments {
    def tradesCryptoDs(ss: SparkSession): Dataset[Crypto] = {
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
        
        val (ds2013, lastTimestamp2013): (Dataset[Seq[BeforeSplit]], Timestamp) =
            toBigSegmentsBetween2013and2016(spark, year = "2013", lastYearTimestamp = None)
        
        //        val smallSegments: Dataset[Seq[BeforeSplit]] = ds.flatMap(BigSegmentToSegment.get)(BeforeSplit.encoderSeq(spark))
    }
    
    
    def toBigSegmentsBetween2013and2016(spark: SparkSession, year: String, lastYearTimestamp: Option[Timestamp]):
    (Dataset[Seq[BeforeSplit]], Timestamp) = {
        import spark.implicits._
        val trades: Dataset[Crypto] = spark.createDataset(Seq[Crypto]())(Crypto.encoder(spark))
        val ohlcs: Dataset[Crypto] = ohlcCryptoDs(spark).filter(_.partitionKey.year == year)
        
        val joined: Dataset[KrakenCrypto] = SpacingSpreadingJoiner.join(spark, trades, ohlcs)
        val collected: Seq[KrakenCrypto] = joined.collect().toSeq
        val beforeSplits: Seq[BeforeSplit] = SegmentsCalculator.toBeforeSplits(collected)
        val (bigSegments, lastTimestamp): (Seq[Seq[BeforeSplit]], Timestamp) = Splitter.toBigSegmentsAndLastTimestamp(beforeSplits)
        val ds: Dataset[Seq[BeforeSplit]] = spark.createDataset(bigSegments)(BeforeSplit.encoderSeq(spark))
        
        ds.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).show(100, true)
        
        (ds, lastTimestamp)
    }
}
