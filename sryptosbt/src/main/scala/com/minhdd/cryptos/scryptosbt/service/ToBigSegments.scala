package com.minhdd.cryptos.scryptosbt.service

import com.minhdd.cryptos.scryptosbt.constants.dataDirectory
import com.minhdd.cryptos.scryptosbt.domain.KrakenCrypto
import com.minhdd.cryptos.scryptosbt.exploration.{BeforeSplit, SamplerObj, Segment}
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
    
    def toSplit(b: BeforeSplit): Boolean = b.importantChange.getOrElse(false)
    
    def splitToBigSegments(beforeSplits: Seq[BeforeSplit]): Seq[Seq[BeforeSplit]] = {
        if (!beforeSplits.exists(toSplit)) Seq(beforeSplits) else {
            val splits: (Seq[BeforeSplit], Seq[BeforeSplit]) = beforeSplits.partition(toSplit)
            Seq(splits._1) ++ splitToBigSegments(splits._2) 
        }
    }
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .config("spark.driver.maxResultSize", "3g")
            .config("spark.network.timeout", "600s")
            .appName("exploration")
            .master("local[*]").getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        val trades: Dataset[Crypto] = tradesCryptoDs(spark)
        val ohlcs: Dataset[Crypto] = ohlcCryptoDs(spark)
        
        val joined: Dataset[KrakenCrypto] = SpacingSpreadingJoiner.join(spark, trades, ohlcs)
        val krakenCryptos: Seq[KrakenCrypto] = joined.collect().toSeq
        val beforeSplits: Seq[BeforeSplit] = SegmentsCalculator.toBeforeSplits(krakenCryptos)
        beforeSplits.take(10).foreach(println)
        val bigSegments: Seq[Seq[BeforeSplit]] = splitToBigSegments(beforeSplits)
        val ds: Dataset[Seq[BeforeSplit]] = spark.createDataset(bigSegments)(BeforeSplit.encoderSeq(spark))
        val segments: Dataset[Seq[BeforeSplit]] = ds.flatMap(BigSegmentToSegment.get)(BeforeSplit.encoderSeq(spark))
    }
}
