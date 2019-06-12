package com.minhdd.cryptos.scryptosbt

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.exploration.{BeforeSplit, OHLCAndTradesExplorator}
import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.tools.Timestamps
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{Dataset, SparkSession}
import com.minhdd.cryptos.scryptosbt.constants._

object Explorator {
    
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
    
    def lastSegments(ss: SparkSession, lastSegmentsDir: String): Dataset[Seq[BeforeSplit]] = {
        import ss.implicits._
        ss.read.parquet(lastSegmentsDir).as[Seq[BeforeSplit]]
    }
    
    def explorateFromLastSegment(ss: SparkSession, 
                                 lastSegments: Dataset[Seq[BeforeSplit]],
                                 outputDir: String) = {
        import Timestamps.fromTimestampsLong
        import ss.implicits._
        
        val lastTimestampDS: Dataset[Timestamp] = lastSegments.map(e => fromTimestampsLong(e.last.datetime.getTime))
        val lastTimestamp: Timestamp = lastTimestampDS.agg(max("value").as("max")).first().getAs[Timestamp](0)
        
        val ts: Timestamps = Timestamps(lastTimestamp.getTime)
    
        val lastCryptoPartitionKey = CryptoPartitionKey(
            asset = "XBT", 
            currency = "EUR", 
            provider = "KRAKEN", 
            api = "TRADES",  
            year = ts.getYearString,
            month = ts.getMonthString, 
            day = ts.getDayString)
        
        val ohlcs = ohlcCryptoDsFromLastSegment(ss, lastTimestamp)
        val trades: Dataset[Crypto] = tradesFromLastSegment(ss, lastTimestamp, lastCryptoPartitionKey)
        trades.show(10000, false)
    
        OHLCAndTradesExplorator.explorate(ss, ohlcs, trades, outputDir)
    }
    
    def tradesFromLastSegment(ss: SparkSession, lastTimestamps: Timestamp, 
                              lastCryptoPartitionKey: CryptoPartitionKey): Dataset[Crypto] = {
//        val parquetPath = CryptoPartitionKey.getTRADESParquetPath(
//            parquetsDir = "D:\\ws\\cryptos\\data\\parquets", asset = "XBT", currency = "EUR")
        val parquetPath = "D://ws//cryptos//data//parquets"
        val todayPath = "D://ws//cryptos//data//parquets//XBT//EUR//TRADES//today//parquet"
        Crypto.getPartitionsUniFromPathFromLastTimestamp(
            ss = ss, prefix = "file:///", 
            path1 = parquetPath, path2 = parquetPath, todayPath = todayPath, 
            ts = lastTimestamps, lastCryptoPartitionKey = lastCryptoPartitionKey).get
    }
    
    def ohlcCryptoDsFromLastSegment(ss: SparkSession, lastTimestamp: Timestamp): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath(
            parquetsDir = s"file:///$dataDirectory\\parquets", asset = "XBT", currency = "EUR")
        Crypto.getPartitionFromPathFromLastTimestamp(ss, parquetPath, lastTimestamp).get
    }
    

    
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "2g")
          .appName("exploration")
          .master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
    
//        run(ss, tradesCryptoDs(ss), outputDir = "trades-190407")
//        run(ss, ohlcCryptoDs(ss), outputDir = "ohlc-190407")
//        OHLCAndTradesExplorator.explorate(ss, ohlcCryptoDs(ss), tradesCryptoDs(ss), outputDir = 
//          "D:\\ws\\cryptos\\data\\csv\\segments\\all-190502")
//                e(ss)
        allSegments(ss)
    }
    
    def allSegments(ss: SparkSession): Unit = {
        import constants.dataDirectory
        val last = "all-190606-fusion"
        val now = "all-190611"
        val lastSegmentsDir = s"$dataDirectory\\csv\\segments\\$last\\$BEFORE_SPLITS"
        val afterLastSegmentDir = s"$dataDirectory\\csv\\segments\\$now"
        
        explorateFromLastSegment(ss = ss,
            lastSegments = lastSegments(ss, lastSegmentsDir = lastSegmentsDir),
            outputDir = afterLastSegmentDir)

        fusion(ss,s"$dataDirectory\\csv\\segments\\$now-fusion\\$BEFORE_SPLITS",
            Seq(lastSegmentsDir, s"$dataDirectory\\csv\\segments\\$now\\$BEFORE_SPLITS"))
        
    }
    
    def fusion(ss: SparkSession, targetPath: String, elementsPaths: Seq[String]) = {
        import ss.implicits._
        val finalDs = elementsPaths.map(p => ss.read.parquet(p).as[Seq[BeforeSplit]]).reduce(_.union(_))
        finalDs.write.parquet(targetPath)
    }
    
}