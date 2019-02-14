package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
import org.apache.spark.sql.{Dataset, SparkSession}
import Explorates._

object Explorator {
    
    def tradesCryptoDs(ss: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getTRADESParquetPath(
            parquetsDir = "D:\\ws\\cryptos\\data\\parquets", asset = "XBT", currency = "EUR")
        Crypto.getPartitionsUniFromPath(ss, parquetPath).get
    }
    
    def ohlcCryptoDs(ss: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath(
            parquetsDir = "file:///D:\\ws\\cryptos\\data\\parquets", asset = "XBT", currency = "EUR")
        Crypto.getPartitionFromPath(ss, parquetPath).get
    }
    
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "2g")
          .appName("exploration")
          .master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
    
//        run(ss, tradesCryptoDs(ss), outputDir = "trades-190213")
//        run(ss, ohlcCryptoDs(ss), outputDir = "ohlc-190213")
        OHLCAndTradesExplorator.explorate(ss, ohlcCryptoDs(ss), tradesCryptoDs(ss), outputDir = "all-190215")

    }
    
    
}
