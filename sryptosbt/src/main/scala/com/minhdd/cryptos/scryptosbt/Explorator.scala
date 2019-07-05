package com.minhdd.cryptos.scryptosbt

import com.minhdd.cryptos.scryptosbt.exploration.OHLCAndTradesExplorator
import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
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
    

    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .appName("exploration")
          .master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        
        //    Explorator.run(ss, tradesCryptoDs(ss), outputDir = "trades-190407")
        //    Explorator.run(ss, ohlcCryptoDs(ss), outputDir = "ohlc-190407")
        OHLCAndTradesExplorator.explorate(ss, ohlcCryptoDs(ss), tradesCryptoDs(ss), outputDir = 
          "D:\\ws\\cryptos\\data\\segments\\all-190705-from-brut")
    
//        val last = "all-190703-fusion"
//        val now = "all-190704"
//        OHLCAndTradesExplorator.allSegments(ss, last, now)
    }
    
}
