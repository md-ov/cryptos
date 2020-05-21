package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.env
import com.minhdd.cryptos.scryptosbt.tools.FileHelper
import org.apache.spark.sql.{Dataset, SparkSession}

object ParquetHelper {
    
    def tradesFromLastSegment(ss: SparkSession, lastTimestamps: Timestamp,
                              lastCryptoPartitionKey: CryptoPartitionKey): Dataset[Crypto] = {
        
        Crypto.getPartitionsUniFromPathFromLastTimestamp(
            spark = ss, prefix = env.prefixPath,
            path1 = env.parquetsPath, path2 = env.parquetsPath, todayPath = env.todayPath,
            ts = lastTimestamps, lastCryptoPartitionKey = lastCryptoPartitionKey).get
    }
    
    def tradesCryptoDs(year: String, ss: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getTRADESParquetPath(parquetsDir = env.parquetsPath, asset = "XBT", currency = "EUR", year = year)
        Crypto.getPartitionsUniFromPath(ss, env.prefixPath, parquetPath).get
    }
    
    def allTradesCryptoDs(ss: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getTRADESParquetPath(parquetsDir = env.parquetsPath, asset = "XBT", currency = "EUR")
        Crypto.getPartitionsUniFromPath(ss, env.prefixPath, parquetPath).get
    }
    
    def ohlcCryptoDs(ss: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath(
            parquetsDir = FileHelper.getPathForSpark(env.parquetsPath), asset = "XBT", currency = "EUR")
        val optionDS: Option[Dataset[Crypto]] = Crypto.getLastParquet(ss, parquetPath)
        if (optionDS.isEmpty) {
            throw new RuntimeException("There is no OHLC data")
        } else {
            optionDS.get
        }
    }
}
