package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import org.apache.spark.sql.{Dataset, SparkSession}

object ParquetHelper {
    
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
        val optionDS = Crypto.getPartitionFromPath(ss, parquetPath)
        if (optionDS.isEmpty) {
            throw new RuntimeException("There is no OHLC data")
        } else {
            optionDS.get
        }
    }
}
