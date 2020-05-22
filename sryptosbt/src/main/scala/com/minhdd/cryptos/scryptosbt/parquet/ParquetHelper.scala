package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.env
import com.minhdd.cryptos.scryptosbt.tools.FileHelper
import org.apache.spark.sql.{Dataset, SparkSession}

case class ParquetHelper(asset: String, currency: String) {

    def tradesFromLastSegment(spark: SparkSession, lastTimestamps: Timestamp,
                              lastCryptoPartitionKey: CryptoPartitionKey): Dataset[Crypto] = {

        Crypto.getPartitionsUniFromPathFromLastTimestamp(
            spark = spark, prefix = env.prefixPath,
            path1 = env.parquetsPath, path2 = env.parquetsPath, todayPath = env.todayPath,
            ts = lastTimestamps, lastCryptoPartitionKey = lastCryptoPartitionKey).get
    }

    def tradesCryptoDs(year: String, spark: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getTRADESParquetPath(parquetsDir = env.parquetsPath, asset = asset, currency = currency, year = year)
        Crypto.getPartitionsUniFromPath(spark, env.prefixPath, parquetPath).get
    }

    def allTradesCryptoDs(spark: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getTRADESParquetPath(parquetsDir = env.parquetsPath, asset = asset, currency = currency)
        Crypto.getPartitionsUniFromPath(spark, env.prefixPath, parquetPath).get
    }

    def ohlcCryptoDs(spark: SparkSession): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath(
            parquetsDir = FileHelper.getPathForSpark(env.parquetsPath), asset = asset, currency = currency)
        val optionDS: Option[Dataset[Crypto]] = Crypto.getLastParquet(spark, parquetPath)
        if (optionDS.isEmpty) {
            throw new RuntimeException("There is no OHLC data")
        } else {
            optionDS.get
        }
    }
}

object ParquetHelper {
    def apply(): ParquetHelper = ParquetHelper("XBT", "EUR")
}
