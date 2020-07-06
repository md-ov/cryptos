package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.env.todayPath
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper
import org.apache.spark.sql.SparkSession
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper.getString

object ParquetChecker {
    val spark: SparkSession = SparkSession.builder()
        .config("spark.driver.maxResultSize", "3g")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .appName("parquet check")
        .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    def main(args: Array[String]): Unit = {

        val parquetHelper = ParquetHelper()

        println("ohlc")
        parquetHelper.ohlcCryptoDs(spark)
            .map(x => getString(x.cryptoValue.datetime))
            .distinct()
            .sort("value")
            .show(1, false)
    
        println("all trades")
        parquetHelper.allTradesCryptoDs(spark).show(1, false)
    
        println("2020 trades")
        parquetHelper.tradesCryptoDs("2020", spark).show(1, false)
    
        println("trades from last segment")
        parquetHelper.tradesFromLastSegment(
            spark,
            TimestampHelper.getTimestamp("2020-02-03 10:25:23"),
            CryptoPartitionKey("XBT", "EUR", "KRAKEN", "TRADES", "2020", "02", "03")).show(1, false)
    
        println("trades today")
        spark.read.parquet(todayPath).as[Crypto]
            .map(x => x.cryptoValue.datetime)
            .distinct()
            .sort("value")
            .show(1, false)
    }
}
