package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.domain.Crypto
import com.minhdd.cryptos.scryptosbt.segment.app.ToBigSegments
import org.apache.spark.sql.{Dataset, SparkSession}

object ParquetChecker {
    val spark: SparkSession = SparkSession.builder()
        .config("spark.driver.maxResultSize", "3g")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .appName("parquet check")
        .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    def main(args: Array[String]): Unit = {
        val cryptos: Dataset[Crypto] = ToBigSegments.ohlcCryptoDs(spark)
        
        import spark.implicits._
        
        import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper.getString
        
        cryptos
            .map(x => getString(x.cryptoValue.datetime))
            .distinct()
            .sort("value")
            .show(9999999, false)
    }
}
