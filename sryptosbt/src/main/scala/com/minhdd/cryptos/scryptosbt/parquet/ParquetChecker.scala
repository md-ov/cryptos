package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.domain.Crypto
import com.minhdd.cryptos.scryptosbt.env.todayPath
import com.minhdd.cryptos.scryptosbt.segment.app.{ActualSegment, ToBigSegments}
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
        
        import spark.implicits._
        
        import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper.getString
    
        ParquetHelper.ohlcCryptoDs(spark)
            .map(x => getString(x.cryptoValue.datetime))
            .distinct()
            .sort("value")
            .show(1, false)
    
        spark.read.parquet(todayPath).as[Crypto]
            .map(x => x.cryptoValue.datetime)
            .distinct()
            .sort("value")
            .show(9999999, false)
    }
}
