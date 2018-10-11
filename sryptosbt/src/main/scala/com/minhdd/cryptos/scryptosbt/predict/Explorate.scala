package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.tools.Timestamps

import org.apache.spark.sql.{Dataset, SparkSession}

object Explorate {
    
    val maximumDeltaTime = 4 * Timestamps.oneDayTimestampDelta
    val minDeltaValue = 350
    
    
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("explorate").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath("file:///D:\\ws\\cryptos\\data\\parquets", "XBT", "EUR")
        println(parquetPath)
        val ds: Dataset[Crypto] = Crypto.getPartitionFromPath(ss, parquetPath).get
        val analyticsCryptoDS: Dataset[AnalyticsCrypto] = DerivativeCrypto.derive(ds, ss)
        import ss.implicits._
        
        val twoCryptos: Array[Crypto] = ds.take(2)
        
        val deltaTimeOfTwoCrypto: Long = 
            twoCryptos.apply(1).cryptoValue.datetime.getTime - twoCryptos.head.cryptoValue.datetime.getTime
        
        val numberOfCryptoOnOneWindow: Int = (maximumDeltaTime/deltaTimeOfTwoCrypto).toInt
    
        import org.apache.spark.sql.expressions.Window
        val cryptoValueColumnName = "crypto.cryptoValue.value"
        val datetimeColumnName = "crypto.cryptoValue.datetime"
        
        val window = Window.orderBy(datetimeColumnName).rowsBetween(-numberOfCryptoOnOneWindow, 0)
    
        import org.apache.spark.sql.functions.{max, min, col, when}
    
        analyticsCryptoDS
          .withColumn("value", col(cryptoValueColumnName))
          .withColumn("max", max("value").over(window))
          .withColumn("min", min("value").over(window))
          .withColumn("variation",
              max(cryptoValueColumnName).over(window) - min(cryptoValueColumnName).over(window))
          .select(datetimeColumnName, "value", "max", "min", "variation", "analytics.derive")
          .withColumn("evolution", 
              when($"min" === $"value" && $"variation" > minDeltaValue, "down")
                .when($"max" === $"value" && $"variation" > minDeltaValue, "up")
                .otherwise("-"))
//          .filter("evolution != '-'")
//          .filter(($"evolution" === "up" && $"derive" < 0) || ($"evolution" === "down" && $"derive" > 0))
          .show(1000, false)
    }
}
