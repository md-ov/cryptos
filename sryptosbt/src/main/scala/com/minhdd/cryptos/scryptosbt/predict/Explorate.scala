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
        import ss.implicits._
        
        val twoCryptos: Array[Crypto] = ds.take(2)
        
        val deltaTimeOfTwoCrypto: Long = 
            twoCryptos.apply(1).cryptoValue.datetime.getTime - twoCryptos.head.cryptoValue.datetime.getTime
        
        val numberOfCryptoOnOneWindow: Int = (maximumDeltaTime/deltaTimeOfTwoCrypto).toInt
    
        import org.apache.spark.sql.expressions.Window
        val window = Window.orderBy("cryptoValue.datetime").rowsBetween(-numberOfCryptoOnOneWindow, 0)
    
        import org.apache.spark.sql.functions.{max, min, col, when}
        
        ds
          .withColumn("value", col("cryptoValue.value"))
          .withColumn("max", max("value").over(window))
          .withColumn("min", min("value").over(window))
          .withColumn("variation",
              max("cryptoValue.value").over(window) - min("cryptoValue.value").over(window))
          .filter("variation > " + minDeltaValue)
          .select("cryptoValue.datetime", "value", "max", "min", "variation")
          .withColumn("evolution", 
              when($"min" === $"value", "down").when($"max" === $"value", "up").otherwise("-"))
          .filter("evolution != '-'")
          .show(1000, false)
    }
}
