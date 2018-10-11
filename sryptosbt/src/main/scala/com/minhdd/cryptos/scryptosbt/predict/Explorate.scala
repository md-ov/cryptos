package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.tools.Timestamps
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

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
    
        val evolutionColumnName = "evolution"
        val evolutionNullValue = "-"
        val aaa = analyticsCryptoDS
          .withColumn("value", col(cryptoValueColumnName))
          .withColumn("max", max("value").over(window))
          .withColumn("min", min("value").over(window))
          .withColumn("variation",
              max(cryptoValueColumnName).over(window) - min(cryptoValueColumnName).over(window))
          .withColumn(evolutionColumnName, 
              when($"min" === $"value" && $"variation" > minDeltaValue, "down")
                .when($"max" === $"value" && $"variation" > minDeltaValue, "up")
                .otherwise(evolutionNullValue))
//          .filter("evolution != '-'")
//          .filter(($"evolution" === "up" && $"derive" < 0) || ($"evolution" === "down" && $"derive" > 0))
//          .select(datetimeColumnName, "value", "variation", "evolution", "analytics.derive")
//          .show(1000, false)
    
        val w = Window.orderBy(datetimeColumnName)
        
        val binaryEvolution = when($"evolution" === evolutionNullValue, 1).otherwise(0)
        val persistenceColumnName = "persistence"
        val customSum = new CustomSum()
        val persistence: Column = customSum(binaryEvolution).over(w)

        val ccc = aaa.withColumn(persistenceColumnName, persistence)
            ccc.select(datetimeColumnName, "value", "variation", evolutionColumnName, "analytics.derive", persistenceColumnName)
          .show(1000, false)
    
    
    }
    
    class CustomSum extends UserDefinedAggregateFunction {
        override def inputSchema: org.apache.spark.sql.types.StructType =
            StructType(StructField("value", DoubleType) :: Nil)
        
        override def bufferSchema: StructType = StructType(
            StructField("value", LongType) :: Nil
        )
        override def dataType: DataType = LongType
        override def deterministic: Boolean = true
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
        }
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            val value2 = input.getAs[Double](0)
            if (value2 == 0) buffer(0) = 0L else buffer(0) = buffer.getAs[Long](0) + 1
        }
        override def merge(buffer: MutableAggregationBuffer, buffer2: Row): Unit = {
            println("merge buffer " + buffer(0))
            println("merge row" + buffer.getAs[Long](0))
        }
        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0)
        }
    }
   }
