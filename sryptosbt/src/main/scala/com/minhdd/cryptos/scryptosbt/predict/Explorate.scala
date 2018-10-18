package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.tools.Timestamps
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object Explorate {
    
    val maximumDeltaTime = 4 * Timestamps.oneDayTimestampDelta
    val minDeltaValue = 350
    val cryptoValueColumnName = "crypto.cryptoValue.value"
    val datetimeColumnName = "crypto.cryptoValue.datetime"
    val volumeColumnName = "crypto.cryptoValue.volume"
    
    class CustomSum extends UserDefinedAggregateFunction {
        override def inputSchema: org.apache.spark.sql.types.StructType =
            StructType(StructField("toMark", BooleanType) :: Nil)
        
        override def bufferSchema: StructType = StructType(
            StructField("value", LongType) :: StructField("counter", LongType) :: Nil
        )
        override def dataType: DataType = LongType
        override def deterministic: Boolean = true
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L // la valeur de sortie
            buffer(1) = 0L // le compteur
        }
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            if (buffer(1) != buffer(0)) {
                buffer(0) = 0L
                buffer(1) = 0L
            }
            buffer(1) = buffer.getLong(1) + 1
            if (input.getAs[Boolean](0)) {  
                 
            } else {  
                buffer(0) = buffer.getLong(0) + 1 
            }
        }
        
        override def merge(buffer: MutableAggregationBuffer, buffer2: Row): Unit = {
            println("merge buffer " + buffer(0))
            println("merge row" + buffer.getAs[Long](0))
        }
        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0)
        }
    }
    
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("explorate").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath("file:///D:\\ws\\cryptos\\data\\parquets", "XBT", "EUR")
        val ds: Dataset[Crypto] = Crypto.getPartitionFromPath(ss, parquetPath).get
        val dsWithDerive: DataFrame = DerivativeCrypto.deriveWithWindow(ds, ss)
        import ss.implicits._
        
        val twoCryptos: Array[Crypto] = ds.take(2)
        
        val deltaTimeOfTwoCrypto: Long = 
            twoCryptos.apply(1).cryptoValue.datetime.getTime - twoCryptos.head.cryptoValue.datetime.getTime
        
        val numberOfCryptoOnOneWindow: Int = (maximumDeltaTime/deltaTimeOfTwoCrypto).toInt
    
        import org.apache.spark.sql.expressions.Window
        
        val window = Window.orderBy(datetimeColumnName).rowsBetween(-numberOfCryptoOnOneWindow, 0)
    
        import org.apache.spark.sql.functions.{max, min, col, when}
        import org.apache.spark.sql.functions.{lit, struct}
    
        val evolutionColumnName = "evolution"
        val evolutionNullValue = "-"
        val aaa: DataFrame = dsWithDerive
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
    
        val w = Window.orderBy(datetimeColumnName, volumeColumnName)
        
        val binaryEvolution = when($"evolution" === evolutionNullValue, false).otherwise(true)
        val numberOfStableDayColumnName = "numberOfStableDay"
        val customSum = new CustomSum()
        val ccc: DataFrame = aaa
          .withColumn("importantChange", binaryEvolution)
          .withColumn(numberOfStableDayColumnName, customSum(binaryEvolution).over(w))
//        ccc
//          .select(datetimeColumnName, "value", "variation", evolutionColumnName, "derive", numberOfStableDayColumnName)
//          .show(2, false)
    
        val ddd = ccc
          .withColumn("secondDerive", lit(0))
          
          .withColumn("analytics", 
              struct($"derive", $"secondDerive", $"numberOfStableDay", $"importantChange", $"variation", $"evolution"))
          .select("crypto", "analytics").as[AnalyticsCrypto]
    
        ddd
          .select("crypto.cryptoValue.datetime","analytics.*")
          .filter($"importantChange" === true)
          .filter($"numberOfStableDay" !== 0)
          .show(1000, false)
    }
    
    
   }
