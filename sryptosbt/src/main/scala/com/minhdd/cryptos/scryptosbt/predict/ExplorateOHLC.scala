package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.tools.{DataFrames, Sparks, Timestamps}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ExplorateOHLC {
    
//    val maximumDeltaTime = 4 * Timestamps.oneDayTimestampDelta
    val numberOfMinutesBetweenTwoElement = 15
    val numberOfCryptoOnOneWindow: Int = (4 * 24 *60 / 15)
    val minDeltaValue = 150
    val datetime = "datetime"
    
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
        import ss.implicits._
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath("file:///D:\\ws\\cryptos\\data\\parquets", "XBT", "EUR")
//        val ds: Dataset[Crypto] = Crypto.getPartitionFromPath(ss, parquetPath).get
        val dsFromPath: Dataset[Crypto] = Crypto.getPartitionFromPath(ss, parquetPath).get
        val ds = SamplerObj.sampling(ss, dsFromPath)
        
        val dsWithDatetime =
            ds
              .map(c => (c, c.cryptoValue.datetime.getTime.toDouble / 1000000))
              .withColumnRenamed("_2", datetime)
              .withColumnRenamed("_1", "crypto")
    
        val cryptoValueColumnName = "crypto.cryptoValue.value"
        val datetimeColumnName = "crypto.cryptoValue.datetime"
        val volumeColumnName = "crypto.cryptoValue.volume"
        
        import org.apache.spark.sql.expressions.Window
        
        val window = Window.orderBy(datetimeColumnName, volumeColumnName).rowsBetween(-numberOfCryptoOnOneWindow, 0)
        
        import org.apache.spark.sql.functions.{max, min, col, when, struct}
        
        val evolutionColumnName = "evolution"
        val evolutionNullValue = "-"
        val aaa: DataFrame = dsWithDatetime
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

        val cce = DataFrames.derive(ccc, cryptoValueColumnName, datetime, "derive")
        val ddd = DataFrames.derive(cce, "derive", datetime, "secondDerive")
          .withColumn("analytics",
              struct($"derive", $"secondDerive", $"numberOfStableDay", $"importantChange", $"variation", $"evolution"))
          .as[AnalyticsCrypto]
        
        val eee: DataFrame = ddd
          .select("analytics.*", datetimeColumnName, cryptoValueColumnName)
    
//        eee.filter($"crypto.cryptoValue.datetime" > "2017").show(100000, false)
//          .filter($"importantChange" === true)
//              .filter($"numberOfStableDay" !== 0)
//              .show(1000, false)
//        Sparks.csvFromDataframe("D:\\ws\\cryptos\\data\\csv\\11", eee)
        val numberOfPartition: Int = ddd.rdd.getNumPartitions
        println(numberOfPartition)
        val fff: Dataset[AnalyticsSegment] = ddd.mapPartitions(iterator => split(iterator)).map(AnalyticsSegment(_))
        import org.apache.spark.sql.functions.sum
        val numberOfElement: Long = ddd.count()
        println(numberOfElement)
        val numberOfSegment: Long = fff.count()
        println(numberOfSegment)
        val sumOfSize: Long = fff.agg(sum("numberOfElement")).first().getLong(0)
        println (sumOfSize)
        assert(numberOfElement + numberOfSegment - numberOfPartition == sumOfSize)
        
    }
    
    def split(iterator: Iterator[AnalyticsCrypto]): Iterator[Seq[AnalyticsCrypto]] = {
        if (iterator.hasNext) {
            new Iterator[Seq[AnalyticsCrypto]] {
                var first: AnalyticsCrypto = iterator.next
                override def hasNext: Boolean = iterator.hasNext
                override def next(): Seq[AnalyticsCrypto] = {
                    var nextSeq: Seq[AnalyticsCrypto] = Seq(first)
                    var importantChange: Option[Boolean] = None
                    while (iterator.hasNext && (importantChange.isEmpty || !importantChange.get)) {
                        val actual = iterator.next()
                        importantChange = actual.analytics.importantChange
                        nextSeq = nextSeq :+ actual
                        if (importantChange.isDefined && importantChange.get) {
                            first = actual
                        }
                    }
                    nextSeq
                }
            }
        } else {
            Iterator[Seq[AnalyticsCrypto]]()
        }
    }
    
}
