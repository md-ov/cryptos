package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.tools.DataFrames
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ExplorateOHLC {
    
//    val maximumDeltaTime = 4 * Timestamps.oneDayTimestampDelta
    val numberOfMinutesBetweenTwoElement = 15
    val numberOfCryptoOnOneWindow: Int = (4 * 24 *60 / 15) // sampling every 15 minutes, 4 cryptos on one window
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
        val ss: SparkSession = SparkSession.builder().appName("exploration OHLC").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        import ss.implicits._
        
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath(
            parquetsDir = "file:///D:\\ws\\cryptos\\data\\parquets", 
            asset = "XBT", 
            currency = "EUR")
        
        val dsOfCrypto: Dataset[Crypto] = Crypto.getPartitionFromPath(ss, parquetPath).get
        
        val sampledDataSet = SamplerObj.sampling(ss, dsOfCrypto)
        
        val dfWithDatetimeAndWrappedCrypto: DataFrame =
            sampledDataSet
              .map(c => (c, c.cryptoValue.datetime.getTime.toDouble / 1000000))
              .withColumnRenamed("_1", "crypto")
              .withColumnRenamed("_2", datetime)
              
        val cryptoValueColumnName = "crypto.cryptoValue.value"
        val datetimeColumnName = "crypto.cryptoValue.datetime"
        val volumeColumnName = "crypto.cryptoValue.volume"
        val evolutionNullValue = "-"
        
        import org.apache.spark.sql.expressions.Window
        val window = Window.orderBy(datetimeColumnName, volumeColumnName).rowsBetween(-numberOfCryptoOnOneWindow, 0)
        import org.apache.spark.sql.functions.{max, min, col, when, struct}
        
        val dfWithAnalyticsColumns: DataFrame = dfWithDatetimeAndWrappedCrypto
          .withColumn("value", col(cryptoValueColumnName))
          .withColumn("max", max("value").over(window))
          .withColumn("min", min("value").over(window))
          .withColumn("variation", max(cryptoValueColumnName).over(window) - min(cryptoValueColumnName).over(window))
          
          val dfWithEvolutionUpOrDown = dfWithAnalyticsColumns.withColumn("evolution", 
                when($"min" === $"value" && $"variation" > minDeltaValue, "down")
                .when($"max" === $"value" && $"variation" > minDeltaValue, "up")
                .otherwise(evolutionNullValue))
        
//          dfWithEvolutionUpOrDown.filter("evolution != '-'")
//          .filter(($"evolution" === "up" && $"derive" < 0) || ($"evolution" === "down" && $"derive" > 0))
//          .select(datetimeColumnName, "value", "variation", "evolution", "analytics.derive")
//          .show(1000, false)
        
        val binaryEvolution = when($"evolution" === evolutionNullValue, false).otherwise(true)
        val dfWithImportantChanges: DataFrame = dfWithEvolutionUpOrDown.withColumn("importantChange", binaryEvolution)
    
        val w = Window.orderBy(datetimeColumnName, volumeColumnName)
        val customSum = new CustomSum()
        val numberOfStableDayColumnName = "numberOfStableDay"
        val dfWithNumberOfStableDay: DataFrame = dfWithImportantChanges
          .withColumn(numberOfStableDayColumnName, customSum(binaryEvolution).over(w))

        val dfWithDerive = DataFrames.derive(
            df = dfWithNumberOfStableDay, 
            yColumn = cryptoValueColumnName, 
            xColumn = datetime, 
            newCol = "derive")
        
        val dfWithSecondDerive: DataFrame = 
            DataFrames.derive(
                df = dfWithDerive, 
                yColumn = "derive", 
                xColumn = datetime, 
                newCol = "secondDerive")
              
        val analyticsCrypto = dfWithSecondDerive.withColumn("analytics",
              struct($"derive", $"secondDerive", $"numberOfStableDay", $"importantChange", $"variation", $"evolution"))
            .as[AnalyticsCrypto]
        
//        val eee: DataFrame = analyticsCrypto.select("analytics.*", datetimeColumnName, cryptoValueColumnName)
//        eee.filter($"crypto.cryptoValue.datetime" > "2017").show(100000, false)
//          .filter($"importantChange" === true)
//              .filter($"numberOfStableDay" !== 0)
//              .show(1000, false)
//        Sparks.csvFromDataframe("D:\\ws\\cryptos\\data\\csv\\11", eee)
        
        val segments: Dataset[AnalyticsSegment] =
            analyticsCrypto.mapPartitions(splitAnalyticsCryptos).map(AnalyticsSegment(_))
        
        //verification
        val numberOfPartition: Int = analyticsCrypto.rdd.getNumPartitions
        println(numberOfPartition)
        import org.apache.spark.sql.functions.sum
        val numberOfElement: Long = analyticsCrypto.count()
        println(numberOfElement)
        val numberOfSegment: Long = segments.count()
        println(numberOfSegment)
        val sumOfSize: Long = segments.agg(sum("numberOfElement")).first().getLong(0)
        println (sumOfSize)
        if (!(numberOfElement + numberOfSegment - numberOfPartition == sumOfSize)) {
            println("not equal ! ")   
        }
        
        val regularTrends: DataFrame = segments.mapPartitions(splitSegments).map(RegularSegment(_))
          .select("beginTimestamp", "beginValue", "endTimestamp", "endValue", "days", "pattern")
          .sort("beginTimestamp")
    
        regularTrends.show(10000, false)
        
    }
    
    def splitAnalyticsCryptos(iterator: Iterator[AnalyticsCrypto]): Iterator[Seq[AnalyticsCrypto]] = {
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
    
    def splitSegments(iterator: Iterator[AnalyticsSegment]): Iterator[Seq[AnalyticsSegment]] = {
        if (iterator.hasNext) {
            new Iterator[Seq[AnalyticsSegment]] {
                var first: AnalyticsSegment = iterator.next
                override def hasNext: Boolean = iterator.hasNext
                override def next(): Seq[AnalyticsSegment] = {
                    var nextSeq: Seq[AnalyticsSegment] = Seq(first)
                    var last = first
                    var continuation = true
                    while (iterator.hasNext && continuation) {
                        val actual = iterator.next()
                        nextSeq = nextSeq :+ actual
                        if (last.endEvolution == actual.beginEvolution && actual.sameEvolution) {
                            last = actual
                        } else {
                            first = actual
                            continuation = false
                        }
                    }
                    nextSeq
                }
            }
        } else {
            Iterator[Seq[AnalyticsSegment]]()
        }
    }
}
