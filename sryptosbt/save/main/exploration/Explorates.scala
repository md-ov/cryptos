package com.minhdd.cryptos.scryptosbt.exploration

import com.minhdd.cryptos.scryptosbt.domain.{AnalyticsCrypto, AnalyticsSegment, Crypto, RegularSegment}
import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.tools.{DataFrameHelper, SparkHelper}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Explorates {
    //    val maximumDeltaTime = 4 * Timestamps.oneDayTimestampDelta
    val numberOfMinutesBetweenTwoElement = 15
    val numberOfCryptoOnOneWindow: Int = (4 * 24 *60 / 15) // sampling every 15 minutes, 4 cryptos on one window
    val minDeltaValue = 150
    val datetime = "datetime"
    val cryptoValueColumnName = "crypto.cryptoValue.value"
    val datetimeColumnName = "crypto.cryptoValue.datetime"
    val volumeColumnName = "crypto.cryptoValue.volume"
    
    def toAnalytics(ss: SparkSession, sampledDataSet: Dataset[Crypto]): Dataset[AnalyticsCrypto] = {
        import ss.implicits._
        
        val dfWithDatetimeAndWrappedCrypto: DataFrame =
            sampledDataSet
              .map(c => (c, c.cryptoValue.datetime.getTime.toDouble / 1000000))
              .withColumnRenamed("_1", "crypto")
              .withColumnRenamed("_2", datetime)
        
        val volumeColumnName = "crypto.cryptoValue.volume"
        
        import org.apache.spark.sql.expressions.Window
        val window = Window.orderBy(datetimeColumnName, volumeColumnName).rowsBetween(-numberOfCryptoOnOneWindow, 0)
        import org.apache.spark.sql.functions.{max, min, struct, when}
        
        val dfWithAnalyticsColumns: DataFrame = dfWithDatetimeAndWrappedCrypto
          .withColumn("value", col(cryptoValueColumnName))
          .withColumn("max", max("value").over(window))
          .withColumn("min", min("value").over(window))
          .withColumn("variation", max(cryptoValueColumnName).over(window) - min(cryptoValueColumnName).over(window))
        
        val dfWithEvolutionUpOrDown = dfWithAnalyticsColumns.withColumn("evolution",
            when(col("min") === col("value") && col("variation") > minDeltaValue, evolutionDown)
              .when(col("max") === col("value") && col("variation") > minDeltaValue, evolutionUp)
              .otherwise(evolutionNone))
        
        //          dfWithEvolutionUpOrDown.filter("evolution != '-'")
        //          .filter(($"evolution" === "up" && $"derive" < 0) || ($"evolution" === evolutionDown && $"derive" > 0))
        //          .select(datetimeColumnName, "value", "variation", "evolution", "analytics.derive")
        //          .show(1000, false)
        
        val binaryEvolution = when(col("evolution") === evolutionNone, false).otherwise(true)
        val dfWithImportantChanges: DataFrame = dfWithEvolutionUpOrDown.withColumn("importantChange", binaryEvolution)
        
        val w = Window.orderBy(datetimeColumnName, volumeColumnName)
        val customSum = new CustomSum()
        val numberOfStableDayColumnName = "numberOfStableDay"
        val dfWithNumberOfStableDay: DataFrame = dfWithImportantChanges
          .withColumn(numberOfStableDayColumnName, customSum(binaryEvolution).over(w))
        
        val dfWithDerive = DataFrameHelper.derive(
            df = dfWithNumberOfStableDay,
            yColumn = cryptoValueColumnName,
            xColumn = datetime,
            newCol = "derive")
        
        val dfWithSecondDerive: DataFrame =
            DataFrameHelper.derive(
                df = dfWithDerive,
                yColumn = "derive",
                xColumn = datetime,
                newCol = "secondDerive")
        
        val analyticsCrypto =
            dfWithSecondDerive.withColumn("analytics",
                struct(
                    col("derive"),
                    col("secondDerive"),
                    col("numberOfStableDay"),
                    col("importantChange"),
                    col("variation"),
                    col("evolution")))
              .as[AnalyticsCrypto]
        
        analyticsCrypto
    }
    
    def toSegmentsAndTrends(ss: SparkSession, analyticsCrypto: Dataset[AnalyticsCrypto], outputDir: String) = {
        import ss.implicits._
        
        val segments: Dataset[AnalyticsSegment] =
            analyticsCrypto.mapPartitions(splitAnalyticsCryptos).map(AnalyticsSegment(_))
        
        val segmentsDF: DataFrame =
            segments
              .withColumn("begindt", col("begin.crypto.cryptoValue.datetime"))
              .withColumn("beginvalue", col("begin.crypto.cryptoValue.value"))
              .withColumn("enddt", col("end.crypto.cryptoValue.datetime"))
              .withColumn("endvalue", col("end.crypto.cryptoValue.value"))
              .select(
                  "begindt", "enddt", "beginvalue", "endvalue",
                  "beginEvolution", "beginVariation", "beginVolume",
                  "endEvolution", "endVariation", "endVolume",
                  "standardDeviationVolume",
                  "sameEvolution", "numberOfElement")
        
        SparkHelper.csvFromDataframe("D:\\ws\\cryptos\\data\\segments\\" + outputDir, segmentsDF)
        
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
    
        val window = Window.orderBy("beginTimestamp1").rowsBetween(Long.MinValue, 0)
        
        val regularTrends: DataFrame = segments.mapPartitions(splitSegments).map(RegularSegment(_))
          .select("beginTimestamp1", "beginTimestamp2", "endTimestamp1", "endTimestamp2", "beginValue", "endValue", 
              "days", "pattern", "evolution", "numberOfSegment", "beginVariation", "endVariation", 
              "ecartTypeVariations")
            .withColumn("days-with-sign", 
                when(col("evolution") === evolutionUp, col("days")).otherwise(col("days") * -1)
            )
            .withColumn("cumdays", sum(col("days-with-sign")).over(window))
    
        SparkHelper.csvFromDataframe("D:\\ws\\cryptos\\data\\csv\\trends\\" + outputDir, regularTrends)
    }
    
    def printAnalyticsCrypto(analyticsCrypto: Dataset[AnalyticsCrypto], outputDir: String) = {
        val selectedAnalytics: DataFrame =
            analyticsCrypto.select("analytics.*", datetimeColumnName, cryptoValueColumnName, volumeColumnName)
        //        selectedAnalytics
        // .filter($"crypto.cryptoValue.datetime" > "2017").show(100000, false)
        //          .filter($"importantChange" === true)
        //              .filter($"numberOfStableDay" !== 0)
        //              .show(10000, false)
        SparkHelper.csvFromDataframe("D:\\ws\\cryptos\\data\\csv\\evolutions\\" + outputDir, selectedAnalytics)
    }
    
    def splitAnalyticsCryptos(iterator: Iterator[AnalyticsCrypto]): Iterator[Seq[AnalyticsCrypto]] = {
        if (iterator.hasNext) {
            new Iterator[Seq[AnalyticsCrypto]] {
                var last: AnalyticsCrypto = iterator.next
                override def hasNext: Boolean = iterator.hasNext
                override def next(): Seq[AnalyticsCrypto] = {
                    var nextSeq: Seq[AnalyticsCrypto] = Seq(last)
                    var cut = false
                    while (iterator.hasNext && !cut) {
                        val actual = iterator.next()
                        nextSeq = nextSeq :+ actual
                        if (actual.crypto.cryptoValue.value != last.crypto.cryptoValue.value) {
                            val importantChange = actual.analytics.importantChange
                            if (importantChange.isDefined && importantChange.get) {
                                cut = true
                            }
                        }
                        last = actual
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
    
    def run(ss:SparkSession, dsOfCrypto: Dataset[Crypto], outputDir: String) = {
        val sampledDataSet: Dataset[Crypto] = SamplerObj.sampling(ss, dsOfCrypto)
        val analyticsCrypto: Dataset[AnalyticsCrypto] = toAnalytics(ss, sampledDataSet)
        toSegmentsAndTrends(ss, analyticsCrypto, outputDir)
//        printAnalyticsCrypto(analyticsCrypto, outputDir)
    }
}
