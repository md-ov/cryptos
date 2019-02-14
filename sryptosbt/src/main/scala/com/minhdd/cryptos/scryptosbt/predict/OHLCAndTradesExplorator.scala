package com.minhdd.cryptos.scryptosbt.predict

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.parquet.Crypto
import com.minhdd.cryptos.scryptosbt.predict.Explorates.CustomSum
import com.minhdd.cryptos.scryptosbt.tools.{DataFrames, Sparks, Statistics}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


case class BeforeSplit(
                      datetime: Timestamp,
                      value: Double,
                      evolution: String,
                      variation: Double,
                      derive: Option[Double],
                      secondDerive: Option[Double],
                      ohlc_value: Double,
                      ohlc_volume: Double,
                      volume: Double,
                      count: BigInt,
                      importantChange: Option[Boolean])

case class Segment (
                   begin: BeforeSplit,
                   end: BeforeSplit,
                   standardDeviationVolume: Double,
                   sameEvolution: Boolean,
                   numberOfElement: Int,
                   averageVolume: Double,
                   averageVariation: Double,
                   standardDeviationVariation: Double,
                   averageDerive: Double,
                   standardDeviationDerive: Double,
                   averageSecondDerive: Double,
                   standardDeviationSecondDerive: Double,
                   averageCount: Double,
                   standardDeviationCount: Double
                   )

object Segment {
    def apply(seq: Seq[BeforeSplit]): Segment = {
        val begin = seq.head
        val end = seq.last
        new Segment(
            begin = begin,
            end = end,
            numberOfElement = seq.size,
            sameEvolution = begin.evolution == end.evolution,
            standardDeviationVolume = Statistics.standardDeviation(seq.map(_.volume)),
            averageVolume = Statistics.avg(seq.map(_.volume)),
            averageVariation = Statistics.avg(seq.map(_.variation)),
            standardDeviationVariation = Statistics.standardDeviation(seq.map(_.variation)),
            averageDerive = Statistics.avg(seq.flatMap(_.derive)),
            standardDeviationDerive = Statistics.standardDeviation(seq.flatMap(_.derive)),
            averageSecondDerive = Statistics.avg(seq.flatMap(_.secondDerive)),
            standardDeviationSecondDerive = Statistics.standardDeviation(seq.flatMap(_.secondDerive)),
            averageCount = Statistics.avg(seq.map(_.count.toDouble)),
            standardDeviationCount = Statistics.standardDeviation(seq.map(_.count.toDouble))
        )
    }
}
object OHLCAndTradesExplorator {
    val numberOfMinutesBetweenTwoElement = 15
    val numberOfCryptoOnOneWindow: Int = (4 * 24 *60 / 15) // sampling every 15 minutes, 4 cryptos on one window
    val minDeltaValue = 150
    val evolutionNullValue = "-"
    val datetime = "datetime"
    val volume = "volume"
    val ohlc_volume = "ohlc_volume"
    val trades_volume = "trades_volume"
    val count = "count"
    val trades_count = "trades_count"
    val value = "value"
    val ohlc_value = "ohlc_value"
    val importantChange = "importantChange"
    val evolution = "evolution"
    val variation = "variation"
    val derive = "derive"
    val numberOfStableDayColumnName = "numberOfStableDay"
    
    
    def explorate(ss:SparkSession, ohlc: Dataset[Crypto], trades: Dataset[Crypto], outputDir: String): Unit = {
        import ss.implicits._
        
        val sampledOhlcDataSet: DataFrame =
            SamplerObj.sampling(ss, ohlc)
              .withColumn(datetime, unix_timestamp(col("cryptoValue.datetime")) / 1000)
              .withColumn(ohlc_volume, col("cryptoValue.volume"))
              .withColumn(count, col("count"))
              .withColumn(ohlc_value, col("cryptoValue.value"))
              .select(datetime, count, ohlc_value, ohlc_volume)
        val sampledTradesDataSet: DataFrame = 
            SamplerObj.sampling(ss, trades)
              .withColumn(datetime, unix_timestamp(col("cryptoValue.datetime")) / 1000)
              .withColumn(volume, col("cryptoValue.volume"))
              .withColumn(value, col("cryptoValue.value"))
              .select(datetime, value, volume)
        val joined = sampledOhlcDataSet.join(sampledTradesDataSet, datetime)
        
        import org.apache.spark.sql.expressions.Window
        val window = Window.orderBy(datetime, volume).rowsBetween(-numberOfCryptoOnOneWindow, 0)
        import org.apache.spark.sql.functions.{max, min, when, struct}

        val dfWithAnalyticsColumns: DataFrame = joined
          .withColumn("max", max("value").over(window))
          .withColumn("min", min("value").over(window))
          .withColumn(variation, max(value).over(window) - min(value).over(window))
    
        val dfWithEvolutionUpOrDown = dfWithAnalyticsColumns.withColumn(evolution,
            when(col("min") === col(value) && col(variation) > minDeltaValue, "down")
              .when(col("max") === col(value) && col(variation) > minDeltaValue, "up")
              .otherwise(evolutionNullValue))

        val binaryEvolution = when(col(evolution) === evolutionNullValue, false).otherwise(true)
        val dfWithImportantChanges: DataFrame = dfWithEvolutionUpOrDown.withColumn(importantChange, binaryEvolution)

        val w = Window.orderBy(datetime, volume)
        val customSum = new CustomSum()
        val dfWithNumberOfStableDay: DataFrame = dfWithImportantChanges
          .withColumn(numberOfStableDayColumnName, customSum(binaryEvolution).over(w))

        val dfWithDerive = DataFrames.derive(
            df = dfWithNumberOfStableDay,
            yColumn = value,
            xColumn = datetime,
            newCol = derive)

        val dfWithSecondDerive: DataFrame =
            DataFrames.derive(
                df = dfWithDerive,
                yColumn = derive,
                xColumn = datetime,
                newCol = "secondDerive")
    
        val beforeSplit: Dataset[BeforeSplit] = dfWithSecondDerive.as[BeforeSplit]
    
        val segments: Dataset[Segment] = beforeSplit.mapPartitions(split).map(Segment(_))
        segments.show(5, false)
    
    
        val segmentsDF: DataFrame =
            segments
              .withColumn("begindt", col("begin.datetime"))
              .withColumn("enddt", col("end.datetime"))
              .withColumn("beginvalue", col("begin.value"))
              .withColumn("endvalue", col("end.value"))
              .withColumn("beginderive", col("begin.derive"))
              .withColumn("endderive", col("end.derive"))
              .withColumn("beginsecondderive", col("begin.secondDerive"))
              .withColumn("endsecondderive", col("end.secondDerive"))
              .withColumn("beginEvolution", col("begin.evolution"))
              .withColumn("endEvolution", col("end.evolution"))
              .withColumn("beginVariation", col("begin.variation"))
              .withColumn("endVariation", col("end.variation"))
              .withColumn("beginVolume", col("begin.volume"))
              .withColumn("endVolume", col("end.volume"))
              .withColumn("beginCount", col("begin.count"))
              .withColumn("ohlcBeginVolume", col("begin.ohlc_volume"))
              .select(
                  "begindt", "enddt", "beginvalue", "endvalue",
                  "beginEvolution", "beginVariation", "beginVolume",
                  "endEvolution", "endVariation", "endVolume",
                  "standardDeviationVolume",
                  "sameEvolution", "numberOfElement", "averageVolume", 
                  "averageVariation", "standardDeviationVariation",
                  "averageDerive", "standardDeviationDerive",
                  "averageSecondDerive", "standardDeviationSecondDerive",
                  "averageCount", "standardDeviationCount",
                  "beginCount", "ohlcBeginVolume",
                  "beginderive", "endderive", "beginsecondderive", "endsecondderive"
              )
    
        Sparks.csvFromDataframe("D:\\ws\\cryptos\\data\\csv\\segments\\" + outputDir, segmentsDF)
    }
    
    def split(iterator: Iterator[BeforeSplit]): Iterator[Seq[BeforeSplit]] = {
        if (iterator.hasNext) {
            new Iterator[Seq[BeforeSplit]] {
                var last: BeforeSplit = iterator.next
                override def hasNext: Boolean = iterator.hasNext
                override def next(): Seq[BeforeSplit] = {
                    var nextSeq: Seq[BeforeSplit] = Seq(last)
                    var cut = false
                    while (iterator.hasNext && !cut) {
                        val actual = iterator.next()
                        nextSeq = nextSeq :+ actual
                        if (actual.value != last.value) {
                            val importantChange = actual.importantChange
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
            Iterator[Seq[BeforeSplit]]()
        }
    }
}
