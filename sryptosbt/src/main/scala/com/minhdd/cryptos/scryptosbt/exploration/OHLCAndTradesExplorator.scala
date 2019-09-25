package com.minhdd.cryptos.scryptosbt.exploration

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.tools.{DataFrames, Statistics, Timestamps}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}


case class BeforeSplit( //représente l'élément juste avant le découpage en segments
                      datetime: Timestamp,
                      value: Double,
                      evolution: String,
                      variation: Double,
                      derive: Option[Double],
                      secondDerive: Option[Double],
                      ohlc_value: Option[Double],
                      ohlc_volume: Option[Double],
                      volume: Double,
                      count: Option[Int],
                      importantChange: Option[Boolean])

object BeforeSplit {
    def encoderSeq(spark: SparkSession): Encoder[Seq[BeforeSplit]] = {
        import spark.implicits._
        implicitly[Encoder[Seq[BeforeSplit]]]
    }
}

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
    def apply(seq: Seq[BeforeSplit], last: BeforeSplit): Segment = {
        val begin = seq.head
        new Segment(
            begin = begin,
            end = last,
            numberOfElement = seq.size,
            sameEvolution = begin.evolution == last.evolution,
            standardDeviationVolume = Statistics.standardDeviation(seq.map(_.volume)),
            averageVolume = Statistics.avg(seq.map(_.volume)),
            averageVariation = Statistics.avg(seq.map(_.variation)),
            standardDeviationVariation = Statistics.standardDeviation(seq.map(_.variation)),
            averageDerive = Statistics.avg(seq.flatMap(_.derive)),
            standardDeviationDerive = Statistics.standardDeviation(seq.flatMap(_.derive)),
            averageSecondDerive = Statistics.avg(seq.flatMap(_.secondDerive)),
            standardDeviationSecondDerive = Statistics.standardDeviation(seq.flatMap(_.secondDerive)),
            averageCount = Statistics.avg(seq.filter(_.count.isDefined).map(_.count.get.toDouble)),
            standardDeviationCount = Statistics.standardDeviation(seq.filter(_.count.isDefined).map(_.count.get.toDouble))
        )
    }
    
    def segments(seq: Seq[BeforeSplit]): Seq[Segment] = {
        def size = seq.size
        (2 to size).map(i => {
            val s = seq.take(i)
            Segment(s, seq.last)
        })
    }

}
object OHLCAndTradesExplorator {
    val numberOfMinutesBetweenTwoElement = 15
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
    
    def expansion(ss: SparkSession, beforeSplitsSeqDataset: Dataset[Seq[BeforeSplit]]) = {
        import ss.implicits._
        val expandedSegments: Dataset[Segment] = beforeSplitsSeqDataset.flatMap(Segment.segments)
    
        val segmentsDF: DataFrame =
            expandedSegments
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
        segmentsDF
    }
    
    def explorate(ss:SparkSession, ohlc: Dataset[Crypto], trades: Dataset[Crypto], outputDir: String): Unit = {
        import ss.implicits._
        
        val sampledOhlcDataSet: DataFrame =
            SamplerObj.sampling(ss, ohlc)
              .withColumn(datetime, unix_timestamp(col("cryptoValue.datetime")) / 1000) //pourquoi diviser par 1000 ? peut etre pour que tableau puisse lire
              .withColumn(ohlc_volume, col("cryptoValue.volume"))
              .withColumn(count, col("count"))
              .withColumn(ohlc_value, col("cryptoValue.value"))
              .select(datetime, count, ohlc_value, ohlc_volume)
        val sampledTradesDataSet: DataFrame = 
            SamplerObj.sampling(ss, trades)
              .withColumn(datetime, unix_timestamp(col("cryptoValue.datetime")) / 1000) //pourquoi diviser par 1000 ? peut etre pour que tableau puisse lire
              .withColumn(volume, col("cryptoValue.volume"))
              .withColumn(value, col("cryptoValue.value"))
              .select(datetime, value, volume)
        val joined: DataFrame = sampledOhlcDataSet.join(sampledTradesDataSet, datetime)
//        println(sampledOhlcDataSet.count())
//        println(sampledTradesDataSet.count())
//        println(joined.count())
    
        import org.apache.spark.sql.expressions.Window
        val window = Window.orderBy(datetime, volume).rowsBetween(-numberOfCryptoOnOneWindow, 0)
        import org.apache.spark.sql.functions.{max, min, when}

        val dfWithAnalyticsColumns: DataFrame = joined
          .withColumn("max", max("value").over(window))
          .withColumn("min", min("value").over(window))
          .withColumn(variation, max(value).over(window) - min(value).over(window))
    
//        dfWithAnalyticsColumns.withColumn("dt", (col("datetime") * 1000).cast(TimestampType))
//          .select("dt", "value", "max", "min", "variation")
//          .show(100000,false)
        
        val dfWithEvolutionUpOrDown = dfWithAnalyticsColumns.withColumn(evolution,
            when(col("min") === col(value) && col(variation) > minDeltaValue, evolutionDown)
              .when(col("max") === col(value) && col(variation) > minDeltaValue, evolutionUp)
              .otherwise(evolutionNone))

        val binaryEvolution = when(col(evolution) === evolutionNone, false).otherwise(true)
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
    
        val beforeSplits: Dataset[BeforeSplit] =  //rétablir 1000 pour le vrai timestamp
            dfWithSecondDerive.as[BeforeSplit].map(b => b.copy(datetime = new Timestamp(b.datetime.getTime * 1000))) 
        
        val beforeSplitsSeqDataset: Dataset[Seq[BeforeSplit]] = beforeSplits.mapPartitions(split)
        beforeSplitsSeqDataset.write.parquet(outputDir+ s"\\${beforesplits}") 
//        val Array(trainingdf, crossValidationdf, testingdf) = beforeSplitsSeqDataset.randomSplit(Array(0.5, 0.2, 0.3), seed=42)
//    
//        val trainingSegments: DataFrame = expansion(ss, trainingdf)
//        val crossValidationSegments = expansion(ss, crossValidationdf)
//        val testSegments = expansion(ss, testingdf)
//        Sparks.csvFromDataframe(outputDir + "\\training", trainingSegments)
//        Sparks.csvFromDataframe(outputDir + "\\crossvalidation", crossValidationSegments)
//        Sparks.csvFromDataframe(outputDir + "\\test", testSegments)
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
    
    def tradesFromLastSegment(ss: SparkSession, lastTimestamps: Timestamp,
                              lastCryptoPartitionKey: CryptoPartitionKey): Dataset[Crypto] = {
        //        val parquetPath = CryptoPartitionKey.getTRADESParquetPath(
        //            parquetsDir = "D:\\ws\\cryptos\\data\\parquets", asset = "XBT", currency = "EUR")
        val parquetPath = "D://ws//cryptos//data//parquets"
        val todayPath = "D://ws//cryptos//data//parquets//XBT//EUR//TRADES//today//parquet"
        Crypto.getPartitionsUniFromPathFromLastTimestamp(
            ss = ss, prefix = "file:///",
            path1 = parquetPath, path2 = parquetPath, todayPath = todayPath,
            ts = lastTimestamps, lastCryptoPartitionKey = lastCryptoPartitionKey).get
    }
    
    def ohlcCryptoDsFromLastSegment(ss: SparkSession, lastTimestamp: Timestamp): Dataset[Crypto] = {
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath(
            parquetsDir = s"file:///$dataDirectory\\parquets", asset = "XBT", currency = "EUR")
        Crypto.getPartitionFromPathFromLastTimestamp(ss, parquetPath, lastTimestamp).get
    }
    
    def explorateFromLastSegment(ss: SparkSession,
                                 allTargetedSegments: Dataset[Seq[BeforeSplit]],
                                 outputDir: String): Unit = {
        import ss.implicits._
        val lastTimestampDS: Dataset[Timestamp] = allTargetedSegments.map(_.last.datetime)
        val lastTimestamp: Timestamp = lastTimestampDS.agg(max("value").as("max")).first().getAs[Timestamp](0)
        val ts: Timestamps = Timestamps(lastTimestamp.getTime)
        
        val lastCryptoPartitionKey = CryptoPartitionKey(
            asset = "XBT",
            currency = "EUR",
            provider = "KRAKEN",
            api = "TRADES",
            year = ts.getYearString,
            month = ts.getMonthString,
            day = ts.getDayString)
        
        val ohlcs = ohlcCryptoDsFromLastSegment(ss, lastTimestamp)
        val trades: Dataset[Crypto] = tradesFromLastSegment(ss, lastTimestamp, lastCryptoPartitionKey)
        OHLCAndTradesExplorator.explorate(ss, ohlcs, trades, outputDir)
    }
    
    def allSegments(ss: SparkSession, last: String, now: String): Unit = {
        val lastSegmentsDir = s"$dataDirectory\\segments\\$last\\$beforesplits"
        val afterLastSegmentDir = s"$dataDirectory\\segments\\$now"
        import ss.implicits._
        val allCalculatedSegments: Dataset[Seq[BeforeSplit]] = ss.read.parquet(lastSegmentsDir).as[Seq[BeforeSplit]]
        
        val allTargetedSegments: Dataset[Seq[BeforeSplit]] =
            allCalculatedSegments.filter(_.last.importantChange.getOrElse(false) == true)
        
        explorateFromLastSegment(
            ss = ss,
            allTargetedSegments = allTargetedSegments,
            outputDir = afterLastSegmentDir)
    
        val lastSegments: Dataset[Seq[BeforeSplit]] = ss.read.parquet(s"$dataDirectory\\segments\\$now\\$beforesplits").as[Seq[BeforeSplit]]
        val finalDs = allTargetedSegments.union(lastSegments)
        finalDs.write.parquet(s"$dataDirectory\\segments\\$now-fusion\\$beforesplits")
    }
}
