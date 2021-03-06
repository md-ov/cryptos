package com.minhdd.cryptos.scryptosbt.exploration

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.parquet.CryptoPartitionKey
import com.minhdd.cryptos.scryptosbt.tools.{DataFrameHelper, StatisticHelper, TimestampHelper}
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
            standardDeviationVolume = StatisticHelper.standardDeviation(seq.map(_.volume)),
            averageVolume = StatisticHelper.avg(seq.map(_.volume)),
            averageVariation = StatisticHelper.avg(seq.map(_.variation)),
            standardDeviationVariation = StatisticHelper.standardDeviation(seq.map(_.variation)),
            averageDerive = StatisticHelper.avg(seq.flatMap(_.derive)),
            standardDeviationDerive = StatisticHelper.standardDeviation(seq.flatMap(_.derive)),
            averageSecondDerive = StatisticHelper.avg(seq.flatMap(_.secondDerive)),
            standardDeviationSecondDerive = StatisticHelper.standardDeviation(seq.flatMap(_.secondDerive)),
            averageCount = StatisticHelper.avg(seq.filter(_.count.isDefined).map(_.count.get.toDouble)),
            standardDeviationCount = StatisticHelper.standardDeviation(seq.filter(_.count.isDefined).map(_.count.get.toDouble))
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

        val dfWithDerive = DataFrameHelper.derive(
            df = dfWithNumberOfStableDay,
            yColumn = value,
            xColumn = datetime,
            newCol = derive)

        val dfWithSecondDerive: DataFrame =
            DataFrameHelper.derive(
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
            spark = ss, prefix = "file:///",
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
        val ts: TimestampHelper = TimestampHelper(lastTimestamp.getTime)
        
        val lastCryptoPartitionKey = CryptoPartitionKey(
            asset = "XBT",
            currency = "EUR",
            provider = "KRAKEN",
            api = "TRADES",
            year = ts.getYear,
            month = ts.getMonth,
            day = ts.getDay)
        
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
