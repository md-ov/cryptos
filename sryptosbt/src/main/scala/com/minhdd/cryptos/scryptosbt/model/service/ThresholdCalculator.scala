package com.minhdd.cryptos.scryptosbt.model.service

import ml._
import org.apache.spark.sql.functions._
import com.minhdd.cryptos.scryptosbt.model.domain.Rates
import com.minhdd.cryptos.scryptosbt.model.service.ml.{label, predict, prediction}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{DataFrame, SparkSession}

object ThresholdCalculator {
    
    def exploreDfAndFindThreshold(ss: SparkSession, df: DataFrame): ((Double, Rates), (Double, Rates)) = {
//        for (i <- 0 to 10) {
//            val binarizerForSegmentDetection = new Binarizer()
//              .setInputCol(prediction)
//              .setOutputCol(predict)
//            println("threshold : " + i.toDouble / 10)
//            binarizerForSegmentDetection.setThreshold(i.toDouble / 10)
//            val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(df)
//            val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
//            counts.show()
//        }
        val rates: Seq[(Double, Rates)] = getRates(df)
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
        
        val centeredThresholdForPositive: (Double, Rates) = bestRateForPositive(rates)
        println("center threshold for positive : " + centeredThresholdForPositive._1 + " - " + centeredThresholdForPositive._2)
        val thresholdForPositive: (Double, Rates) = getAdjustedThresholdForPositive(df, centeredThresholdForPositive._1, precision = 10)
        println("threshold for positive: " + thresholdForPositive)
        println("results for positive: ")
        binarizerForSegmentDetection.setThreshold(thresholdForPositive._1)
        val segmentDetectionBinaryResultsForPositive = binarizerForSegmentDetection.transform(df)
        val countsForPositive: DataFrame = segmentDetectionBinaryResultsForPositive.groupBy(label, predict).count()
        countsForPositive.show()
    
        val centeredThresholdForNegative: (Double, Rates) = bestRateForNegative(rates)
        val thresholdForNegative: (Double, Rates) = getAdjustedThresholdForNegative(df, centeredThresholdForNegative._1, precision = 10)
        println("threshold for negative: " + thresholdForNegative)
        println("results for negative: ")
        binarizerForSegmentDetection.setThreshold(thresholdForNegative._1)
        val segmentDetectionBinaryResultsForNegative = binarizerForSegmentDetection.transform(df)
        val countsForNegative: DataFrame = segmentDetectionBinaryResultsForNegative.groupBy(label, predict).count()
        countsForNegative.show()
        
        (thresholdForPositive, thresholdForNegative)
    }
    
    private def getCountsDf(df: DataFrame, threshold: Double): DataFrame = {
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
          .setThreshold(threshold)
        val segmentDetectionBinaryResults: DataFrame = binarizerForSegmentDetection.transform(df)
        val counts: DataFrame = segmentDetectionBinaryResults.groupBy(label, predict).count()
        counts
    }
    
    private def extractThridValueWithTwoFilter(counts: DataFrame, labelValue: Int, predictValue: Double): Long = {
        import org.apache.spark.sql.functions._
        val row = counts.filter(col("label") === labelValue).filter(col("predict") === predictValue)
        
        if (row.count() == 1) {
            row.first().getAs[Long](2)
        } else {
            0
        }
    }
    
    private def getRates(s: Seq[Double], df: DataFrame): Seq[(Double, Rates)] = s.map(getRates(df, _))
    
    private def getRates(df: DataFrame, threshold: Double): (Double, Rates) = {
        val counts: DataFrame = getCountsDf(df, threshold)
        val truePositive: Long = extractThridValueWithTwoFilter(counts, 1, 1.0)
        val falsePositive: Long = extractThridValueWithTwoFilter(counts, 0, 1.0) +
          extractThridValueWithTwoFilter(counts, -1, 1.0)
        val falseNegative: Long = extractThridValueWithTwoFilter(counts, 1, 0.0)+
        extractThridValueWithTwoFilter(counts, -1, 0.0)
        val trueNegative: Long = extractThridValueWithTwoFilter(counts, 0, 0.0) 
        val total = truePositive + falsePositive + falseNegative + trueNegative
        val rate1 = truePositive.toDouble / (truePositive + falsePositive)
        val rate2 = trueNegative.toDouble / (falseNegative + trueNegative)
        val rate3 = (truePositive + falsePositive).toDouble / total
        val rate4 = (trueNegative + falseNegative).toDouble / total
        val rate5 = (truePositive + trueNegative).toDouble / total
        (threshold, Rates(rate1, rate2, rate3, rate4, rate5))
    }
    
    private def bestRateForPositive(rates: Seq[(Double, Rates)]): (Double, Rates) = {
        rates
          .filter(_._2.truePositiveOnPositive > minimumTruePositiveRate)
          .filter(_._2.positiveRate > minimumPositiveRate)
          .maxBy(_._2.truePositiveOnPositive)
    }
    
    private def bestRateForNegative(rates: Seq[(Double, Rates)]): (Double, Rates) = {
        rates
          .filter(_._2.trueNegativeOnNegative > minimumTrueNegativeRate)
          .filter(_._2.negativeRate > minimumNegativeRate)
          .maxBy(_._2.trueNegativeOnNegative)
    }
    
    private def getRates(df: DataFrame): Seq[(Double, Rates)] = {
        import df.sparkSession.implicits._
        val minAndMaxDataFrame: DataFrame = df.agg(min(prediction), max(prediction))
        val minValue: Double = minAndMaxDataFrame.map(_.getDouble(0)).first()
        val maxValue: Double = minAndMaxDataFrame.map(_.getDouble(1)).first()
        val diff: Double = maxValue - minValue
        val samplingThresholds: Seq[Int] = 0 until 40
        val thresholds: Seq[Double] = samplingThresholds.map(s => minValue + (s * (diff / (samplingThresholds.length - 1))))
        val rates: Seq[(Double, Rates)] = getRates(thresholds, df)
        rates.foreach(println)
        rates
    }
    
    private def getAdjustedThresholdForPositive(df: DataFrame, centeredThreshold: Double, precision: Int): (Double, Rates) = {
        val rates: Seq[(Double, Rates)] = getThresholdAndRatesAroundCenter(df, centeredThreshold, precision)
        bestRateForPositive(rates)
    }
    
    private def getAdjustedThresholdForNegative(df: DataFrame, centeredThreshold: Double, precision: Int): (Double, Rates) = {
        val rates: Seq[(Double, Rates)] = getThresholdAndRatesAroundCenter(df, centeredThreshold, precision)
        bestRateForNegative(rates)
    }
    
    private def getThresholdAndRatesAroundCenter(df: DataFrame, centeredThreshold: Double, precision: Int) = {
        val epsilon = 0.005
        val thresholds = (-precision until precision).map(e => centeredThreshold + e * epsilon)
        val rates: Seq[(Double, Rates)] = getRates(thresholds, df)
        rates.foreach(println)
        println("----------")
        rates
    }
}
