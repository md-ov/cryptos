package com.minhdd.cryptos.scryptosbt.model.service

import ml._
import org.apache.spark.sql.functions._
import com.minhdd.cryptos.scryptosbt.model.domain.Rates
import com.minhdd.cryptos.scryptosbt.model.service.ml.{label, predict, prediction}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{DataFrame, SparkSession}

object ThresholdCalculator {
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
    
    def getRates(df: DataFrame, threshold: Double): (Double, Rates) = {
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
        val rate4 = (truePositive + trueNegative).toDouble / total
        (threshold, Rates(rate1, rate2, rate3, rate4))
    }
    
    private def bestRate(rates: Seq[(Double, Rates)]): (Double, Rates) = {
        rates
          .filter(_._2.truePositiveOnPositive > minimumTruePositiveRate)
          .filter(_._2.positiveRate > minimumPositiveRate)
          .maxBy(_._2.positiveRate)
    }
    
    private def getCenteredThreshold(ss: SparkSession, df: DataFrame): (Double, Rates) = {
        import ss.implicits._
        val minAndMaxDataFrame: DataFrame = df.agg(min(prediction), max(prediction))
        val minValue: Double = minAndMaxDataFrame.map(_.getDouble(0)).first()
        val maxValue: Double = minAndMaxDataFrame.map(_.getDouble(1)).first()
        val diff: Double = maxValue - minValue
        val samplingThresholds: Seq[Int] = 0 until 40
        val thresholds: Seq[Double] = samplingThresholds.map(s => minValue + (s * (diff/(samplingThresholds.length - 1))))
        val rates = getRates(thresholds, df)
        bestRate(rates)
    }
    
    def getAdjustedThreshold(ss: SparkSession, df: DataFrame, centeredThreshold: Double, precision: Int): (Double, Rates) = {
        val epsilon = 0.0005
        val thresholds = (-precision until precision).map(e => centeredThreshold + e*epsilon)
        val rates = getRates(thresholds, df)
        rates.foreach(println)
        println("----------")
        bestRate(rates)
    }
    
    def exploreDfAndFindThreshold(ss: SparkSession, df: DataFrame): (Double, Rates) = {
        for (i <- 0 to 10) {
            val binarizerForSegmentDetection = new Binarizer()
              .setInputCol(prediction)
              .setOutputCol(predict)
            println("threshold : " + i.toDouble / 10)
            binarizerForSegmentDetection.setThreshold(i.toDouble / 10)
            val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(df)
            val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
            counts.show()
        }
        
        val t: (Double, Rates) = ThresholdCalculator.getThreshold(ss, df)
        println(t)
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
        binarizerForSegmentDetection.setThreshold(t._1)
        val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(df)
        val counts: DataFrame = segmentDetectionBinaryResults.groupBy(label, predict).count()
        counts.show()
        t
    }
    
    def getThreshold(ss: SparkSession, df: DataFrame, precision: Int = 60): (Double, Rates) = {
        val centeredThreshold: (Double, Rates) = getCenteredThreshold(ss, df)
        getAdjustedThreshold(ss, df, centeredThreshold._1, precision)
    }
}
