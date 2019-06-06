package com.minhdd.cryptos.scryptosbt.predict.ml2

import com.minhdd.cryptos.scryptosbt.predict.ml2.ml2.{label, predict, prediction}
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
    
    private def getRates(s: Seq[Double], df: DataFrame) = {s.map(t=>{
            val counts: DataFrame = getCountsDf(df, t)
            val truePositive: Long = extractThridValueWithTwoFilter(counts, 1, 1.0)
            val falsePositive: Long = extractThridValueWithTwoFilter(counts, 0, 1.0)
            val falseNegative: Long = extractThridValueWithTwoFilter(counts, 1, 0.0)
            val trueNegative: Long = extractThridValueWithTwoFilter(counts, 0, 0.0)
            val total = truePositive + falsePositive + falseNegative + trueNegative
            val rate1 = truePositive.toDouble / (truePositive + falsePositive)
            val rate2 = trueNegative.toDouble / (falseNegative + trueNegative)
            val rate3 = (truePositive + falsePositive).toDouble/total
            val rate4 = (truePositive + trueNegative).toDouble/total
            (t, Rates(rate1, rate2, rate3, rate4))
        })
    }
    
    private def bestRate(minimumTruePositiveRate: Double, rates: Seq[(Double, Rates)]): (Double, Rates) = {
        rates
          .filter(_._2.truePositive > minimumTruePositiveRate)
          .maxBy(_._2.trueRate)
    }
    
    private def getCenteredThreshold(ss: SparkSession, df: DataFrame, minimumTruePositiveRate: Double = 0.82): (Double, Rates) = {
        import ss.implicits._
        import org.apache.spark.sql.functions._
        val minAndMaxDataFrame: DataFrame = df.agg(min(prediction), max(prediction))
        val minValue: Double = minAndMaxDataFrame.map(_.getDouble(0)).first()
        val maxValue: Double = minAndMaxDataFrame.map(_.getDouble(1)).first()
        val diff: Double = maxValue - minValue
        val samplingThresholds: Seq[Int] = 0 until 40
        val thresholds: Seq[Double] =
            samplingThresholds.map(s => minValue + (s * (diff/(samplingThresholds.length - 1))))
        
        val rates = getRates(thresholds, df)
        
        bestRate(minimumTruePositiveRate, rates)
    }
    
    def getAdjustedThreshold(ss: SparkSession, df: DataFrame, centeredThreshold: Double,
                                     minimumTruePositiveRate: Double, precision: Int = 60): (Double, Rates) = {
        val epsilon = 0.0005
        val thresholds = (-precision until precision).map(e => centeredThreshold + e*epsilon)
        val rates = getRates(thresholds, df)
        rates.foreach(println)
        println("----------")
        bestRate(minimumTruePositiveRate, rates)
    }
    
    def getThreshold(ss: SparkSession, df: DataFrame, minimumTruePositiveRate: Double = 0.82, precision: Int = 60): (Double, Rates) = {
        val centeredThreshold = getCenteredThreshold(ss, df)
        getAdjustedThreshold(ss, df, centeredThreshold._1, minimumTruePositiveRate, precision)
    }
}
