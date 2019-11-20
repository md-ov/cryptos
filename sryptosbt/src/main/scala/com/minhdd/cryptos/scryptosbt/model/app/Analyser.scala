package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.model.service.ThresholdCalculator
import com.minhdd.cryptos.scryptosbt.model.service.ml.{label, predict, prediction}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{DataFrame, SparkSession}

object Analyser {
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    val df: DataFrame = spark.read.parquet(s"$dataDirectory\\ml\\results\\$numberOfMinutesBetweenTwoElement\\$directoryNow")
    
    
    def main(args: Array[String]): Unit = {
//        val (t, rates) = ThresholdCalculator.exploreDfAndFindThreshold(spark, df)
        val (t, rates) = ThresholdCalculator.getRates(df, 0.44899120939479653D)
        println(t)
        println(rates)
    }
    
    def seeResults() = {
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
        
        for (i <- 0 to 10) {
            println("threshold : " + i.toDouble / 10);
            binarizerForSegmentDetection.setThreshold(i.toDouble / 10)
            val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(df)
            val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
            counts.show()
        }
    }
}
