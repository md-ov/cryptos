package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
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
    
    def main(args: Array[String]): Unit = {
    
        val testDfWithRawPrediction: DataFrame = spark.read.parquet(s"$dataDirectory\\ml\\results\\$numberOfMinutesBetweenTwoElement\\$directoryNow")
        
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
    
        for (i <- 0 to 10) {
            println("threshold : " + i.toDouble/10)
            binarizerForSegmentDetection.setThreshold(i.toDouble/10)
            val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(testDfWithRawPrediction)
            val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
            counts.show()
        }
    }
}
