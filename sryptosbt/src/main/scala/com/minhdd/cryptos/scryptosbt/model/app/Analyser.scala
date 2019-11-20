package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.model.service.ThresholdCalculator
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
//        df.select("numberOfElement", "label", "prediction").show(5, false)
//        val (t, rates) = ThresholdCalculator.exploreDfAndFindThreshold(spark, df)
//        val (t, rates) = ThresholdCalculator.getRates(df, 0.726536009649354)
//        val (t, rates) = ThresholdCalculator.getRates(df, 1.0095808099039112)
//        val (t, rates) = ThresholdCalculator.getRates(df, 0.44899120939479653)
//        println(t)
//        println(rates)
    }
}
