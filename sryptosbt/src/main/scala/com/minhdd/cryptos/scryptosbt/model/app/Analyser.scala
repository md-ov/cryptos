package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.model.service.{ThresholdCalculator, ml}
import org.apache.spark.sql.{DataFrame, SparkSession}


//après les trainers
object Analyser {
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
//    val df: DataFrame = spark.read.parquet(s"$dataDirectory/ml/linear-results/${ml.linearPath}")
    val df: DataFrame = spark.read.parquet(s"$dataDirectory/ml/results/${ml.upDownPath}")

    def main(args: Array[String]): Unit = {
        println(df.count())
        df.groupBy("label").count().show()
        df.select("numberOfElement", "label", "prediction").show(10, false)
        val ((t1, ratesForPositive), (t2, ratesForNegative)) = ThresholdCalculator.exploreDfAndFindThreshold(spark, df)
//        val (t, rates) = ThresholdCalculator.getRates(df, 0.726536009649354)
//        val (t, rates) = ThresholdCalculator.getRates(df, 1.0095808099039112)
//        val (t, rates) = ThresholdCalculator.getRates(df, 0.44899120939479653)
        println(t1)
        println(ratesForPositive)
        println(t2)
        println(ratesForNegative)
    }
}
