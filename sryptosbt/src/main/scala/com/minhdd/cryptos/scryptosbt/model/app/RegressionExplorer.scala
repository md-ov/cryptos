package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.model.service.ml._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{avg, col, max}

//après regression trainer
object RegressionExplorer {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val df: DataFrame = spark.read.parquet(s"$dataDirectory/ml/size-results/15/20201108144658")
//    val df: DataFrame = spark.read.parquet(s"$dataDirectory/ml/variation-results/15/20201108152242")
//
//    df.drop("features", "begin-evo",
//      "endsecondderive", "beginsecondderive", "endderive", "beginderive",
//      "ohlcBeginVolume", "beginCount", "standardDeviationCount", "averageCount",
//      "standardDeviationSecondDerive", "averageSecondDerive", "standardDeviationDerive",
//      "averageDerive", "standardDeviationVariation", "averageVariation",
//      "averageVolume", "standardDeviationVolume", "endVolume", "endVariation", "beginVolume",
//      "beginVariation")
//      .show(false)


    val df1: Dataset[(Double, Double, Double, Double, Int)] = df.map(x => {
//      val label = x.getAs[Double]("label").toDouble
      val label = x.getAs[Int]("label").toDouble
      val prediction = x.getAs[Double]("prediction")
      val error = (prediction - label)/label
      val goodPercentage = prediction/label
      val numberOfElement = x.getAs[Int]("numberOfElement")
      (label, prediction, scala.math.abs(error), goodPercentage, numberOfElement)
    })

    println("sort by column 3 : error ")
    df1.sort("_3")
      .show(100, false)

    println("sort by column 4 : good prediction percentage")
    df1.sort("_4")
      .show(100, false)

    println("sort by column 1 : label")
    df1.sort("_1")
      .show(100, false)

    println("number of element")
    df1.map(_._5).agg(max("value")).show()

//    for (i <- 1 until 50) {
//      val minimumNumberOfElement: Int = i * 10
//      println(s"minimumNumberOfElement : $minimumNumberOfElement")
//      val errorDf = df1.filter(x => x._5 > minimumNumberOfElement).map(x => scala.math.abs(x._3))
//      println(errorDf.count())
//      println(errorDf.filter(col("value") < 0.2).count())
//      errorDf.agg(avg("value")).show()
//    }

    // best numberOfElement je dirais que c'est environs 340 pas plus



  }

  val spark: SparkSession = SparkSession.builder()
    .config("spark.driver.maxResultSize", "3g")
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "60s")
    .appName("big segments")
    .master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


}
