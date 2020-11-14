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
      val error = (prediction - label) / label
      val goodPercentage = prediction / label
      val numberOfElement = x.getAs[Int]("numberOfElement")
      (label, prediction, scala.math.abs(error), goodPercentage, numberOfElement)
    })

    //    println("sort by column 3 : error ")
    //    df1.sort("_3")
    //      .show(100, false)
    //
    //    println("sort by column 4 : good prediction percentage")
    //    df1.sort("_4")
    //      .show(100, false)
    //
    //    println("sort by column 1 : label")
    //    df1.sort("_1")
    //      .show(100, false)

    val maxNumberOfElement: Double = df1.map(_._5).agg(max("value")).map(_.getInt(0)).collect().head
    println(s"max number of element : $maxNumberOfElement")

    for (i <- 1 until 2 + maxNumberOfElement.toInt / 10) {
      val minimumNumberOfElement: Int = (i - 1) * 10
      val maximumNumberOfElement: Int = i * 10

      val errorDf =
        df1
          .filter(x => x._5 > minimumNumberOfElement)
          .filter(x => x._5 <= maximumNumberOfElement)
          .map(x => scala.math.abs(x._3))
      //      println(errorDf.count())
      //      println(errorDf.filter(col("value") < 0.2).count())
      //      errorDf
      val score: Double = errorDf.agg(avg("value")).map(_.getDouble(0)).collect().head
      println(s"${(i - 1) * 10},${i * 10},$score")
    }

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
