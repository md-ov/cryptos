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
    df.show(5, false)
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


    val ds: Dataset[(Double, Double, Double, Double, Int, Int)] = df.map(x => {
      //      val label = x.getAs[Double]("label").toDouble
      val label = x.getAs[Int]("label").toDouble
      val prediction = x.getAs[Double]("prediction")
      val error = (prediction - label) / label
      val goodPercentage = prediction / label
      val numberOfElement = x.getAs[Int]("numberOfElement")
      val upOrDown: Int = if (x.getAs[Double]("beginvalue") < x.getAs[Double]("endvalue")) 1 else 0
      (label, prediction, scala.math.abs(error), goodPercentage, numberOfElement, upOrDown)
    })

    val dsUp: Dataset[(Double, Double, Double, Double, Int, Int)] = ds.filter(_._6 == 1)
    val dsDown: Dataset[(Double, Double, Double, Double, Int, Int)] = ds.filter(_._6 == 0)
    printScores(dsUp)
    printScores(dsDown)

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

    // best numberOfElement je dirais que c'est environs 340 pas plus

  }

  def printScores(ds: Dataset[(Double, Double, Double, Double, Int, Int)]) = {
    import spark.implicits._
    val maxNumberOfElement: Double = ds.map(_._5).agg(max("value")).map(_.getInt(0)).collect().head
    println(s"max number of element : $maxNumberOfElement")

    for (i <- 1 until 2 + maxNumberOfElement.toInt / 10) {
      val minimumNumberOfElement: Int = (i - 1) * 10
      val maximumNumberOfElement: Int = i * 10

      val errorDf =
        ds
          .filter(x => x._5 > minimumNumberOfElement)
          .filter(x => x._5 <= maximumNumberOfElement)
          .map(x => scala.math.abs(x._3))
      //      println(errorDf.count())
      //      println(errorDf.filter(col("value") < 0.2).count())
      //      errorDf
      val score: Double = errorDf.agg(avg("value")).map(_.getDouble(0)).collect().head
      println(s"${(i - 1) * 10},${i * 10},$score")
    }
  }

  val spark: SparkSession = SparkSession.builder()
    .config("spark.driver.maxResultSize", "3g")
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "60s")
    .appName("big segments")
    .master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
