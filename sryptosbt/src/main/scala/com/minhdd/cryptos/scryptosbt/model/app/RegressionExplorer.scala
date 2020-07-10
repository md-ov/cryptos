package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.model.service.ml._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.avg

//après regression trainer
object RegressionExplorer {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

//    val df: DataFrame = spark.read.parquet(s"$dataDirectory/ml/size-results/$sizeModelPath")
    val df: DataFrame = spark.read.parquet(s"$dataDirectory/ml/variation-results/$variationModelPath")
//
//    df.drop("features", "begin-evo",
//      "endsecondderive", "beginsecondderive", "endderive", "beginderive",
//      "ohlcBeginVolume", "beginCount", "standardDeviationCount", "averageCount",
//      "standardDeviationSecondDerive", "averageSecondDerive", "standardDeviationDerive",
//      "averageDerive", "standardDeviationVariation", "averageVariation",
//      "averageVolume", "standardDeviationVolume", "endVolume", "endVariation", "beginVolume",
//      "beginVariation")
//      .show(false)


    df.map(x => {
      val label = x.getAs[Double]("label").toDouble
      //      val label = x.getAs[Int]("label").toDouble
      val prediction = x.getAs[Double]("prediction")
      val error = (prediction - label)/label
      (label, prediction, error)
    }).show(1000, false)

    val errorDf = df.map(x => {
      val label = x.getAs[Double]("label").toDouble
//      val label = x.getAs[Int]("label").toDouble
      val prediction = x.getAs[Double]("prediction")
      val error = (prediction - label)/label
      scala.math.abs(error)
    }).agg(avg("value"))
      .show()
  }

  val spark: SparkSession = SparkSession.builder()
    .config("spark.driver.maxResultSize", "3g")
    .config("spark.network.timeout", "600s")
    .config("spark.executor.heartbeatInterval", "60s")
    .appName("big segments")
    .master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


}
