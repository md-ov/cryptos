package com.minhdd.cryptos.scryptosbt.model.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.model.app.Predictor.{linearModelPath, modelPath}
import com.minhdd.cryptos.scryptosbt.segment.app.ActualSegment.getActualSegments
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper.DateTimeImplicit
import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object ResultTaker {
    def main(args: Array[String]): Unit = {
        main("2020-05-21 20:45")
    }
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    def main(beginDtString: String): Unit = {
        val dtf: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
        val beginDt: Timestamp = dtf.parseDateTime(beginDtString).toTimestamp
        val actualSegments: Seq[Seq[BeforeSplit]] = getActualSegments(beginDt: Timestamp)

        val lastBeforeSplit: BeforeSplit = actualSegments.last.last
        println("last element : ")
        println(s"- value : ${lastBeforeSplit.value}")
        println(s"- dt : ${lastBeforeSplit.datetime}")

        val ds: Dataset[Seq[BeforeSplit]] = spark.createDataset(actualSegments)
        val df = ds.toDF()
        val mapBegindtAndSegmentLength: Array[(Timestamp, Int)] = ds.map(x => (x.head.datetime, x.length)).collect()
        val
        (segmentsWithRawPrediction: DataFrame,
        predictionOfLastSegment: DataFrame,
        predictionLinearOfLastSegment: DataFrame) = predictMethod(df, mapBegindtAndSegmentLength)

        segmentsWithRawPrediction.show()
        predictionOfLastSegment.show()
        predictionLinearOfLastSegment.show()
    }
    
    private def predictMethod(df: DataFrame, mapBegindtAndSegmentLength: Array[(Timestamp, Int)]): (DataFrame, DataFrame, DataFrame) = {
        val model: CrossValidatorModel = ModelHelper.getModel(spark, modelPath)
        val linearModel: CrossValidatorModel = ModelHelper.getModel(spark, linearModelPath)
        val segmentsWithRawPrediction: DataFrame = model.transform(df).cache()
        val segmentsWithRawPredictionForLinear: DataFrame = linearModel.transform(df).cache()
        val predictionOfLastSegment: DataFrame = segmentsWithRawPrediction
          .filter(row => {
              val foundElement: Option[(Timestamp, Int)] = mapBegindtAndSegmentLength.find(_._1 == row.getAs[Timestamp]("begindt"))
              foundElement.get._2 == row.getAs[Int]("numberOfElement")
          })
          .select("begindt", "enddt", "isSegmentEnd", "beginEvolution", "endEvolution", "evolutionDirection",
              "beginvalue", "endvalue", "numberOfElement", "label", "prediction")
        val predictionLinearOfLastSegment: DataFrame = segmentsWithRawPredictionForLinear
          .filter(row => {
              val foundElement: Option[(Timestamp, Int)] = mapBegindtAndSegmentLength.find(_._1 == row.getAs[Timestamp]("begindt"))
              foundElement.get._2 == row.getAs[Int]("numberOfElement")
          })
          .select("begindt", "enddt", "isSegmentEnd", "linear", "beginEvolution", "endEvolution", "evolutionDirection",
              "beginvalue", "endvalue", "numberOfElement", "label", "prediction")
        (segmentsWithRawPrediction, predictionOfLastSegment, predictionLinearOfLastSegment)
    }
}
