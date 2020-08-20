package com.minhdd.cryptos.scryptosbt.model.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.model.app.Predictor.{modelPath}
import com.minhdd.cryptos.scryptosbt.segment.service.ActualSegment.getActualSegments
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper.DateTimeImplicit
import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

//3 après predictor
object ResultTaker {
    def main(args: Array[String]): Unit = {
        main("2020-07-09 17:15:00")
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
        val dtf: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        val beginDt: Timestamp = dtf.parseDateTime(beginDtString).toTimestamp
        val actualSegments: Seq[Seq[BeforeSplit]] = getActualSegments(spark, beginDt: Timestamp)

        println("last element : ")
        println(s"- value : ${actualSegments.last.head.value} -> ${actualSegments.last.last.value}")
        println(s"- dt : ${actualSegments.last.head.datetime} -> ${actualSegments.last.last.datetime}")

        val ds: Dataset[Seq[BeforeSplit]] = spark.createDataset(actualSegments)
        val df = ds.toDF()
        val mapBegindtAndSegmentLength: Array[(Timestamp, Int)] = ds.map(x => (x.head.datetime, x.length)).collect()
        val
        (segmentsWithRawPrediction: DataFrame,
        predictionOfLastSegment: DataFrame) = predictMethod(df, mapBegindtAndSegmentLength)

        segmentsWithRawPrediction.show()
        predictionOfLastSegment.show()
    }
    
    private def predictMethod(df: DataFrame, mapBegindtAndSegmentLength: Array[(Timestamp, Int)]): (DataFrame, DataFrame) = {
        val model: CrossValidatorModel = ModelHelper.getModel(spark, modelPath)
        val segmentsWithRawPrediction: DataFrame = model.transform(df).cache()
        val predictionOfLastSegment: DataFrame = segmentsWithRawPrediction
          .filter(row => {
              val foundElement: Option[(Timestamp, Int)] = mapBegindtAndSegmentLength.find(_._1 == row.getAs[Timestamp]("begindt"))
              foundElement.get._2 == row.getAs[Int]("numberOfElement")
          })
          .select("begindt", "enddt", "isSegmentEnd", "beginEvolution", "endEvolution", "evolutionDirection",
              "beginvalue", "endvalue", "numberOfElement", "label", "prediction")

        (segmentsWithRawPrediction, predictionOfLastSegment)
    }
}
