package com.minhdd.cryptos.scryptosbt.model.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.constants.{directoryNow, numberOfMinutesBetweenTwoElement}
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.segment.app.ActualSegment.getActualSegment
import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.functions.{array_contains, col, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import shapeless.DefaultSymbolicLabelling

object Predictor {
    
    def main(args: Array[String]): Unit = {
        predict()
    }
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    def getDataFrameFromSegments(seq: Seq[Seq[BeforeSplit]]): (Array[(Timestamp, Int)], DataFrame) = {
        import spark.implicits._
        val ds: Dataset[Seq[BeforeSplit]] = spark.createDataset(seq).cache()
        //        val segments: DataFrame = Expansion.expansion(spark, ds)
        //        segments.filter(col("numberOfElement") === 2).show(10, false)
        val beginDtAndLengths: Array[(Timestamp, Int)] = ds.map(x => (x.head.datetime, x.length)).collect()
        (beginDtAndLengths, ds.toDF())
        //        spark.read.parquet(s"$dataDirectory\\segments\\small\\$numberOfMinutesBetweenTwoElement\\$directoryNow").limit(5)
    }
    
    def predict() = {
        val (mapBegindtAndSegmentLength, df): (Array[(Timestamp, Int)], DataFrame) = getDataFrameFromSegments(getActualSegment)
//        mapBegindtAndSegmentLength.foreach(println)
//        println(df.count())
        val modelPath: String = s"$dataDirectory\\ml\\models\\$numberOfMinutesBetweenTwoElement\\$directoryNow"
        val model: CrossValidatorModel = ModelHelper.getModel(spark, modelPath)
        val segmentsWithRawPrediction: DataFrame = model.transform(df)
//        println(segmentsWithRawPrediction.count())
        segmentsWithRawPrediction
          .filter(row => !row.getAs[Boolean]("isSegmentEnd") && {
              val foundElement: Option[(Timestamp, Int)] = mapBegindtAndSegmentLength.find(_._1 == row.getAs[Timestamp]("begindt"))
              foundElement.get._2 == row.getAs[Int]("numberOfElement")
          })
          .select("begindt", "enddt", "isSegmentEnd", "beginEvolution", "endEvolution", "evolutionDirection", 
              "beginvalue", "endvalue", "numberOfElement", "label", "prediction")
          .show(false)
    }
}
