package com.minhdd.cryptos.scryptosbt.model.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants.{directoryNow, numberOfMinutesBetweenTwoElement}
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.model.service.ml.{label, predict, prediction}
import com.minhdd.cryptos.scryptosbt.segment.app.ActualSegment.getActualSegment
import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Predictor {
    
    val thresholdForPositive = 0.9455041916498401
    val thresholdForNegative = 0.10381390129891797
    val modelPath: String = s"$dataDirectory\\ml\\models\\$numberOfMinutesBetweenTwoElement\\$directoryNow"
    
    def main(args: Array[String]): Unit = {
        main()
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
        println(ds.collect.last.last)
        //        val segments: DataFrame = Expansion.expansion(spark, ds)
        //        segments.filter(col("numberOfElement") === 2).show(10, false)
        val beginDtAndLengths: Array[(Timestamp, Int)] = ds.map(x => (x.head.datetime, x.length)).collect()
        (beginDtAndLengths, ds.toDF())
        //        spark.read.parquet(s"$dataDirectory\\segments\\small\\$numberOfMinutesBetweenTwoElement\\$directoryNow").limit(5)
    }
    
    def main() = {
        val (mapBegindtAndSegmentLength, df): (Array[(Timestamp, Int)], DataFrame) = getDataFrameFromSegments(getActualSegment)
        //        mapBegindtAndSegmentLength.foreach(println)
        println("number of segments: " + df.count())
        
        val model: CrossValidatorModel = ModelHelper.getModel(spark, modelPath)
        val segmentsWithRawPrediction: DataFrame = model.transform(df).cache()
        //        println(segmentsWithRawPrediction.count())
        val predictionOfLastSegment = segmentsWithRawPrediction
          .filter(row => !row.getAs[Boolean]("isSegmentEnd") && {
              val foundElement: Option[(Timestamp, Int)] = mapBegindtAndSegmentLength.find(_._1 == row.getAs[Timestamp]("begindt"))
              foundElement.get._2 == row.getAs[Int]("numberOfElement")
          })
          .select("begindt", "enddt", "isSegmentEnd", "beginEvolution", "endEvolution", "evolutionDirection",
              "beginvalue", "endvalue", "numberOfElement", "label", "prediction")
        predictionOfLastSegment.show(false)
        
        println("number of predicted elements: " + segmentsWithRawPrediction.count())
        val targeted: DataFrame = segmentsWithRawPrediction.filter(_.getAs[Boolean]("isSegmentEnd")).cache()
        val targetedCount: Long = targeted.count()
        println("number of targeted elements: " + targetedCount)
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
        binarizerForSegmentDetection.setThreshold(thresholdForPositive)
        val binarizedResultsForPositive: DataFrame = binarizerForSegmentDetection.transform(targeted)
        val positive: DataFrame = binarizedResultsForPositive.filter(col(predict) === 1.0)
        val positiveCount = positive.count()
        val okPositive: DataFrame = positive.filter(col(predict) === col(label))
        println("positive rate: " + okPositive.count() + "/" + positiveCount + " - " + 
          (positiveCount.toDouble/targetedCount))
    
        binarizerForSegmentDetection.setThreshold(thresholdForNegative)
        val binarizedResultsForNegative: DataFrame = binarizerForSegmentDetection.transform(targeted)
        val negative: DataFrame = binarizedResultsForNegative.filter(col(predict) === 0.0)
        val negativeCount = negative.count()
        val okNegative: DataFrame = negative.filter(col(predict) === col(label))
        println("negative rate: " + okNegative.count() + "/" + negativeCount + " - " + 
          (negativeCount.toDouble/targetedCount))
    }
}
