package com.minhdd.cryptos.scryptosbt.model.app

import java.sql.Timestamp
import java.util.Date

import com.minhdd.cryptos.scryptosbt.constants.{directoryNow, numberOfMinutesBetweenTwoElement}
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.model.service.ml.{label, predict, prediction}
import com.minhdd.cryptos.scryptosbt.segment.app.ActualSegment.getActualSegments
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, ModelHelper, SparkHelper}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
    
    def main() = {
        import spark.implicits._
        val actualSegments: Seq[Seq[BeforeSplit]] = getActualSegments
//        SparkHelper.csvFromSeqBeforeSplit(spark, "D:\\tmp\\actualsegments-20200115.csv", actualSegments.flatten)
        
        val ds: Dataset[Seq[BeforeSplit]] = spark.createDataset(actualSegments).cache()
        val lastSegment = ds.collect.last
        val df = ds.toDF()
        println("number of segments: " + actualSegments.size)
        val mapBegindtAndSegmentLength: Array[(Timestamp, Int)] = ds.map(x => (x.head.datetime, x.length)).collect()
        //        val segments: DataFrame = Expansion.expansion(spark, ds)
        //        segments.filter(col("numberOfElement") === 2).show(10, false)
        
        
        val (segmentsWithRawPrediction: DataFrame, predictionOfLastSegment: DataFrame) = predictMethod(df, mapBegindtAndSegmentLength)
        val p = predictionOfLastSegment.map(_.getAs[Double]("prediction")).head()
        
        predictionOfLastSegment.show(false)
        
        println("number of predicted elements: " + segmentsWithRawPrediction.count())
        val targeted: DataFrame = segmentsWithRawPrediction.filter(_.getAs[Boolean]("isSegmentEnd")).cache()
        
        val (targetedCount: Long,
        positiveCount: Long,
        okPositive: DataFrame,
        negativeCount: Long,
        okNegative: DataFrame) = stats(targeted)
        
        printPredictionHistory(p, lastSegment)
        printHistory(actualSegments, lastSegment, targetedCount, positiveCount, okPositive, negativeCount, okNegative)
    }
    
    private def stats(targeted: DataFrame): (Long, Long, DataFrame, Long, DataFrame) = {
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
          (positiveCount.toDouble / targetedCount))
        
        binarizerForSegmentDetection.setThreshold(thresholdForNegative)
        val binarizedResultsForNegative: DataFrame = binarizerForSegmentDetection.transform(targeted)
        val negative: DataFrame = binarizedResultsForNegative.filter(col(predict) === 0.0)
        val negativeCount = negative.count()
        val okNegative: DataFrame = negative.filter(col(predict) === col(label))
        println("negative rate: " + okNegative.count() + "/" + negativeCount + " - " +
          (negativeCount.toDouble / targetedCount))
        
        (targetedCount, positiveCount, okPositive, negativeCount, okNegative)
    }
    
    private def predictMethod(df: DataFrame, mapBegindtAndSegmentLength: Array[(Timestamp, Int)] ) = {
        val model: CrossValidatorModel = ModelHelper.getModel(spark, modelPath)
        val segmentsWithRawPrediction: DataFrame = model.transform(df).cache()
        val predictionOfLastSegment: DataFrame = segmentsWithRawPrediction
//          .filter(row => !row.getAs[Boolean]("isSegmentEnd"))
            .filter(row => {
              val foundElement: Option[(Timestamp, Int)] = mapBegindtAndSegmentLength.find(_._1 == row.getAs[Timestamp]("begindt"))
              foundElement.get._2 == row.getAs[Int]("numberOfElement")
          })
          .select("begindt", "enddt", "isSegmentEnd", "beginEvolution", "endEvolution", "evolutionDirection",
              "beginvalue", "endvalue", "numberOfElement", "label", "prediction")
        (segmentsWithRawPrediction, predictionOfLastSegment)
    }
    
    private def printHistory(actualSegments: Seq[Seq[BeforeSplit]], lastSegment: Seq[BeforeSplit], targetedCount: Long, positiveCount: Long, okPositive: DataFrame, negativeCount: Long, okNegative: DataFrame) = {
        print(DateTimeHelper.defaultDateFormat.format(new Date()))
        print(";")
        print(s"$numberOfMinutesBetweenTwoElement/$directoryNow")
        print(";")
        print(targetedCount)
        print(";")
        print(okPositive.count() + "/" + positiveCount)
        print(";")
        print(okNegative.count() + "/" + negativeCount)
        print(";")
        print(thresholdForPositive)
        print(";")
        print(thresholdForNegative)
        print(";")
        print(actualSegments.head.head.datetime)
        print(";")
        print(lastSegment.last.datetime)
        print(";")
        print(actualSegments.size)
    }
    
    private def printPredictionHistory(p: Double, lastSegment: Seq[BeforeSplit]) = {
        print(DateTimeHelper.defaultDateFormat.format(new Date()))
        print(";")
        print(s"$numberOfMinutesBetweenTwoElement/$directoryNow")
        print(";")
        print(lastSegment.last.value)
        print(";")
        print(lastSegment.last.datetime)
        print(";")
        print(lastSegment.head.datetime)
        print(";")
        print(lastSegment.head.value)
        print(";")
        print(lastSegment.size)
        print(";")
        print(p)
        print(";")
        if (p >= thresholdForPositive) {
            print(1)
        } else if (p <= thresholdForNegative) {
            print(0)
        } else {
            print(-1)
        }
        println(";;;")
    }
}
