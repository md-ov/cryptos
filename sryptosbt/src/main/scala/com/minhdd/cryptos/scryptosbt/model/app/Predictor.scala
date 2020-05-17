package com.minhdd.cryptos.scryptosbt.model.app

import java.sql.Timestamp
import java.util.Date

import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.model.service.ml
import com.minhdd.cryptos.scryptosbt.model.service.ml.{label, predict, prediction}
import com.minhdd.cryptos.scryptosbt.segment.app.ActualSegment.getActualSegments
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, ModelHelper}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Predictor {
    val thresholdForPositiveLinear = 0.31886961827175364
    val thresholdForPositive = 0.5
    val thresholdForNegative = 0.5
    val modelPath: String = s"$dataDirectory/ml/models/${ml.upDownPath}"
    val linearModelPath = s"$dataDirectory/ml/linear-models/${ml.linearPath}"

    def main(args: Array[String]): Unit = {
        main()
    }
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("predict")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    def main() = {
        import spark.implicits._
        val actualSegments: Seq[Seq[BeforeSplit]] = getActualSegments
        val ds: Dataset[Seq[BeforeSplit]] = spark.createDataset(actualSegments).cache()
        val lastSegment: Seq[BeforeSplit] = ds.collect.last
        val df = ds.toDF()
        println("number of actualSegments: " + actualSegments.size)
        println("last segment: " + lastSegment.head.datetime + " -> " + lastSegment.last.datetime)
        val mapBegindtAndSegmentLength: Array[(Timestamp, Int)] = ds.map(x => (x.head.datetime, x.length)).collect()
        //        val segments: DataFrame = Expansion.expansion(spark, ds)
        //        segments.filter(col("numberOfElement") === 2).show(10, false)
        
        
        val
        (segmentsWithRawPrediction: DataFrame,
        predictionOfLastSegment: DataFrame,
        predictionLinearOfLastSegment: DataFrame) = predictMethod(df, mapBegindtAndSegmentLength)

        println("prediction for last segment : ")
        predictionOfLastSegment.show()
        println("prediction lineaire for last segment : ")
        predictionLinearOfLastSegment.show()
        
        val p = predictionOfLastSegment.map(_.getAs[Double]("prediction")).head()
        val linearP = predictionLinearOfLastSegment.map(_.getAs[Double]("prediction")).head()

        println("number of predicted elements: " + segmentsWithRawPrediction.count())
        val targeted: DataFrame = segmentsWithRawPrediction.filter(_.getAs[Boolean]("isSegmentEnd")).cache()

        val (targetedCount: Long,
        positiveCount: Long,
        okPositive: DataFrame,
        negativeCount: Long,
        okNegative: DataFrame) = stats(targeted)

        printPredictionHistory(p, linearP, lastSegment)
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
    
    private def predictMethod(df: DataFrame, mapBegindtAndSegmentLength: Array[(Timestamp, Int)]) = {
        val model: CrossValidatorModel = ModelHelper.getModel(spark, modelPath)
        val linearModel: CrossValidatorModel = ModelHelper.getModel(spark, linearModelPath)
        val segmentsWithRawPrediction: DataFrame = model.transform(df).cache()
        val segmentsWithRawPredictionForLinear: DataFrame = linearModel.transform(df).cache()
        val predictionOfLastSegment: DataFrame = segmentsWithRawPrediction
          .filter(row => !row.getAs[Boolean]("isSegmentEnd")) //afficher la prédiction du dernier segment avec isSegmentEnd == false
          .filter(row => {
              val foundElement: Option[(Timestamp, Int)] = mapBegindtAndSegmentLength.find(_._1 == row.getAs[Timestamp]("begindt"))
              foundElement.get._2 == row.getAs[Int]("numberOfElement")
          })
          .select("begindt", "enddt", "isSegmentEnd", "beginEvolution", "endEvolution", "evolutionDirection",
              "beginvalue", "endvalue", "numberOfElement", "label", "prediction")
        val predictionLinearOfLastSegment: DataFrame = segmentsWithRawPredictionForLinear
          .filter(row => !row.getAs[Boolean]("isSegmentEnd"))
          .filter(row => {
              val foundElement: Option[(Timestamp, Int)] = mapBegindtAndSegmentLength.find(_._1 == row.getAs[Timestamp]("begindt"))
              foundElement.get._2 == row.getAs[Int]("numberOfElement")
          })
          .select("begindt", "enddt", "isSegmentEnd", "linear", "beginEvolution", "endEvolution", "evolutionDirection",
              "beginvalue", "endvalue", "numberOfElement", "label", "prediction")
        (segmentsWithRawPrediction, predictionOfLastSegment, predictionLinearOfLastSegment)
    }
    
    private def printHistory(actualSegments: Seq[Seq[BeforeSplit]], lastSegment: Seq[BeforeSplit], targetedCount: Long, positiveCount: Long, okPositive: DataFrame, negativeCount: Long, okNegative: DataFrame) = {
        print(DateTimeHelper.defaultDateFormat.format(new Date()))
        print(";")
        print(s"${ml.upDownPath},${ml.linearPath}")
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
        print(actualSegments.head.head.datetime.toString.substring(0, 19))
        print(";")
        print(lastSegment.last.datetime.toString.substring(0, 19))
        print(";")
        print(actualSegments.size)
    }
    
    private def printPredictionHistory(p: Double, linearP: Double, lastSegment: Seq[BeforeSplit]) = {
        print(DateTimeHelper.defaultDateFormat.format(new Date()))
        print(";")
        print(s"${ml.upDownPath},${ml.linearPath}")
        print(";")
        print(lastSegment.last.value)
        print(";")
        print(lastSegment.last.datetime.toString.substring(0, 19))
        print(";")
        print(lastSegment.head.datetime.toString.substring(0, 19))
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
        print(";")
        print(linearP)
        print(";")
        if (linearP >= thresholdForPositiveLinear) {
            print(1)
        } else {
            print(-1)
        }
        println(";;;;;;;;")
    }
}
