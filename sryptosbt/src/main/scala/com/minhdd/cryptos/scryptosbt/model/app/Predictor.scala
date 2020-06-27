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

//3
//change threshold and run
object Predictor {
    val thresholdForPositive = 0.6609068691287857
    val thresholdForNegative = 0.06683752680961988
    val modelPath: String = s"$dataDirectory/ml/models/${ml.upDownPath}"

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
        println("last segment: " + lastSegment.head.datetime + " -> " + lastSegment.last.datetime + " : " + lastSegment.last.value)
        val mapBegindtAndSegmentLength: Array[(Timestamp, Int)] = ds.map(x => (x.head.datetime, x.length)).collect()
        //        val segments: DataFrame = Expansion.expansion(spark, ds)
        //        segments.filter(col("numberOfElement") === 2).show(10, false)
        
        
        val
        (segmentsWithRawPrediction: DataFrame,
        predictionOfLastSegment: DataFrame) = predictMethod(df, mapBegindtAndSegmentLength)

        println("raw predictions : ")
        segmentsWithRawPrediction
          .filter(row => {
              val foundElement: Option[(Timestamp, Int)] = mapBegindtAndSegmentLength.find(_._1 == row.getAs[Timestamp]("begindt"))
              foundElement.get._2 == row.getAs[Int]("numberOfElement")
          }).select("begindt", "enddt", "isSegmentEnd", "beginEvolution", "endEvolution", "evolutionDirection",
            "beginvalue", "endvalue", "numberOfElement", "label", "linear", "prediction").show()
        println("prediction for last segment : ")
        predictionOfLastSegment.show()

        val p = predictionOfLastSegment.map(_.getAs[Double]("prediction")).head()

        println("number of predicted elements: " + segmentsWithRawPrediction.count())
        val targeted: DataFrame = segmentsWithRawPrediction.filter(_.getAs[Boolean]("isSegmentEnd")).cache()

        val (targetedCount: Long,
        positiveCount: Long,
        okPositive: DataFrame,
        negativeCount: Long,
        okNegative: DataFrame) = stats(targeted)

        println("prediction")
        printPrediction(p, lastSegment)

        println;println("stats")
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
        val segmentsWithRawPrediction: DataFrame = model.transform(df).cache()
        val predictionOfLastSegment: DataFrame = segmentsWithRawPrediction
          .filter(row => !row.getAs[Boolean]("isSegmentEnd")) //afficher la prédiction du dernier segment avec isSegmentEnd == false
          .filter(row => {
              val foundElement: Option[(Timestamp, Int)] = mapBegindtAndSegmentLength.find(_._1 == row.getAs[Timestamp]("begindt"))
              foundElement.get._2 == row.getAs[Int]("numberOfElement")
          })
          .select("begindt", "enddt", "isSegmentEnd", "beginEvolution", "endEvolution", "evolutionDirection",
              "beginvalue", "endvalue", "numberOfElement", "label", "prediction")

        (segmentsWithRawPrediction, predictionOfLastSegment)
    }

    val delimiter = ","
    
    private def printHistory(actualSegments: Seq[Seq[BeforeSplit]], lastSegment: Seq[BeforeSplit], targetedCount: Long, positiveCount: Long, okPositive: DataFrame, negativeCount: Long, okNegative: DataFrame) = {
        print(DateTimeHelper.defaultDateFormat.format(new Date()))
        print(delimiter)
        print(s"${ml.upDownPath}")
        print(delimiter)
        print(targetedCount)
        print(delimiter)
        print(okPositive.count() + "/" + positiveCount)
        print(delimiter)
        print(okNegative.count() + "/" + negativeCount)
        print(delimiter)
        print(thresholdForPositive)
        print(delimiter)
        print(thresholdForNegative)
        print(delimiter)
        print(actualSegments.head.head.datetime.toString.substring(0, 19))
        print(delimiter)
        print(lastSegment.last.datetime.toString.substring(0, 19))
        print(delimiter)
        print(actualSegments.size)
    }
    
    private def printPrediction(p: Double, lastSegment: Seq[BeforeSplit]) = {
        print(lastSegment.head.datetime.toString.substring(0, 19))
        print(delimiter)
        print(DateTimeHelper.defaultDateFormat.format(new Date()))
        print(delimiter)
        print(lastSegment.head.value)
        print(delimiter)
        print(lastSegment.last.value)
        print(delimiter)
        print(lastSegment.last.datetime.toString.substring(0, 19))
        print(delimiter)
        print(lastSegment.size)
        print(delimiter)
        print(p)
        print(delimiter)
        if (p >= thresholdForPositive) {
            print(1)
        } else if (p <= thresholdForNegative) {
            print(0)
        } else {
            print(-1)
        }
        print(delimiter)
        print("1")
        print(delimiter)
        print("1")
        print(s"$delimiter$delimiter$delimiter$delimiter$delimiter$delimiter$delimiter")
        println(s"${ml.upDownPath}")
    }
}
