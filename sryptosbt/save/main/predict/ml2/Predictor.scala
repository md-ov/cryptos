package com.minhdd.cryptos.scryptosbt.predict.ml2

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.Predict
import com.minhdd.cryptos.scryptosbt.exploration.BeforeSplit
import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.predict.ml2.ml2.{label, predict, prediction}
import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Predictor {
    val segmentDirectory = "all-190814-from-brut"
    val modelDirectory = "all-190612-fusion"
    val threshold = 0.895
    
    def predictMain(args: Predict): String = {
        getActualSegmentAndPredict
        "status|SUCCESS, result|366"
    }
    
    def predictSomeSegment(): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df: DataFrame =
            ss.read.parquet(s"$dataDirectory\\segments\\$segmentDirectory\\$beforesplits")
        val someSegments: DataFrame = df.limit(3)
        Predictor.predictTheSegment(ss, s"$dataDirectory\\models\\models\\$modelDirectory", someSegments)
    }
    
    def getActualSegmentAndPredict() = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        import ss.implicits._
        val df: Dataset[Seq[BeforeSplit]] =
            ss.read.parquet(s"$dataDirectory\\segments\\$segmentDirectory\\$beforesplits").as[Seq[BeforeSplit]]
        
        val s: Array[Seq[BeforeSplit]] = df.collect().sortWith({case (a, b) => a.last.datetime.getTime < b.last.datetime.getTime})
        val headDateTimeAndLastDateTimeSeq: Array[(Timestamp, Timestamp, Option[Boolean])] = 
            s.map(e => (e.head.datetime, e.last.datetime, e.last.importantChange))
        headDateTimeAndLastDateTimeSeq.foreach(f => println("" + f._1 + " -> " + f._2 + " - " + f._3))
        val bugs = headDateTimeAndLastDateTimeSeq.indices.filter(i =>
            (i != headDateTimeAndLastDateTimeSeq.length - 1) &&
              (headDateTimeAndLastDateTimeSeq.apply(i)._2 != headDateTimeAndLastDateTimeSeq.apply(i+1)._1)
        )
        if (bugs.nonEmpty) {
            println("---- les anomalies : ")
            bugs.foreach(f => println("" + headDateTimeAndLastDateTimeSeq.apply(f)._1 + " -> " + headDateTimeAndLastDateTimeSeq.apply(f)._2))
        } else {
            println("-- there is no anomalie !!!! --")
        }
        println("----")
        println("Actual segment : ")
        val actualSegment = s.last
        actualSegment.map(e => {
            val evolution = e.evolution
            val importantChange = e.importantChange
            (e.datetime, evolution, importantChange)
        }).foreach(println)
        predictOneSegment(ss, s"$dataDirectory\\models\\models\\$modelDirectory", actualSegment)
    }
    
    def getDfFromOneSegment(ss: SparkSession, segment: Seq[BeforeSplit]): DataFrame = {
        import ss.implicits._
        val ds: Dataset[Seq[BeforeSplit]] = ss.createDataset(Seq(segment))
        ds.toDF()
    }
    
    def predictTheSegment(ss: SparkSession, modelPath: String, segments: DataFrame): String = {
        import ml2.{prediction, predict}
        val model: CrossValidatorModel = ModelHelper.getModel(ss, modelPath)
        val segmentsWithRawPrediction = model.transform(segments)
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
        binarizerForSegmentDetection.setThreshold(1.02)
        val dfWithFinalPrediction = binarizerForSegmentDetection.transform(segmentsWithRawPrediction)
        dfWithFinalPrediction.show(1000, false)
        import org.apache.spark.sql.functions.lit
        import org.apache.spark.sql.functions.{col, max}
        val maxNumberOfElement: Int = dfWithFinalPrediction.agg(max("numberOfElement")).first().getInt(0)
        val longestSegment = dfWithFinalPrediction.filter(col("numberOfElement") === maxNumberOfElement).first()
        val binaryPrediction: Double = longestSegment.getAs[Double](predict)
        val predictionValue: Double = longestSegment.getAs[Double](prediction)
        val numberOfElement: Int = longestSegment.getAs[Integer]("numberOfElement")
        val beginValue: Double = longestSegment.getAs[Double]("beginvalue")
        val endvalue: Double = longestSegment.getAs[Double]("endvalue")
        val beginDt: Timestamp = longestSegment.getAs[Timestamp]("begindt")
        dfWithFinalPrediction
          .withColumn("modelPath", lit(modelDirectory))
          .write.parquet(s"$dataDirectory\\models\\predictions\\$modelDirectory\\${beginDt.getTime}")
        println("prediction : " + binaryPrediction)
        Seq(modelDirectory, beginDt, beginValue, endvalue, endvalue,numberOfElement, "", predictionValue, binaryPrediction).mkString(";")
    }
    
    def predictOneSegment(ss: SparkSession, modelPath: String, segment: Seq[BeforeSplit]): String = {
        predictTheSegment(ss, modelPath, getDfFromOneSegment(ss, segment))
        //        val binarizerForSegmentDetection = new Binarizer()
        //          .setInputCol(prediction)
        //          .setOutputCol(predict)
        //        binarizerForSegmentDetection.setThreshold(1.02)
        //        val result = binarizerForSegmentDetection.transform(p)
        //        result.show(false)
        //        import org.apache.spark.sql.functions._
        //        val maxNumberOfElement: Int = result.agg(max("numberOfElement")).first().getInt(0)
        //        val aa: Double = result.filter(col("numberOfElement") === maxNumberOfElement).first().getAs[Double](predict)
        //        println("prediction :" + aa)
    }
    
    def predictSegments(segmentsPath: String): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val segmentsWithRawPrediction: DataFrame = transformSegmentsWithModel(segmentsPath, ss)
    
        import org.apache.spark.sql.functions.{abs, col, sum}
        val totalError = segmentsWithRawPrediction.withColumn("error", abs(col(prediction) - col(label))).agg(sum("error")).first().getDouble(0)
        println("total error : " + totalError)
//        segmentsWithRawPrediction.show(10, false)
        val t = ThresholdCalculator.getAdjustedThreshold(ss, segmentsWithRawPrediction, threshold, 0.9, 10)
//        println(t)
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
//        binarizerForSegmentDetection.setThreshold(t._1)
        binarizerForSegmentDetection.setThreshold(t._1)
        val dfWithFinalPrediction = binarizerForSegmentDetection.transform(segmentsWithRawPrediction)
        val counts = dfWithFinalPrediction.groupBy(label, predict).count()
        counts.show()
        val all = counts.agg(sum("count")).first().getLong(0)
        println(Seq(modelDirectory, segmentsPath, totalError, t._1, t._2.truePositive, t._2.trueRate, all).mkString(";"))
    }
    
    private def transformSegmentsWithModel(segmentsPath: String, ss: SparkSession) = {
        val segments: DataFrame =
            ss.read.parquet(s"$dataDirectory\\segments\\$segmentsPath\\$beforesplits")
        val model: CrossValidatorModel = ModelHelper.getModel(ss, s"$dataDirectory\\models\\models\\$modelDirectory")
        val segmentsWithRawPrediction: DataFrame = model.transform(segments)
        segmentsWithRawPrediction
    }
    
    def seedf() = {
        val beginTimestamp = "1562131800000"
        val ss: SparkSession = 
            SparkSession.builder()
          .appName("read actual segment prediction")
          .master("local[*]")
          .getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        ss.read.parquet(s"$dataDirectory\\models\\predictions\\$modelDirectory\\$beginTimestamp").show(100, false)
    }
    
    def findsegment(segmentsPath: String, ts: Int): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        
        val df: DataFrame = transformSegmentsWithModel(segmentsPath, ss)
        import org.apache.spark.sql.functions.{col, unix_timestamp}
//        df.withColumn("ss", unix_timestamp(col("begindt"))*1000)
//          .select("begindt", "ss")
//          .dropDuplicates("ss")
//          .orderBy("ss")
//          .show(100000,false)
//        df.filter(unix_timestamp(col("begindt"))*1000 >= 1562131800000L).show(10000, false)
        df.filter(col("endEvolution") === evolutionNone).show(10000, false)
    }
    
    def main(args: Array[String]): Unit = {
//        predictSomeSegment
        println(getActualSegmentAndPredict)
//        seedf()
//        predictSegments("all-190701")
        
//        findsegment("all-190708-fusion", 1562131800)
//        findsegment("all-190708-from-brut", 1562131800)
    }
}
