package com.minhdd.cryptos.scryptosbt.predict.ml2

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.Predict
import com.minhdd.cryptos.scryptosbt.exploration.BeforeSplit
import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.tools.Models
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Predictor {
    val segmentDirectory = "all-190701-fusion"
    val modelDirectory = "all-190612-fusion"
    
    def predictMain(args: Predict): String = {
        getActualSegmentAndPredict
        "status|SUCCESS, result|366"
    }
    
    def predictSomeSegment(): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df: DataFrame =
            ss.read.parquet(s"$dataDirectory\\csv\\segments\\$segmentDirectory\\$BEFORE_SPLITS")
        val someSegments: DataFrame = df.limit(3)
        Predictor.predictTheSegment(ss, s"$dataDirectory\\models\\$modelDirectory", someSegments)
    }
    
    def getActualSegmentAndPredict() = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        import ss.implicits._
        val df: Dataset[Seq[BeforeSplit]] =
            ss.read.parquet(s"$dataDirectory\\csv\\segments\\$segmentDirectory\\$BEFORE_SPLITS").as[Seq[BeforeSplit]]
        
        val s: Array[Seq[BeforeSplit]] = df.collect().sortWith({case (a, b) => a.last.datetime.getTime < b.last.datetime.getTime})
        val headDateTimeAndLastDateTimeSeq: Array[(Timestamp, Timestamp)] = s.map(e => (e.head.datetime, e.last.datetime))
        headDateTimeAndLastDateTimeSeq.foreach(f => println("" + f._1 + " -> " + f._2))
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
        predictOneSegment(ss, s"$dataDirectory\\models\\$modelDirectory", actualSegment)
    }
    
    def getDfFromOneSegment(ss: SparkSession, segment: Seq[BeforeSplit]): DataFrame = {
        import ss.implicits._
        val ds: Dataset[Seq[BeforeSplit]] = ss.createDataset(Seq(segment))
        ds.toDF()
    }
    
    def predictTheSegment(ss: SparkSession, modelPath: String, segments: DataFrame): DataFrame = {
        import ml2.{prediction, predict}
        val model: CrossValidatorModel = Models.getModel(ss, modelPath)
        val segmentsWithRawPrediction = model.transform(segments)
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
        binarizerForSegmentDetection.setThreshold(1.02)
        val dfWithFinalPrediction = binarizerForSegmentDetection.transform(segmentsWithRawPrediction)
        dfWithFinalPrediction.show(1000, false)
        import org.apache.spark.sql.functions.{col, max}
        val maxNumberOfElement: Int = dfWithFinalPrediction.agg(max("numberOfElement")).first().getInt(0)
        val finalPrediction: Double = 
            dfWithFinalPrediction.filter(col("numberOfElement") === maxNumberOfElement).first().getAs[Double](predict)
        println("prediction : " + finalPrediction);
        segmentsWithRawPrediction
    }
    
    def predictOneSegment(ss: SparkSession, modelPath: String, segment: Seq[BeforeSplit]): Unit = {
        val p: DataFrame = predictTheSegment(ss, modelPath, getDfFromOneSegment(ss, segment))
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
    
    def main(args: Array[String]): Unit = {
//        predictSomeSegment
        getActualSegmentAndPredict
    }
}
