package com.minhdd.cryptos.scryptosbt.predict.ml

import java.sql.Timestamp

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.minhdd.cryptos.scryptosbt.predict.ml.ml._
import com.minhdd.cryptos.scryptosbt.tools.Timestamps

object MLSegmentsGBTRegressor {
    
    val absolute_label = "absolute_label"
    val absolute_predict = "absolute_predict"
    val prediction = "prediction"
    val label = "label"
    val predict = "predict"
    
    def main(args: Array[String]): Unit = {
        val ts1 = Timestamps.now
        println("begin : "  + ts1)
        
        val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction")
        val gbt = new GBTRegressor()
        gbt.setSeed(273).setMaxIter(5)
        val ml = gbt
        val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(5, 10, 20, 50, 100)).build()
        val pipelineForAll = new Pipeline().setStages(Array(indexerBegin, vectorAssembler, ml))
    
        val cvForAll = new CrossValidator()
          .setEstimator(pipelineForAll).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
          .setNumFolds(3).setSeed(27)
        
        val pipelineForTrueSegment = new Pipeline().setStages(Array(indexerBegin, indexerEnd, vectorAssembler, ml))
        
        val cvForTrueSegment = new CrossValidator()
          .setEstimator(pipelineForTrueSegment).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
          .setNumFolds(3).setSeed(27)
        
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
    
        val dfWithoutBeginEvolutionNull: DataFrame =
            ss.read
              .option("sep", ";").schema(csvSchema)
              .csv("D:\\ws\\cryptos\\data\\csv\\segments\\all-190407")
              .filter(!(col("begin-evolution") === "-"))
              
        val dfForTrueSegment = dfWithoutBeginEvolutionNull.filter(!(col("end-evolution") === "-"))
        val modelForTrueSegment = cvForTrueSegment.fit(dfForTrueSegment)      
              
        ///// begin of segment detection 
        
        val dfOfAll = dfWithoutBeginEvolutionNull.withColumn("label",
                    when(col("end-evolution") === "up", 1)
                    .when(col("end-evolution") === "down", 0)
                    .otherwise(-1))
    
        val Array(trainDF, testDF) = dfOfAll.randomSplit(Array(0.7, 0.3), seed=42)
        val modelOFSegmentDetection = cvForAll.fit(trainDF)
        val segmentDetectionResult: DataFrame = 
            modelOFSegmentDetection.transform(testDF)
              .withColumn(absolute_predict, abs(col(prediction)))
              .withColumn(absolute_label, abs(col(label)))
    
        segmentDetectionResult.select("begin-evolution", "end-evolution", "label", "prediction")
          .write.csv("D:\\ws\\cryptos\\data\\mlresults\\all-190407")
        
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
          .setThreshold(0.5)

        val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(segmentDetectionResult)
    
        println("segmentDetectionBinaryResults : ")
        segmentDetectionBinaryResults.show(false)
    
        segmentDetectionBinaryResults.groupBy("label", predict).count().show()
        
//        val detectedSegments = segmentDetectionBinaryResults.filter(col(predict) === 1)
//          .drop(label, predict, prediction, "begin-evo", "features")
//          .filter(!(col("begin-evolution") === "-"))
//        
//        val finalResults: DataFrame = modelForTrueSegment.transform(detectedSegments)
//    
//        println("final results : ")
//        finalResults.show(false)
//        
//        finalResults.select("begin-evolution", "end-evolution", "label", "prediction")
//          .write.csv("D:\\ws\\cryptos\\data\\mlresults\\5.txt")
//        
//        val finalBinarizer = new Binarizer()
//            .setInputCol("prediction")
//            .setOutputCol("predict")
//            .setThreshold(0.5)
//        
//        val finalBinaryResults: DataFrame = finalBinarizer.transform(finalResults)
//        finalBinaryResults.groupBy(label, predict).count().show(false)
    
    
        val ts2: Timestamp = Timestamps.now
        println("fin : " + ts2)
        println(ts2.getTime - ts1.getTime)
    }
}


//        binaryResultDf
//          .filter(col("predict") === 1)
//          .filter(col("label") === 0)
//          .show(100, false)
//
//        binaryResultDf
//          .filter(col("predict") === 0)
//          .filter(col("label") === 1)
//          .show(100, false)
//
//        binaryResultDf
//          .filter(col("predict") === 1)
//          .filter(col("label") === 1)
//          .filter(!(col("begin-evolution") === col("end-evolution")))
//          .show(100, false)
//
//        binaryResultDf
//          .filter(col("predict") === 0)
//          .filter(col("label") === 0)
//          .filter(!(col("begin-evolution") === col("end-evolution")))
//          .show(100, false)
//    
//        println(binaryResultDf.filter(!(col("begin-evolution") === col("end-evolution"))).count())


//        val df1: DataFrame =
//            ss.read
//              .option("sep", ";")
//              .schema(csvSchema)
//              .csv("D:\\ws\\cryptos\\data\\csv\\segments\\trades-190206-before")
//
//        val df2: DataFrame =
//            ss.read
//              .option("sep", ";")
//              .schema(csvSchema)
//              .csv("D:\\ws\\cryptos\\data\\csv\\segments\\trades-190206-from")
//
//        val df = df1.union(df2)
//          .filter(!(col("begin-evolution") === "-"))
//          .filter(!(col("end-evolution") === "-"))

//        val df1: DataFrame =
//            ss.read
//              .option("sep", ";")
//              .schema(csvSchema)
//              .csv("D:\\ws\\cryptos\\data\\csv\\segments\\trades-190120-5")

        