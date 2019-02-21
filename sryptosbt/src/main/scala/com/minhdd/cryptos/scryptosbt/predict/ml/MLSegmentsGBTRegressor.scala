package com.minhdd.cryptos.scryptosbt.predict.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Binarizer, StringIndexer}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.minhdd.cryptos.scryptosbt.predict.ml.ml._

object MLSegmentsGBTRegressor {
    
    val absolute_label = "absolute_label"
    val absolute_predict = "absolute_predict"
    val prediction = "prediction"
    val label = "label"
    val predict = "predict"
    
    def main(args: Array[String]): Unit = {
        val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction")
        
        val gbt = new GBTRegressor()
        gbt.setSeed(273).setMaxIter(5)
        val ml = gbt
        val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(5, 10, 20, 50, 100)).build()
        val pipeline = new Pipeline().setStages(Array(indexerBegin, vectorAssembler, ml))
        val pipelineForTrueSegment = new Pipeline().setStages(Array(indexerBegin, indexerEnd, vectorAssembler, ml))

        val cv = new CrossValidator()
            .setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
            .setNumFolds(3).setSeed(27)
        
        val cvForTrueSegment = new CrossValidator()
          .setEstimator(pipelineForTrueSegment).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
          .setNumFolds(3).setSeed(27)
        
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
    
        val dfWithoutBeginEvolutionNull: DataFrame =
            ss.read
              .option("sep", ";").schema(csvSchema)
              .csv("D:\\ws\\cryptos\\data\\csv\\segments\\all-190221")
              .filter(!(col("begin-evolution") === "-"))
              
        val dfForTrueSegment = dfWithoutBeginEvolutionNull.filter(!(col("end-evolution") === "-"))
        val modelForTrueSegment = cvForTrueSegment.fit(dfForTrueSegment)      
              
        val df = dfWithoutBeginEvolutionNull.withColumn("label",
                    when(col("end-evolution") === "up", 1)
                    .when(col("end-evolution") === "down", -1)
                    .otherwise(0))
    
        val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=42)
        val model = cv.fit(trainDF)
        val resultDF: DataFrame = 
            model.transform(testDF)
              .withColumn(absolute_predict, abs(col(prediction)))
              .withColumn(absolute_label, abs(col(label)))
    
        val binarizer = new Binarizer()
          .setInputCol(absolute_predict)
          .setOutputCol(predict)
          .setThreshold(0.9)
        
        val binaryResultDf = binarizer.transform(resultDF)
        
        val filtered = binaryResultDf.filter(col(predict) === 1)
          .drop(label, predict, prediction, "begin-evo", "features")
          .filter(!(col("begin-evolution") === "-"))
        
        val finalResultDF = modelForTrueSegment.transform(filtered)
        finalResultDF.show(false)
    
        val finalBinarizer = new Binarizer()
            .setInputCol("prediction")
            .setOutputCol("predict")
            .setThreshold(0.5)
        
        val finalBinarizedResult = finalBinarizer.transform(finalResultDF)
        finalBinarizedResult.groupBy(label, predict).count().show(false)
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

        