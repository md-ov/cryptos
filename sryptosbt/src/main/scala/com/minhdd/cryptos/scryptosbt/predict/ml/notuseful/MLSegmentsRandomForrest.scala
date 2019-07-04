package com.minhdd.cryptos.scryptosbt.predict.ml.notuseful

import com.minhdd.cryptos.scryptosbt.predict.ml.ml._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object MLSegmentsRandomForrest {
    
    def main(args: Array[String]): Unit = {
        val rf = new RandomForestRegressor()
            .setLabelCol("label")
            .setSeed(27)
        rf.setNumTrees(2)
        rf.setMaxDepth(10)

        val paramGrid = new ParamGridBuilder()
            .addGrid(rf.maxDepth, Array(2, 5, 10, 20, 30))
            .addGrid(rf.numTrees, Array(10, 20, 50, 100, 200))
            .build()

        val ml = rf

        val pipeline = new Pipeline().setStages(Array(indexerBegin, indexerEnd, vectorAssembler, ml))

        val binarizer = new Binarizer()
            .setInputCol("prediction")
            .setOutputCol("predict")
            .setThreshold(0.44)

        val evaluator = new RegressionEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")

        val cv = new CrossValidator()
            .setEstimator(pipeline)
            .setEvaluator(evaluator)
            .setEstimatorParamMaps(paramGrid)
            .setNumFolds(3)
            .setSeed(27)
        
        ///////////////////////////////////////////
        
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")

//        val df: DataFrame =
//            ss.read
//                .option("sep", ";")
//                .schema(csvSchema)
//                .csv("/home/mdao/Downloads/segments.csv")
//                .filter(!(col("begin-evolution") === "-"))
//                .filter(!(col("end-evolution") === "-"))
        
//        val df1: DataFrame =
//            ss.read
//              .option("sep", ";")
//              .schema(csvSchema)
//              .csv("D:\\ws\\cryptos\\data\\segments\\trades-190126before1803")
//
//        val df2: DataFrame = 
//            ss.read
//              .option("sep", ";")
//              .schema(csvSchema)
//              .csv("D:\\ws\\cryptos\\data\\segments\\trades-190129-from")
//
//        val df = df1.union(df2)
//          .filter(!(col("begin-evolution") === "-"))
//          .filter(!(col("end-evolution") === "-"))
    
        val df: DataFrame =
            ss.read
              .option("sep", ";")
              .schema(csvSchema)
              .csv("D:\\ws\\cryptos\\data\\segments\\ohlc-190129-2")
                  .filter(!(col("begin-evolution") === "-"))
                  .filter(!(col("end-evolution") === "-"))
        
        df.show(4, false)
        df.printSchema()
    
        val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=42)

        val pipelineModel = pipeline.fit(trainDF)

        val cvModel = cv.fit(trainDF)
        
//        
        binarizer
            .transform(pipelineModel.transform(testDF))
            .groupBy("label", "predict").count().show(10,false)

        val resultDF2: DataFrame = cvModel.transform(testDF)
        val binaryResultDf = binarizer.transform(resultDF2)
    
//        binaryResultDf.show(2,false)
    
        binaryResultDf.groupBy("label", "predict").count().show(10,false)
    
        binaryResultDf
          .filter(col("predict") === 1)
          .filter(col("label") === 0)
          .show(100, false)
    
        binaryResultDf
          .filter(col("predict") === 0)
          .filter(col("label") === 1)
          .show(100, false)
    
        binaryResultDf
          .filter(col("predict") === 1)
          .filter(col("label") === 1)
          .filter(!(col("begin-evolution") === col("end-evolution")))
          .show(100, false)

        binaryResultDf
          .filter(col("predict") === 0)
          .filter(col("label") === 0)
          .filter(!(col("begin-evolution") === col("end-evolution")))
          .show(100, false)

    }
}
