package com.minhdd.cryptos.scryptosbt.predict.ml

import com.minhdd.cryptos.scryptosbt.predict.ml.ml._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MLSegmentsGBTRegressorForTrueSegment {
    
    def main(args: Array[String]): Unit = {
        val gbt = new GBTRegressor()
        gbt.setSeed(273).setMaxIter(5)
        val ml = gbt

        val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(5, 10, 20, 50, 100)).build()
        val pipeline = new Pipeline().setStages(Array(indexerBegin, indexerEnd, vectorAssembler, ml))

        val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction")

        val cv = new CrossValidator()
            .setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
            .setNumFolds(3).setSeed(27)
        
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
    
        val df: DataFrame =
            ss.read
              .option("sep", ";").schema(csvSchema)
              .csv("D:\\ws\\cryptos\\data\\csv\\segments\\all-190221")
              .filter(!(col("begin-evolution") === "-"))
              .filter(!(col("end-evolution") === "-"))

        df.show(false)
        df.printSchema()
        
        val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=42)
        val model = cv.fit(trainDF)
        val resultDF: DataFrame = model.transform(testDF)
        
        val binarizer = new Binarizer()
            .setInputCol("prediction")
            .setOutputCol("predict")
            .setThreshold(0.68)

        val binaryResultDf = binarizer.transform(resultDF)

        binaryResultDf.groupBy("label", "predict").count().show(10,false)
        println(binaryResultDf.filter(!(col("begin-evolution") === col("end-evolution"))).count())

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

    }
}

