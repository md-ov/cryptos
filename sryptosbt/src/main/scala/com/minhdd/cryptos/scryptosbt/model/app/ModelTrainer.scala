package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.model.service.ml.{indexerBegin, vectorAssembler, _}
import com.minhdd.cryptos.scryptosbt.model.service.{Expansion, ExpansionSegmentsTransformer}
import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}


// it takes 20h to run this trainer of model
object ModelTrainer {
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("big segments")
          .master("local[*]").getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        val path: String = s"$dataDirectory\\segments\\small\\$numberOfMinutesBetweenTwoElement\\$directoryNow"
        val expansionStrucTypePath = "file://" + getClass.getResource("/expansion").getPath
        val modelPath = s"$dataDirectory\\ml\\models\\$numberOfMinutesBetweenTwoElement\\$directoryNow"
        val resultPath = s"$dataDirectory\\ml\\results\\$numberOfMinutesBetweenTwoElement\\$directoryNow"
        trainingModelAndWriteModelAndTestDfWithRawPrediction(spark, path, expansionStrucTypePath, modelPath, resultPath)
    }
    
    def trainingModelAndWriteModelAndTestDfWithRawPrediction(spark: SparkSession,
                                                             segmentsPath: String,
                                                             structypePath: String,
                                                             modelPath: String,
                                                             resultPath: String): Unit = {
        val df: DataFrame = spark.read.parquet(segmentsPath)
        
        val transformer: ExpansionSegmentsTransformer = Expansion.getTransformer(spark, structypePath)
        
        val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed = 42)
        val gbt = new GBTRegressor()
        gbt.setSeed(273).setMaxIter(5)
        
        val pipeline = new Pipeline().setStages(Array(transformer, indexerBegin, vectorAssembler, gbt))
        val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(5, 20, 50, 100)).build()
        val evaluator = new RegressionEvaluator().setLabelCol(label).setPredictionCol(prediction)
        val cv = new CrossValidator()
          .setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
          .setNumFolds(3).setSeed(27)
        
        val model: CrossValidatorModel = cv.fit(trainDF)
        ModelHelper.saveModel(spark, model, modelPath)
        
        val testDfWithRawPrediction: DataFrame = model.transform(testDF)
        testDfWithRawPrediction.show(false)
        testDfWithRawPrediction.write.parquet(resultPath)
    }
}
