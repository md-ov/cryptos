package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.model.service.ml.{indexerBegin, vectorAssembler, _}
import com.minhdd.cryptos.scryptosbt.model.service.{Expansion, ExpansionSegmentsTransformer}
import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ModelTrainer {
    
    def main(args: Array[String]): Unit = {
        trainingModelAndWriteModelAndTestDfWithRawPrediction
    }
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    private def trainingModelAndWriteModelAndTestDfWithRawPrediction() = {
        val path: String = s"$dataDirectory\\segments\\small\\$numberOfMinutesBetweenTwoElement\\$directoryNow"
        val df: DataFrame = spark.read.parquet(path)
        
        val transformer: ExpansionSegmentsTransformer = Expansion.getTransformer(spark)
        
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
        ModelHelper.saveModel(spark, model, s"$dataDirectory\\ml\\models\\$numberOfMinutesBetweenTwoElement\\$directoryNow")
        
        val testDfWithRawPrediction: DataFrame = model.transform(testDF)
        testDfWithRawPrediction.show(false)
        testDfWithRawPrediction.write.parquet(s"$dataDirectory\\ml\\results\\$numberOfMinutesBetweenTwoElement\\$directoryNow")
    }
}
