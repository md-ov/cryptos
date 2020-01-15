package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import com.minhdd.cryptos.scryptosbt.model.service.ml._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Trainer {
    def trainingModelAndWriteModelAndTestDfWithRawPrediction(spark: SparkSession,
                                                             segmentsPath: String,
                                                             modelPath: String,
                                                             resultPath: String,
                                                             transformer: Transformer): Unit = {
        val df: DataFrame = spark.read.parquet(segmentsPath)
        
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
