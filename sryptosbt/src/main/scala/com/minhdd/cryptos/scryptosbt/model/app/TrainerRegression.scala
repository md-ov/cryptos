package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.model.service.ml.{indexerBegin, label, prediction, vectorAssembler}
import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TrainerRegression {
  def train(spark: SparkSession,
            segmentsPath: String,
            modelPath: String,
            resultPath: String,
            transformer: Transformer): Unit = {
    val df: DataFrame = spark.read.parquet(segmentsPath)
    val lr = new LogisticRegression().setMaxIter(10)
    val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed = 42)

    val pipeline: Pipeline = new Pipeline().setStages(Array(transformer, indexerBegin, vectorAssembler, lr))
    val paramGrid: Array[ParamMap] = new ParamGridBuilder().addGrid(param = lr.maxIter, values = Array(5, 50, 100)).build()

    val evaluator: RegressionEvaluator = new RegressionEvaluator().setLabelCol(label).setPredictionCol(prediction)
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3).setSeed(27)

    val model: CrossValidatorModel = cv.fit(trainDF)
    ModelHelper.saveModel(spark, model, modelPath)

    val testDfWithRawPrediction: DataFrame = model.transform(testDF)
    testDfWithRawPrediction.show(false)
    testDfWithRawPrediction.write.parquet(resultPath)
  }
}
