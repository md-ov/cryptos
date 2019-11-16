package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.model.service.{Expansion, ExpansionSegmentsTransformer}
import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.minhdd.cryptos.scryptosbt.model.service.ml.{indexerBegin, vectorAssembler}
import com.minhdd.cryptos.scryptosbt.constants.dataDirectory
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import com.minhdd.cryptos.scryptosbt.model.service.ml._

object ModelTrainer {
    
    def main(args: Array[String]): Unit = {
        trainingModelAndWriteModelAndTestDfWithRawPrediction
    }
    
    private val segmentDirectory = "20191116-all"
    
    private def trainingModelAndWriteModelAndTestDfWithRawPrediction() = {
        val spark: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        val df: DataFrame = spark.read.parquet(s"$dataDirectory\\segments\\small\\15\\$segmentDirectory")
    
        val transformer: ExpansionSegmentsTransformer = Expansion.getTransformer(spark)
        
        val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=42)
        val gbt = new GBTRegressor()
        gbt.setSeed(273).setMaxIter(5)
        
        val pipeline = new Pipeline().setStages(Array(transformer, indexerBegin, vectorAssembler, gbt))
        val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(5, 10, 20, 50, 100)).build()
        val evaluator = new RegressionEvaluator().setLabelCol(label).setPredictionCol(prediction)
        val cv = new CrossValidator()
          .setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
          .setNumFolds(3).setSeed(27)
        
        val model: CrossValidatorModel = cv.fit(trainDF)
        ModelHelper.saveModel(spark, model, s"$dataDirectory\\ml\\models\\$segmentDirectory")
        
        val testDfWithRawPrediction: DataFrame = model.transform(testDF)
        testDfWithRawPrediction.show(false)
        testDfWithRawPrediction.write.parquet(s"$dataDirectory\\ml\\results\\$segmentDirectory")
        
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
        
        for (i <- 0 to 10) {
            println("threshold : " + i.toDouble/10)
            binarizerForSegmentDetection.setThreshold(i.toDouble/10)
            val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(testDfWithRawPrediction)
            val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
            counts.show()
        }
    }
}
