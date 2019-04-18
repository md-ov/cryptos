package com.minhdd.cryptos.scryptosbt.predict.ml2

import com.minhdd.cryptos.scryptosbt.predict.ml.ExpansionSegmentsTransformer
import com.minhdd.cryptos.scryptosbt.predict.ml.MLSegmentsGBTRegressor.{predict, prediction, label}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Regressor {
    def main(args: Array[String]): Unit = {
        t()
    }
    def t() = {
        import com.minhdd.cryptos.scryptosbt.predict.ml2.ml2._
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df: DataFrame = ss.read.parquet("D:\\ws\\cryptos\\data\\csv\\segments\\all-190418\\beforesplits")
        val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=42)
        val gbt = new GBTRegressor()
        gbt.setSeed(273).setMaxIter(5)
        val expanser = new ExpansionSegmentsTransformer(ss, 
            ss.read.parquet("D:\\ws\\cryptos\\data\\csv\\segments\\all-190418\\expansion").schema)
        val pipeline = new Pipeline().setStages(Array(expanser, indexerBegin, vectorAssembler, gbt))
        val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(5, 10, 20, 50, 100)).build()
        val evaluator = new RegressionEvaluator().setLabelCol(label).setPredictionCol(prediction)
        val cv = new CrossValidator()
          .setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
          .setNumFolds(3).setSeed(27)
        val model = cv.fit(trainDF)
        val result = model.transform(testDF)
        result.show(false)
        result.write.csv("D:\\ws\\cryptos\\data\\csv\\segments\\all-190418\\result")
        
        
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
    
    
        for (i <- 2 to 9) {
            binarizerForSegmentDetection.setThreshold(i/10)
            val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(result)
            val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
            counts.show()
        } 
        
    }
    
    
}
