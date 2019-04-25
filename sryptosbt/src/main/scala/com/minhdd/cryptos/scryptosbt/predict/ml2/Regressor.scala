package com.minhdd.cryptos.scryptosbt.predict.ml2

import com.minhdd.cryptos.scryptosbt.predict.ml.MLSegmentsGBTRegressor.{label, predict, prediction}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{DataFrame, SparkSession}

object Regressor {
    def main(args: Array[String]): Unit = {
        results()
    }
    def t() = {
        import com.minhdd.cryptos.scryptosbt.predict.ml2.ml2._
        import com.minhdd.cryptos.scryptosbt.predict.ml.MLSegmentsGBTRegressor.{label, predict, prediction}
        import org.apache.spark.ml.Pipeline
        import org.apache.spark.ml.evaluation.RegressionEvaluator
        import org.apache.spark.ml.feature.Binarizer
        import org.apache.spark.ml.regression.GBTRegressor
        import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
        import org.apache.spark.sql.{DataFrame, SparkSession}
        
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
        result.write.parquet("D:\\ws\\cryptos\\data\\csv\\segments\\all-190418\\result")

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
    
    def results(): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        import org.apache.spark.sql.functions._
        val df = ss.read.parquet("D:\\ws\\cryptos\\data\\csv\\segments\\all-190418\\result")
//        df.select("label", "prediction").show(false)
//        println("-----min-----max------")
//        df.agg(min("prediction"), max("prediction")).show(false)
//        println("-----stats------")
//        val stats: Array[Double] = df.stat.approxQuantile("prediction", Array(0.1D, 0.2D, 0.3D, 0.4D, 0.5D, 0.6D, 0.7D, 0.8D, 0.9D), 0.00001)
//        stats.foreach(println)
        println("-----binarizers------")
        for (i <- Seq(1.01897137, 1.01897139, 1.0189714, 1.02, 1.021)) {
            val binarizerForSegmentDetection = new Binarizer()
              .setInputCol(prediction)
              .setOutputCol(predict)
              .setThreshold(i)
            val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(df)
            val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
            val truePositif: Long = extractThridValueWithTwoFilter(counts, 1, 1.0)
            val falsePositif: Long = extractThridValueWithTwoFilter(counts, 0, 1.0)
            val falseNegative: Long = extractThridValueWithTwoFilter(counts, 1, 0.0)
            val trueNegative: Long = extractThridValueWithTwoFilter(counts, 0, 0.0)
            counts.show()
            val total = truePositif + falsePositif + falseNegative + trueNegative
            val rate1 = truePositif.toDouble / (truePositif + falsePositif)
            println("rate true positif : " + rate1)
            val rate2 = trueNegative.toDouble / (falseNegative + trueNegative)
            println("rate trueNegative : " + rate2)
            println("rate potisif : " + (truePositif + falsePositif).toDouble/total)
            println("rate true :" + (truePositif + trueNegative).toDouble/total)
            println(i)
            println("----------------------------------------------------------------")
        }
    }
    
    
    private def extractThridValueWithTwoFilter(counts: DataFrame, labelValue: Int, predictValue: Double): Long = {
        import org.apache.spark.sql.functions._
        val row = counts.filter(col("label") === labelValue).filter(col("predict") === predictValue)
        
        if (row.count() == 1) {
            row.first().getAs[Long](2)
        } else {
            0
        }
    }
    
    
    def getThreshold(df: DataFrame): Double = {
        1D
    }
}
