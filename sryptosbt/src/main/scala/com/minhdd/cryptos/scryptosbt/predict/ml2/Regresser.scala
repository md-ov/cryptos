package com.minhdd.cryptos.scryptosbt.predict.ml2

import com.minhdd.cryptos.scryptosbt.predict.BeforeSplit
import com.minhdd.cryptos.scryptosbt.predict.predict.dataDirectory
import com.minhdd.cryptos.scryptosbt.tools.{Models, Timestamps}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ml2.{label, predict, prediction}

case class Rates(truePositive: Double, falsePositive: Double, trueRate: Double, falseNegative: Double)

object Regressor {
    val segmentDirectory = "all-190601-fusion"
    
    def main(args: Array[String]): Unit = {
//        resultss()
//        t
//        predictOneSegment()
//        trainingModelAndWriteTestDfWithRawPrediction
        whyThereIsSomeNull
    }
    
    def whyThereIsSomeNull() = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        import ss.implicits._
        val ds = ss.read.parquet(s"$dataDirectory\\csv\\segments\\all-190611-fusion\\beforesplits").as[Seq[BeforeSplit]]
        val f: Dataset[Seq[BeforeSplit]] = ds.filter(s => s.exists(_.secondDerive.isEmpty))
        f.show(false)
    }
    
    def trainingModelAndWriteTestDfWithRawPrediction() = {
        import com.minhdd.cryptos.scryptosbt.predict.ml2.ml2.{indexerBegin, vectorAssembler}
        import org.apache.spark.ml.Pipeline
        import org.apache.spark.ml.evaluation.RegressionEvaluator
        import org.apache.spark.ml.feature.Binarizer
        import org.apache.spark.ml.regression.GBTRegressor
        import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
        
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df: DataFrame = ss.read.parquet(s"$dataDirectory\\csv\\segments\\$segmentDirectory\\beforesplits")
        val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=42)
        val gbt = new GBTRegressor()
        gbt.setSeed(273).setMaxIter(5)
        val expanser = new ExpansionSegmentsTransformer(ss,
                    ss.read.parquet("file://" + getClass.getResource("/expansion").getPath).schema)
//        val expanser = new ExpansionSegmentsTransformer(ss, 
//            ss.read.parquet(s"$dataDirectory\\csv\\segments\\all-190418\\expansion").schema)
        val pipeline = new Pipeline().setStages(Array(expanser, indexerBegin, vectorAssembler, gbt))
        val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(5, 10, 20, 50, 100)).build()
        val evaluator = new RegressionEvaluator().setLabelCol(label).setPredictionCol(prediction)
        val cv = new CrossValidator()
          .setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
          .setNumFolds(3).setSeed(27)
        val model: CrossValidatorModel = cv.fit(trainDF)
        Models.saveModel(ss, model, s"$dataDirectory\\models\\$segmentDirectory")
        val testDfWithRawPrediction: DataFrame = model.transform(testDF)
        testDfWithRawPrediction.show(false)
        testDfWithRawPrediction.write.parquet(s"$dataDirectory\\csv\\segments\\$segmentDirectory\\result")

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
    
    def exploreTestDfAndFindThreshold(): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df: DataFrame = ss.read.parquet(s"$dataDirectory\\csv\\segments\\$segmentDirectory\\result")
        
        for (i <- 0 to 10) {
            val binarizerForSegmentDetection = new Binarizer()
              .setInputCol(prediction)
              .setOutputCol(predict)
            println("threshold : " + i.toDouble/10)
            binarizerForSegmentDetection.setThreshold(i.toDouble/10)
            val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(df)
            val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
            counts.show()
        }
        
        val t: (Double, Rates) = ThresholdCalculator.getThreshold(ss, df)
        println(t)
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
        binarizerForSegmentDetection.setThreshold(t._1)
        val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(df)
        val counts: DataFrame = segmentDetectionBinaryResults.groupBy(label, predict).count()
        counts.show()
    }

}
