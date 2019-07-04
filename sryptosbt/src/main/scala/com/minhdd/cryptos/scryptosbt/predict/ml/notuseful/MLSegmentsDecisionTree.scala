package com.minhdd.cryptos.scryptosbt.predict.ml.notuseful

import com.minhdd.cryptos.scryptosbt.predict.ml.ml._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MLSegmentsDecisionTree {
    
    val dtc = new DecisionTreeClassifier()
    val ml = dtc
    
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
    
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
    
        val df: DataFrame =
            ss.read
              .option("sep", ";")
              .schema(csvSchema)
              .csv("D:\\ws\\cryptos\\data\\segments\\trades-190213")
    
        df.printSchema()
    
        val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=42)
    
        val pipeline = new Pipeline().setStages(Array(indexerBegin, indexerEnd, vectorAssembler, ml))

        val pipelineModel = pipeline.fit(trainDF)
//        
        val resultDF: DataFrame = pipelineModel.transform(testDF)
    
//        val binarizer = new Binarizer()
//          .setInputCol("prediction")
//          .setOutputCol("predict")
//          .setThreshold(0.4)
    
//        val binaryResultDf = binarizer.transform(resultDF)
    
        resultDF.show(2,false)
    
//        val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
//        println(s"Accuracy: ${evaluator.evaluate(resultDF)}")
        resultDF.groupBy("label", "prediction").count().show(10,false)

        resultDF
          .filter(col("prediction") === 1)
          .filter(col("label") === 0)
          .show(100, false)

        resultDF
          .filter(col("prediction") === 0)
          .filter(col("label") === 1)
          .show(100, false)

        resultDF
          .filter(col("prediction") === 1)
          .filter(col("label") === 1)
          .filter(!(col("begin-evolution") === col("end-evolution")))
          .show(100, false)

        resultDF
          .filter(col("prediction") === 0)
          .filter(col("label") === 0)
          .filter(!(col("begin-evolution") === col("end-evolution")))
          .show(100, false)

    }
}
