package com.minhdd.cryptos.scryptosbt.predict

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Binarizer, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MLSegments {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
    
        val csvSchema = StructType(
            List(
                StructField("t1", TimestampType, nullable = false),
                StructField("t2", TimestampType, nullable = false),
                StructField("begin-value", DoubleType, nullable = false),
                StructField("end-value", DoubleType, nullable = false),
                StructField("begin-evolution", StringType, nullable = true),
                StructField("begin-variation", DoubleType, nullable = false),
                StructField("end-evolution", StringType, nullable = true),
                StructField("end-variation", DoubleType, nullable = false),
                StructField("same", BooleanType, nullable = false),
                StructField("size", IntegerType, nullable = false)
            )
        )
        
        val df: DataFrame = 
            ss.read
              .option("sep", ";")
              .schema(csvSchema)
              .csv("D:\\ws\\cryptos\\data\\csv\\segments\\trades-190120-5")
        
//        df.show(4, false)
        df.printSchema()
    
        val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=42)
    
        val indexerBegin = new StringIndexer()
          .setInputCol("begin-evolution")
          .setOutputCol("begin-evo")
    
        val indexerEnd = new StringIndexer()
          .setInputCol("end-evolution")
          .setOutputCol("label")
        
        val vectorAssembler = new VectorAssembler()
          .setInputCols(Array("begin-value", "begin-evo", "begin-variation"))
          .setOutputCol("features")
        
        val rf = new RandomForestRegressor()
          .setLabelCol("label")
          .setSeed(27)
        rf.setNumTrees(2)
        rf.setMaxDepth(10)
        
        val pipeline = new Pipeline().setStages(Array(indexerBegin, indexerEnd, vectorAssembler, rf))

        val pipelineModel = pipeline.fit(trainDF)
//        
        val resultDF: DataFrame = pipelineModel.transform(testDF)
    
        val binarizer = new Binarizer()
          .setInputCol("prediction")
          .setOutputCol("predict")
          .setThreshold(0.5)
    
        val binaryResultDf = binarizer.transform(resultDF)
    
        binaryResultDf.show(2,false)
    
//        val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")
//        println(s"Accuracy: ${evaluator.evaluate(resultDF)}")
        binaryResultDf.groupBy("label", "predict").count().show(4,false)
    }
}
