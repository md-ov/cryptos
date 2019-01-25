package com.minhdd.cryptos.scryptosbt.predict.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MLSegmentsDecisionTree {
    
    val dtc = new DecisionTreeClassifier()
    val ml = dtc
    
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
    
        val df1: DataFrame =
            ss.read
              .option("sep", ";")
              .schema(csvSchema)
              .csv("D:\\ws\\cryptos\\data\\csv\\segments\\trades-190120-1")
        
        val df2: DataFrame = 
            ss.read
              .option("sep", ";")
              .schema(csvSchema)
              .csv("D:\\ws\\cryptos\\data\\csv\\segments\\trades-190120-3")
        
        val df = df1.union(df2)
    
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

//        resultDF
//          .filter(col("predict") === 1)
//          .filter(col("label") === 0)
//          .show(100, false)
//
//        resultDF
//          .filter(col("predict") === 0)
//          .filter(col("label") === 1)
//          .show(100, false)
//
//        resultDF
//          .filter(col("predict") === 1)
//          .filter(col("label") === 1)
//          .filter(!(col("begin-evolution") === col("end-evolution")))
//          .show(100, false)
//    
//        resultDF
//          .filter(col("predict") === 0)
//          .filter(col("label") === 0)
//          .filter(!(col("begin-evolution") === col("end-evolution")))
//          .show(100, false)

    }
}
