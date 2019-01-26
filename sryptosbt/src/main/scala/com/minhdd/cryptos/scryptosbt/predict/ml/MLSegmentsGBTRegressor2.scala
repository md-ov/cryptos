package com.minhdd.cryptos.scryptosbt.predict.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Binarizer, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object MLSegmentsGBTRegressor2 {
    
    def main(args: Array[String]): Unit = {
        val gbt = new GBTRegressor()
        gbt.setSeed(273).setMaxIter(5)
        val ml = gbt
        
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

//        val df: DataFrame =
//            ss.read
//                .option("sep", ";")
//                .schema(csvSchema)
//                .csv("/home/mdao/Downloads/segments.csv")
//                    .filter(!(col("begin-evolution") === "-"))
//                    .filter(!(col("end-evolution") === "-"))

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
                  .filter(!(col("begin-evolution") === "-"))
                  .filter(!(col("end-evolution") === "-"))

        //        val df1: DataFrame =
        //            ss.read
        //              .option("sep", ";")
        //              .schema(csvSchema)
        //              .csv("D:\\ws\\cryptos\\data\\csv\\segments\\trades-190120-5")

        df.show(4, false)
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
        val model = pipeline.fit(trainDF)
        
        val resultDF = model.transform(testDF)

        val binarizer = new Binarizer()
            .setInputCol("prediction")
            .setOutputCol("predict")
            .setThreshold(0.5)

        val binaryResultDf = binarizer.transform(resultDF)

        binaryResultDf.groupBy("label", "predict").count().show(10,false)
        
//        val pipelineModel = pipeline.fit(trainDF)
//        
//        val resultDF: DataFrame = pipelineModel.transform(testDF)
//        
//        resultDF.show(2, false)
//
//        val binarizer = new Binarizer()
//          .setInputCol("prediction")
//          .setOutputCol("predict")
//          .setThreshold(0.53)
//
//        val binaryResultDf = binarizer.transform(resultDF)
//
//        binaryResultDf.show(2,false)
//
//        binaryResultDf.groupBy("label", "predict").count().show(10,false)

        binaryResultDf
          .filter(col("predict") === 1)
          .filter(col("label") === 0)
          .show(100, false)

        binaryResultDf
          .filter(col("predict") === 0)
          .filter(col("label") === 1)
          .show(100, false)

        binaryResultDf
          .filter(col("predict") === 1)
          .filter(col("label") === 1)
          .filter(!(col("begin-evolution") === col("end-evolution")))
          .show(100, false)

        binaryResultDf
          .filter(col("predict") === 0)
          .filter(col("label") === 0)
          .filter(!(col("begin-evolution") === col("end-evolution")))
          .show(100, false)

    }
}
