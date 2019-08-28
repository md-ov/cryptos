package com.minhdd.cryptos.scryptosbt.predict.ml.notuseful

import com.minhdd.cryptos.scryptosbt.predict.ml.ml._
import com.minhdd.cryptos.scryptosbt.constants._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}



object MLSegmentsGBTRegressor2 {
    
    def main(args: Array[String]): Unit = {
        val gbt = new GBTRegressor()
        gbt.setSeed(273).setMaxIter(10)
        val ml = gbt
        
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")


//        val df: DataFrame =
//            ss.read
//                .option("sep", ";")
//                .schema(csvSchema)
//                .csv("/home/mdao/Downloads/segments.csv")
//                    .filter(!(col("begin-evolution") === evolutionNone))
//                    .filter(!(col("end-evolution") === evolutionNone))

        val df1: DataFrame =
            ss.read
              .option("sep", ";")
              .schema(csvSchema)
              .csv("D:\\ws\\cryptos\\data\\segments\\trades-190126before1803")

        val df2: DataFrame = 
            ss.read
              .option("sep", ";")
              .schema(csvSchema)
              .csv("D:\\ws\\cryptos\\data\\segments\\trades-190129-from")

        val df = df1.union(df2)
                  .filter(!(col("begin-evolution") === evolutionNone))
                  .filter(!(col("end-evolution") === evolutionNone))

        //        val df1: DataFrame =
        //            ss.read
        //              .option("sep", ";")
        //              .schema(csvSchema)
        //              .csv("D:\\ws\\cryptos\\data\\segments\\trades-190120-5")

        df.show(4, false)
        df.printSchema()

        val Array(trainDF, testDF) = df.randomSplit(Array(0.8, 0.2), seed=42)

        val pipeline = new Pipeline().setStages(Array(indexerBegin, indexerEnd, vectorAssembler, ml))
        val model = pipeline.fit(trainDF)
        
        val resultDF = model.transform(testDF)

        val binarizer = new Binarizer()
            .setInputCol("prediction")
            .setOutputCol("predict")
            .setThreshold(0.6)

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
