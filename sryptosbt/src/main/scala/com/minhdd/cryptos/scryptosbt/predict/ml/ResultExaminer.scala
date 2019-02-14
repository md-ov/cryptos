package com.minhdd.cryptos.scryptosbt.predict.ml

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ResultExaminer {
    val thresholds = Seq(0.68, 0.6)
    val labelColumn = "_c2"
    val predict = "predict"
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df = ss.read
          .option("inferSchema", "true")
          .csv("D:\\ws\\cryptos\\data\\mlresults\\3.txt")
        
        val binarizer = new Binarizer()
          .setInputCol("_c3")
          .setOutputCol(predict)
        for (threshold <- thresholds) {
            binarizer.setThreshold(threshold)
            val binaryResultDf = binarizer.transform(df)
            binaryResultDf.groupBy(labelColumn, predict).count().show(10, false)
            binaryResultDf
              .filter(col(predict) === 1)
              .filter(col(labelColumn) === 0)
              .show(100, false)
    
            binaryResultDf
              .filter(col(predict) === 0)
              .filter(col(labelColumn) === 1)
              .show(100, false)
    
            binaryResultDf
              .filter(col(predict) === 1)
              .filter(col(labelColumn) === 1)
              .filter(col("_c1") === "false")
              .show(100, false)
    
            binaryResultDf
              .filter(col(predict) === 0)
              .filter(col(labelColumn) === 0)
              .filter(col("_c1") === "false")
              .show(100, false)
    
    
            println(binaryResultDf.filter(col("_c1") === "false").count())
        }
    }
}
