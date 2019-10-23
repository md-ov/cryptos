package com.minhdd.cryptos.scryptosbt.predict.ml.notuseful

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

object ResultExaminerForAllSegmentsWithImportantChangeAtTheEndOrNotV2 {
    val thresholds = Seq(0.99, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1)
    // _c0 : begin-evolution
    // _c1 : end-evolution
    val labelColumn = "_c2" // c'est le label -1 c'est down, 1 c'est up, 0 : pas de changement important up ou down à
    // la fin, 
    val predictionColumn = "_c3"
    val absolute_label = "absolute_label"
    val absolute_predict = "absolute_predict"
    val predict = "predict"
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df = ss.read
          .option("inferSchema", "true")
          .csv("D:\\ws\\cryptos\\data\\mlresults\\9")
        
//        df.show(false)
        val totalCount = df.count
        
        val binarizer = new Binarizer()
          .setInputCol(predictionColumn)
          .setOutputCol(predict)

        for (threshold <- thresholds) {
            println(threshold)
            binarizer.setThreshold(threshold)
            val binaryResultDf = binarizer.transform(df)
            val counts = binaryResultDf.groupBy("_c0", "_c1", labelColumn, predict).count()
            if (threshold == 0.5) binaryResultDf.groupBy("_c1", labelColumn, predict).count().show(false)
            val goodResultCount = 
                counts.filter(col(labelColumn) === col(predict)).agg(sum("count")).first.getAs[Long](0)
            println(threshold + " : " + (goodResultCount.toDouble*100)/totalCount)
        }
    

        
    }
}
