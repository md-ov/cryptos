package com.minhdd.cryptos.scryptosbt.predict.ml.notuseful

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{abs, col}

object ResultExaminerForAllSegmentsWithImportantChangeAtTheEndOrNot {
    val thresholds = Seq(0.99, 0.9, 0.8, 0.7, 0.6, 0.55, 0.1)
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
          .csv("D:\\ws\\cryptos\\data\\mlresults\\7")
          .withColumn(absolute_predict, abs(col(predictionColumn)))
          .withColumn(absolute_label, abs(col(labelColumn)))
        
        df.show(false)
        println(df.count)
        
        val binarizer = new Binarizer()
          .setInputCol(absolute_predict)
          .setOutputCol(predict)
    
        binarizer.setThreshold(0.5)
        val binaryResultDf = binarizer.transform(df)
        binaryResultDf.show(false)
        binaryResultDf.groupBy(absolute_label, predict).count().show(false)
    
        println(thresholds)
        for (threshold <- thresholds) {
            binarizer.setThreshold(threshold)
            val binaryResultDf = binarizer.transform(df)
            val counts = binaryResultDf.groupBy(absolute_label, predict).count()
            val falseNegatif = counts.filter(col("predict") === 0 && col(absolute_label) === 1).head.getAs[Long]("count")
            val truePositif = counts.filter(col("predict") === 1 && col(absolute_label) === 1).head.getAs[Long]("count")
            val falsePositif = counts.filter(col("predict") === 1 && col(absolute_label) === 0).head.getAs[Long]("count")
            println(threshold + "\t\t" + 100*truePositif.toDouble/(truePositif+falsePositif) + " %" 
              + " \t\t- " + 100*truePositif.toDouble/ (truePositif + falseNegatif) + " %" )
        }    
        
        val filtered = binaryResultDf.filter(col("predict") === 1)

        val updownbina = new Binarizer()
          .setInputCol(predictionColumn)
          .setOutputCol("upOrDown-predict")

        updownbina.setThreshold(0)
        updownbina.transform(filtered).groupBy("_c0", "_c1", "_c2", "upOrDown-predict", predict).count().show(false)
        //_c0 : begin evolution
        //_c1 : same evolution
            
//            binaryResultDf
//              .filter(col(predict) === 1)
//              .filter(col(absolute_label) === 0)
//              .show(100, false)
//    
//            binaryResultDf
//              .filter(col(predict) === 0)
//              .filter(col(absolute_label) === 1)
//              .show(100, false)
//    
//            binaryResultDf
//              .filter(col(predict) === 1)
//              .filter(col(absolute_label) === 1)
//              .filter(col("_c1") === "false")
//              .show(100, false)
//    
//            binaryResultDf
//              .filter(col(predict) === 0)
//              .filter(col(absolute_label) === 0)
//              .filter(col("_c1") === "false")
//              .show(100, false)
    
    
//            println(binaryResultDf.filter(col("_c1") === "false").count())
        
    }
}
