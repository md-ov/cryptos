package com.minhdd.cryptos.scryptosbt.predict.ml.notuseful

import org.apache.spark.sql.SparkSession

object ResultExaminer3 {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("examiner").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
    
        
        
        val df = ss.read
          .option("inferSchema", "true")
          .csv("D:\\ws\\cryptos\\data\\mlresults\\2.txt")
        
        df.show(false)
        
        df.groupBy("_c0", "_c1", "_c2", "_c3").count().show(false)
    }
}
