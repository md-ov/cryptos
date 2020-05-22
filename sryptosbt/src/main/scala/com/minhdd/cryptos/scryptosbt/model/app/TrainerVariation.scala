package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper
import org.apache.spark.sql.SparkSession

object TrainerVariation {
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("training")
          .master("local[*]").getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        val path: String = s"$dataDirectory/segments/small/$smallSegmentsFolder"
        val expansionStrucTypePath = this.getClass.getResource("/expansion").getPath
        val modelPath = s"$dataDirectory/ml/variation-models/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"
        val resultPath = s"$dataDirectory/ml/variation-results/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"
        //        trainingModelAndWriteModelAndTestDfWithRawPrediction(spark, path, expansionStrucTypePath, modelPath, resultPath)
    }
}
