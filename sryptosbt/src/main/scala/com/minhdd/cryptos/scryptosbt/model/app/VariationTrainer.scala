package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.env._
import org.apache.spark.sql.SparkSession

object VariationTrainer {
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("training")
          .master("local[*]").getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        val path: String = s"$dataDirectory/segments/small/$numberOfMinutesBetweenTwoElement/$directoryNow"
        val expansionStrucTypePath = this.getClass.getResource("/expansion").getPath
        val modelPath = s"$dataDirectory/ml/variation-models/$numberOfMinutesBetweenTwoElement/$directoryNow"
        val resultPath = s"$dataDirectory/ml/variation-results/$numberOfMinutesBetweenTwoElement/$directoryNow"
        //        trainingModelAndWriteModelAndTestDfWithRawPrediction(spark, path, expansionStrucTypePath, modelPath, resultPath)
    }
}
