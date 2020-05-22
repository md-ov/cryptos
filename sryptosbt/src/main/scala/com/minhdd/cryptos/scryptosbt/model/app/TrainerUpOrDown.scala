package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.model.service.{Expansion, ExpansionSegmentsTransformer}
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper
import org.apache.spark.sql.SparkSession

// it takes 20h to run this trainer of model
object TrainerUpOrDown {
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("training")
          .master("local[*]").getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        val path: String = s"$dataDirectory/segments/small/$smallSegmentsFolder"
        
        val modelPath = s"$dataDirectory/ml/models/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"
        val resultPath = s"$dataDirectory/ml/results/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"
        
        val expansionStrucTypePath: String = this.getClass.getResource("/expansion").getPath
        val transformer: ExpansionSegmentsTransformer = Expansion.getTransformer(spark, expansionStrucTypePath)
        Trainer.trainingModelAndWriteModelAndTestDfWithRawPrediction(spark, path, modelPath, resultPath, transformer)
    }
}
