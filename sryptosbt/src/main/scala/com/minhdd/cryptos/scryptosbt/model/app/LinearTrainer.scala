package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.model.service.Expansion
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.SparkSession

object LinearTrainer {
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("training")
          .master("local[*]").getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        val path: String = s"$dataDirectory\\segments\\small\\$numberOfMinutesBetweenTwoElement\\$directoryNow"
        val modelPath = s"$dataDirectory\\ml\\linear-models\\$numberOfMinutesBetweenTwoElement\\$directoryNow"
        val resultPath = s"$dataDirectory\\ml\\linear-results\\$numberOfMinutesBetweenTwoElement\\$directoryNow"
    
        val expansionStrucTypePath = "file://" + getClass.getResource("/expansion").getPath
        val transformer: Transformer = Expansion.getTransformerForLinearModel(spark, expansionStrucTypePath)
        Trainer.trainingModelAndWriteModelAndTestDfWithRawPrediction(spark, path, modelPath, resultPath, transformer)
    }
}
