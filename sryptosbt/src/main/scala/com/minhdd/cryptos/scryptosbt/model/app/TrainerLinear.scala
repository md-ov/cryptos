package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.model.service.Expansion
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.SparkSession

//1 runfirst
object TrainerLinear {
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("training")
          .master("local[*]").getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        val path: String = s"$dataDirectory${pathDelimiter}segments${pathDelimiter}small${pathDelimiter}$smallSegmentsFolder"
        val modelPath = s"$dataDirectory${pathDelimiter}ml${pathDelimiter}linear-models${pathDelimiter}$numberOfMinutesBetweenTwoElement${pathDelimiter}${DateTimeHelper.now}"
        val resultPath = s"$dataDirectory${pathDelimiter}ml${pathDelimiter}linear-results${pathDelimiter}$numberOfMinutesBetweenTwoElement${pathDelimiter}${DateTimeHelper.now}"
    
        val expansionStrucTypePath: String = getClass.getResource("/expansion").getPath
        val transformer: Transformer = Expansion.getTransformerForLinearModel(spark, expansionStrucTypePath)
        Trainer.trainingModelAndWriteModelAndTestDfWithRawPrediction(spark, path, modelPath, resultPath, transformer)
    }
}
