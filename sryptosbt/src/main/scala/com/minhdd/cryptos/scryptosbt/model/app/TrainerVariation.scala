package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.model.service.{Expansion, ExpansionSegmentsTransformer, ExpansionSegmentsTransformerForVariationModel}
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper
import org.apache.spark.sql.SparkSession

//1 runfirst
object TrainerVariation {
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("training")
          .master("local[*]").getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        val segmentsPath: String = s"$dataDirectory/segments/small/$smallSegmentsFolder"
        val expansionStrucTypePath: String = this.getClass.getResource("/expansion").getPath
        val transformer: ExpansionSegmentsTransformerForVariationModel = Expansion.getTransformerForVariationModel(spark, expansionStrucTypePath)
        val modelPath = s"$dataDirectory/ml/variation-models/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"
        val resultPath = s"$dataDirectory/ml/variation-results/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"

        TrainerRegression.train(spark, segmentsPath, modelPath, resultPath, transformer)
    }
}
