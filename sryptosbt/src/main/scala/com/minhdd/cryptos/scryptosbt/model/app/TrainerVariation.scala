package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.model.service.ml.label
import com.minhdd.cryptos.scryptosbt.model.service.{Expansion, ExpansionSegmentsTransformerForVariationModel}
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{abs, col}

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

        import spark.implicits._
        val ds = spark.read.parquet(segmentsPath)
        val expanded = Expansion.expansion(spark, ds.as[Seq[BeforeSplit]]).limit(1).withColumn(label, abs(col("endvalue") - col("beginvalue")))
        val transformer: ExpansionSegmentsTransformerForVariationModel = Expansion.getTransformerForVariationModel(spark, expanded.schema)
        val modelPath = s"$dataDirectory/ml/variation-models/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"
        val resultPath = s"$dataDirectory/ml/variation-results/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"

        TrainerRegression.train(spark, segmentsPath, modelPath, resultPath, transformer)
    }
}
