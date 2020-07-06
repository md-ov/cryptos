package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.model.service.ml.label
import com.minhdd.cryptos.scryptosbt.model.service.{Expansion, ExpansionSegmentsTransformer}
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

//1 runfirst
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
        import spark.implicits._
        val ds = spark.read.parquet(path)
        val expanded = Expansion.expansion(spark, ds.as[Seq[BeforeSplit]])
          .withColumn(label,
              when(col("evolutionDirection") === evolutionUp && col("isSegmentEnd") === true, 1)
                .when(col("evolutionDirection") === evolutionDown && col("isSegmentEnd") === true, 0)
                .otherwise(-1))
        
        val modelPath = s"$dataDirectory/ml/models/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"
        val resultPath = s"$dataDirectory/ml/results/$numberOfMinutesBetweenTwoElement/${DateTimeHelper.now}"
        
        val transformer: ExpansionSegmentsTransformer = Expansion.getTransformer(spark, expanded.schema)
        Trainer.trainingModelAndWriteModelAndTestDfWithRawPrediction(spark, path, modelPath, resultPath, transformer)
    }
}
