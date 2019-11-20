package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants.{dataDirectory, directoryNow, numberOfMinutesBetweenTwoElement}
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.segment.app.ActualSegment.getActualSegment
import com.minhdd.cryptos.scryptosbt.tools.ModelHelper
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, SparkSession}

object Predictor {
    
    def main(args: Array[String]): Unit = {
        predict()
    }
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    def getDataFrameFromSegments(segments: Seq[Seq[BeforeSplit]]): DataFrame = {
        import spark.implicits._
        spark.createDataset(segments).toDF()
//        spark.read.parquet(s"$dataDirectory\\segments\\small\\$numberOfMinutesBetweenTwoElement\\$directoryNow").limit(5)
    }
    
    def predict() = {
        def df: DataFrame = getDataFrameFromSegments(getActualSegment)
        df.show(5, false)
        val model: CrossValidatorModel = ModelHelper.getModel(spark, s"$dataDirectory\\ml\\models\\$numberOfMinutesBetweenTwoElement\\$directoryNow")
        val segmentsWithRawPrediction: DataFrame = model.transform(df)
        println(segmentsWithRawPrediction.count())
        segmentsWithRawPrediction.show(5, false)
    }
    
}
