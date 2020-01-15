package com.minhdd.cryptos.scryptosbt.model.service

import com.minhdd.cryptos.scryptosbt.domain.Segment
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object ml {

    val indexerBegin = new StringIndexer()
      .setInputCol("beginEvolution")
      .setOutputCol("begin-evo")
      .setHandleInvalid("keep")
    
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array(
          "beginvalue", "begin-evo", "beginVariation", "beginVolume", "beginCount", 
          "ohlcBeginVolume", "beginderive", "beginsecondderive",
          "numberOfElement", "averageDerive", "standardDeviationDerive", "averageSecondDerive", "standardDeviationSecondDerive"
      ))
      .setOutputCol("features")
      .setHandleInvalid("keep")
    
    def toDataFrame(ds: Dataset[Segment]): DataFrame = {
        ds
          .withColumn("begindt", col("begin.datetime"))
          .withColumn("enddt", col("end.datetime"))
          .withColumn("beginvalue", col("begin.value"))
          .withColumn("endvalue", col("end.value"))
          .withColumn("beginderive", col("begin.derive"))
          .withColumn("endderive", col("end.derive"))
          .withColumn("beginsecondderive", col("begin.secondDerive"))
          .withColumn("endsecondderive", col("end.secondDerive"))
          .withColumn("beginEvolution", col("begin.evolution"))
          .withColumn("endEvolution", col("end.evolution"))
          .withColumn("beginVariation", col("begin.variation"))
          .withColumn("endVariation", col("end.variation"))
          .withColumn("beginVolume", col("begin.volume"))
          .withColumn("endVolume", col("end.volume"))
          .withColumn("beginCount", col("begin.count"))
          .withColumn("ohlcBeginVolume", col("begin.ohlc_volume"))
          .withColumn("isSegmentEnd", col("isSegmentEnd"))
          .select(
              "begindt", "enddt", "variation", "linear", "isSegmentEnd", "evolutionDirection", "beginvalue", "endvalue",
              "beginEvolution", "beginVariation", "beginVolume",
              "endEvolution", "endVariation", "endVolume",
              "standardDeviationVolume", "numberOfElement", 
              "averageVolume", "averageVariation", "standardDeviationVariation",
              "averageDerive", "standardDeviationDerive", "averageSecondDerive", "standardDeviationSecondDerive",
              "averageCount", "standardDeviationCount",
              "beginCount", "ohlcBeginVolume",
              "beginderive", "endderive", "beginsecondderive", "endsecondderive"
          )
    }
    
    val prediction = "prediction"
    val label = "label"
    val predict = "predict"
    
    val minimumTruePositiveRate = 0.8
    val minimumPositiveRate = 0.4
    
    val minimumTrueNegativeRate = 0.9
    val minimumNegativeRate = 0.34
}
