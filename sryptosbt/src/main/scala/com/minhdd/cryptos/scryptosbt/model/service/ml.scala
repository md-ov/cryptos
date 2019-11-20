package com.minhdd.cryptos.scryptosbt.model.service

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object ml {

    val indexerBegin = new StringIndexer()
      .setInputCol("beginEvolution")
      .setOutputCol("begin-evo")
      .setHandleInvalid("keep")
    
    val indexerEnd = new StringIndexer()
      .setInputCol("endEvolution")
      .setOutputCol("label")
      .setHandleInvalid("keep")
    
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array(
          "beginvalue", "begin-evo", "beginVariation", "beginVolume", "beginCount", 
          "ohlcBeginVolume", "beginderive", "beginsecondderive",
          "numberOfElement", "averageDerive", "standardDeviationDerive", "averageSecondDerive", "standardDeviationSecondDerive"
      ))
      .setOutputCol("features")
      .setHandleInvalid("keep")
    
    val csvSchema = StructType(
        List(
            StructField("t1", TimestampType, nullable = false),
            StructField("t2", TimestampType, nullable = false),
            StructField("begin-value", DoubleType, nullable = false),
            StructField("end-value", DoubleType, nullable = false),
            StructField("begin-evolution", StringType, nullable = true),
            StructField("begin-variation", DoubleType, nullable = false),
            StructField("begin-volume", DoubleType, nullable = false),
            StructField("end-evolution", StringType, nullable = true),
            StructField("end-variation", DoubleType, nullable = false),
            StructField("end-volume", DoubleType, nullable = false),
            StructField("standard-deviation-volume", DoubleType, nullable = false),
            StructField("same", BooleanType, nullable = false),
            StructField("size", IntegerType, nullable = false),
            StructField("average-volume", DoubleType, nullable = false),
            StructField("average-variation", DoubleType, nullable = false),
            StructField("standard-deviation-variation", DoubleType, nullable = false),
            StructField("average-derive", DoubleType, nullable = false),
            StructField("standard-deviation-derive", DoubleType, nullable = false),
            StructField("average-second-derive", DoubleType, nullable = false),
            StructField("standard-deviation-second-derive", DoubleType, nullable = false),
            StructField("average-count", DoubleType, nullable = false),
            StructField("standard-deviation-count", DoubleType, nullable = false),
            StructField("begin-count", DoubleType, nullable = false),
            StructField("ohlc-begin-volume", DoubleType, nullable = false),
            StructField("begin-derive", DoubleType, nullable = true),
            StructField("end-derive", DoubleType, nullable = true),
            StructField("begin-second-derive", DoubleType, nullable = true),
            StructField("end-second-derive", DoubleType, nullable = true)
        )
    )
    
    val prediction = "prediction"
    val label = "label"
    val predict = "predict"
    
    val minimumTruePositiveRate = 0.7
    val minimumPositiveRate = 0.1
}
