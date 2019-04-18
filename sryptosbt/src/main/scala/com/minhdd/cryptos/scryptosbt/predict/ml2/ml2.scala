package com.minhdd.cryptos.scryptosbt.predict.ml2

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object ml2 {
    
    val indexerBegin = new StringIndexer()
      .setInputCol("beginEvolution")
      .setOutputCol("begin-evo")
    
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
    
}
