package com.minhdd.cryptos.scryptosbt.predict.ml

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object ml {
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
            StructField("size", IntegerType, nullable = false)
        )
    )
    
    val indexerBegin = new StringIndexer()
      .setInputCol("begin-evolution")
      .setOutputCol("begin-evo")
    
    val indexerEnd = new StringIndexer()
      .setInputCol("end-evolution")
      .setOutputCol("label")
    
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("begin-value", "begin-evo", "begin-variation", "begin-volume", "standard-deviation-volume", "size" ))
      .setOutputCol("features")
    
}
