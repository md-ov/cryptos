package com.minhdd.cryptos.scryptosbt.exploration

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StructField, StructType}

class CustomSum extends UserDefinedAggregateFunction {
    override def inputSchema: org.apache.spark.sql.types.StructType =
        StructType(StructField("toMark", BooleanType) :: Nil)
    override def bufferSchema: StructType = StructType(
        StructField("value", LongType) :: StructField("counter", LongType) :: Nil
    )
    override def dataType: DataType = LongType
    override def deterministic: Boolean = true
    
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L // la valeur de sortie
        buffer(1) = 0L // le compteur
    }
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (buffer(1) != buffer(0)) {
            buffer(0) = 0L
            buffer(1) = 0L
        }
        buffer(1) = buffer.getLong(1) + 1
        if (input.getAs[Boolean](0)) {
            
        } else {
            buffer(0) = buffer.getLong(0) + 1
        }
    }
    
    override def merge(buffer: MutableAggregationBuffer, buffer2: Row): Unit = {
        println("merge buffer " + buffer(0))
        println("merge row" + buffer.getAs[Long](0))
    }
    override def evaluate(buffer: Row): Any = {
        buffer.getLong(0)
    }
}