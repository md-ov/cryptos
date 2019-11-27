package com.minhdd.cryptos.scryptosbt.model.service

import java.util.UUID

import com.minhdd.cryptos.scryptosbt.constants.{evolutionDown, evolutionNone, evolutionUp}
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Segment}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ExpansionSegmentsTransformer(spark: SparkSession, transformedDataSchema: StructType) extends Transformer {
    
    override def transform(ds: Dataset[_]): DataFrame = {
        import spark.implicits._
        Expansion.expansion(spark, ds.as[Seq[BeforeSplit]])
//          .filter(!(col("beginEvolution") === evolutionNone)) //TODO il faut pas car il y a beaucoup de evolutionNone
          .withColumn("label",
              when(col("evolutionDirection") === evolutionUp, 1)
                .when(col("evolutionDirection") === evolutionDown, 0)
                .otherwise(-1))
    }
    
    override def copy(extra: ParamMap): ExpansionSegmentsTransformer = this
    
    override def transformSchema(schema: StructType): StructType = transformedDataSchema
    
    override val uid: String = UUID.randomUUID().toString()
}

object Expansion {
    
    def getTransformer(spark: SparkSession, structTypeFilePath: String): ExpansionSegmentsTransformer = {
        new ExpansionSegmentsTransformer(spark, spark.read.parquet(structTypeFilePath).schema)
    }
    
    def expansion(ss: SparkSession, ds: Dataset[Seq[BeforeSplit]]): DataFrame = {
        import ss.implicits._
        val expandedSegments: Dataset[Segment] = ds.flatMap(Segment.segments)
        ml.toDataFrame(expandedSegments)
    }
}

