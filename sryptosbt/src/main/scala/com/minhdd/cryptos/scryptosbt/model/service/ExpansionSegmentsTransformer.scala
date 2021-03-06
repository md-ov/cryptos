package com.minhdd.cryptos.scryptosbt.model.service

import java.util.UUID

import com.minhdd.cryptos.scryptosbt.constants.{evolutionDown, evolutionNone, evolutionUp}
import ml.label
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
          .withColumn(label,
              when(col("evolutionDirection") === evolutionUp && col("isSegmentEnd") === true, 1)
                .when(col("evolutionDirection") === evolutionDown && col("isSegmentEnd") === true, 0)
                .otherwise(-1))
    }
    
    override def copy(extra: ParamMap): ExpansionSegmentsTransformer = this
    
    override def transformSchema(schema: StructType): StructType = {
        if (schema.fieldNames.contains(label)) {
            throw new IllegalArgumentException(s"Output column ${label} already exists.")
        }
        transformedDataSchema
    }
    
    override val uid: String = UUID.randomUUID().toString()
}