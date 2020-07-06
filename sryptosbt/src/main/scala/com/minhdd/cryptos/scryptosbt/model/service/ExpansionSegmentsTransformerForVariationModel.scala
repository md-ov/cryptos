package com.minhdd.cryptos.scryptosbt.model.service

import java.util.UUID

import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.model.service.ml.label
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.{col, when, abs}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ExpansionSegmentsTransformerForVariationModel(spark: SparkSession, transformedDataSchema: StructType) extends Transformer {
    
    override def transform(ds: Dataset[_]): DataFrame = {
        import spark.implicits._
        Expansion.expansion(spark, ds.as[Seq[BeforeSplit]]).withColumn(label, abs(col("endvalue") - col("beginvalue")))
    }
    
    override def copy(extra: ParamMap): ExpansionSegmentsTransformerForVariationModel = this
    
    override def transformSchema(schema: StructType): StructType = {
        if (schema.fieldNames.contains(label)) {
            throw new IllegalArgumentException(s"Output column ${label} already exists.")
        }
        transformedDataSchema
    }
    
    override val uid: String = UUID.randomUUID().toString()
}
