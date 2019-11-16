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
          .filter(!(col("beginEvolution") === evolutionNone))
          .withColumn("label",
              when(col("endEvolution") === evolutionUp, 1)
                .when(col("endEvolution") === evolutionDown, 0)
                .otherwise(-1))
    }
    
    override def copy(extra: ParamMap): ExpansionSegmentsTransformer = {
        this
    }
    
    override def transformSchema(schema: StructType): StructType = {
        //il faut retourner le schema de sortie ici
        transformedDataSchema
    }
    
    override val uid: String = UUID.randomUUID().toString()
}

object Expansion {
    
    def getTransformer(spark: SparkSession): ExpansionSegmentsTransformer = {
        new ExpansionSegmentsTransformer(spark, spark.read.parquet("file://" + getClass.getResource("/expansion").getPath).schema)
    }
    
    def expansion(ss: SparkSession, beforeSplitsSeqDataset: Dataset[Seq[BeforeSplit]]): DataFrame = {
        import ss.implicits._
        val expandedSegments: Dataset[Segment] = beforeSplitsSeqDataset.flatMap(Segment.segments)
        
        val segmentsDF: DataFrame =
            expandedSegments
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
              .select(
                  "begindt", "enddt", "beginvalue", "endvalue",
                  "beginEvolution", "beginVariation", "beginVolume",
                  "endEvolution", "endVariation", "endVolume",
                  "standardDeviationVolume",
                  "sameEvolution", "numberOfElement", "averageVolume",
                  "averageVariation", "standardDeviationVariation",
                  "averageDerive", "standardDeviationDerive",
                  "averageSecondDerive", "standardDeviationSecondDerive",
                  "averageCount", "standardDeviationCount",
                  "beginCount", "ohlcBeginVolume",
                  "beginderive", "endderive", "beginsecondderive", "endsecondderive"
              )
        segmentsDF
    }
}

