package com.minhdd.cryptos.scryptosbt.model.service

import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Segment}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Expansion {
    def getTransformer(spark: SparkSession, structTypeFilePath: String): ExpansionSegmentsTransformer = {
        new ExpansionSegmentsTransformer(spark, spark.read.parquet(structTypeFilePath).schema)
    }

    def getTransformerForVariationModel(spark: SparkSession, structTypeFilePath: String): ExpansionSegmentsTransformerForVariationModel = {
        new ExpansionSegmentsTransformerForVariationModel(spark, spark.read.parquet(structTypeFilePath).schema)
    }

    def expansion(ss: SparkSession, ds: Dataset[Seq[BeforeSplit]]): DataFrame = {
        import ss.implicits._
        val expandedSegments: Dataset[Segment] = ds.flatMap(Segment.segments)
                println("expandedSegments")
                expandedSegments.show(2, false)
                println("----------------")
        
        val df = ml.toDataFrame(expandedSegments)
                println("dataframe")
                df.show(2, false)
                println("----------------")
        
        df
    }
}
