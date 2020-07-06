package com.minhdd.cryptos.scryptosbt.model.service

import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Segment}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Expansion {
    def getTransformer(spark: SparkSession, schema: StructType): ExpansionSegmentsTransformer = {
        new ExpansionSegmentsTransformer(spark, schema)
    }

    def getTransformerForVariationModel(spark: SparkSession, schema: StructType): ExpansionSegmentsTransformerForVariationModel = {
        new ExpansionSegmentsTransformerForVariationModel(spark, schema)
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
