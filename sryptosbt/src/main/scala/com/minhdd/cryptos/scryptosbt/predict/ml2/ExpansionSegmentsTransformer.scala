package com.minhdd.cryptos.scryptosbt.predict.ml2

import java.util.UUID

import com.minhdd.cryptos.scryptosbt.exploration.BeforeSplit
import com.minhdd.cryptos.scryptosbt.exploration.OHLCAndTradesExplorator.expansion
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class ExpansionSegmentsTransformer(spark: SparkSession, transformedDataSchema: StructType) extends Transformer{
    override def transform(ds: Dataset[_]): DataFrame = {
        import spark.implicits._
        expansion(spark, ds.as[Seq[BeforeSplit]])
          .filter(!(col("beginEvolution") === "-"))
          .withColumn("label",
              when(col("endEvolution") === "up", 1)
                .when(col("endEvolution") === "down", 0)
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

