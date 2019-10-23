package com.minhdd.cryptos.scryptosbt.domain

import java.sql.Timestamp

import org.apache.spark.sql.{Encoder, SparkSession}

case class BeforeSplit(
    datetime: Timestamp,
    value: Double,
    evolution: String,
    variation: Double,
    derive: Option[Double],
    secondDerive: Option[Double],
    ohlc_value: Option[Double],
    ohlc_volume: Option[Double],
    volume: Double,
    count: Option[Int],
    importantChange: Option[Boolean]
)

object BeforeSplit {
    
    def encoder(spark: SparkSession): Encoder[BeforeSplit] = {
        import spark.implicits._
        implicitly[Encoder[BeforeSplit]]
    }
    
    def encoderSeq(spark: SparkSession): Encoder[Seq[BeforeSplit]] = {
        import spark.implicits._
        implicitly[Encoder[Seq[BeforeSplit]]]
    }
}