package com.minhdd.cryptos.scryptosbt.domain

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants.evolutionNone
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
    
    def apply(krakenCrypto: KrakenCrypto): BeforeSplit = {
        BeforeSplit(
            datetime = krakenCrypto.datetime,
            value = krakenCrypto.value,
            evolution = evolutionNone,
            variation = 0D,
            derive = Some(0D),
            secondDerive = Some(0),
            ohlc_value = krakenCrypto.ohlcValue,
            ohlc_volume = krakenCrypto.ohlcVolume,
            volume = krakenCrypto.volume,
            count = krakenCrypto.count,
            importantChange = None)
    }
}