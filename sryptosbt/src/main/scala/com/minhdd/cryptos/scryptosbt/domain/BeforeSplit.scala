package com.minhdd.cryptos.scryptosbt.domain

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants
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
                        importantChange: Option[Boolean],
                        isEndOfSegment: Boolean
                      ){
    def toLine = {
        ("" /: this.getClass.getDeclaredFields) { (a, f) =>
            f.setAccessible(true)
            a + ";" + f.get(this)
        }.substring(1)
    }
}

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
            importantChange = None,
            isEndOfSegment = false)
    }
    
    def apply(timestamp: Timestamp, value: Double): BeforeSplit = {
        BeforeSplit(
            datetime = timestamp,
            value = value,
            evolution = constants.evolutionNone,
            variation = 0D,
            derive = None,
            secondDerive = None,
            ohlc_value = None,
            ohlc_volume = None,
            volume = 0D,
            count = None,
            importantChange = None,
            isEndOfSegment = false)
    }
}