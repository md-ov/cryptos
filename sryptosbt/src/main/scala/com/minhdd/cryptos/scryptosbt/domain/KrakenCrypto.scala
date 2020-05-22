package com.minhdd.cryptos.scryptosbt.domain

import java.sql.Timestamp

import org.apache.spark.sql.{Encoder, SparkSession}

case class KrakenCrypto(datetime: Timestamp,
                         value: Double,
                         volume: Double,
                         count: Option[Int],
                         ohlcValue: Option[Double],
                         ohlcVolume: Option[Double])


