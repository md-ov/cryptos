package com.minhdd.cryptos.scryptosbt.service

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.domain.KrakenCrypto
import com.minhdd.cryptos.scryptosbt.exploration.SamplerObj.adjustSecond
import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey, CryptoValue}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimes, Timestamps}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.joda.time.DateTime
import org.apache.spark.sql.functions.{collect_list, lead, struct, col}

case class CryptoContainer(
    timestamp: Timestamp,
    api: String,
    partitionKey: CryptoPartitionKey,
    cryptoValue: CryptoValue,
    tradeMode: Option[String],
    count: Option[Int]
)

case class CryptoSpreadContainer(
  timestamp: Timestamp,
  next: Timestamp,
  value: Double,
  volume: Double,
  count: Option[Int],
  ohlcValue: Option[Double],
  ohlcVolume: Option[Double]
) {
    def toKrakenCrypto = KrakenCrypto(timestamp, value, volume, count, ohlcValue, ohlcVolume )
}

object CryptoContainer {
    def encoder(spark: SparkSession): Encoder[CryptoContainer] = {
        import spark.implicits._
        implicitly[Encoder[CryptoContainer]]
    }
    
    def encoderForGroup(spark: SparkSession): Encoder[(Timestamp, Seq[CryptoContainer])] = {
        import spark.implicits._
        implicitly[Encoder[(Timestamp, Seq[CryptoContainer])]]
    }
}

object CryptoSpreadContainer {
    def encoder(spark: SparkSession): Encoder[CryptoSpreadContainer] = {
        import spark.implicits._
        implicitly[Encoder[CryptoSpreadContainer]]
    }
    

}

object SpacingSpreadingJoiner {
    
    def getAdjustedDatetime(numberOfMinutesBetweenTwoElement: Int)(dateTime: DateTime): DateTime = {
        val minutes = dateTime.getMinuteOfHour
        val delta: Int = minutes % numberOfMinutesBetweenTwoElement
        dateTime.minusMinutes(delta)
    }
    
    def oneKrakenCrypto(timestamp: Timestamp, containersTrades: Seq[CryptoContainer], containersOhlcs: Seq[CryptoContainer]): KrakenCrypto = {
        
        if (containersTrades.isEmpty && containersOhlcs.length == 1) {
            val firstOhlc = containersOhlcs.head
            KrakenCrypto(
                datetime = timestamp,
                value = firstOhlc.cryptoValue.value,
                volume = firstOhlc.cryptoValue.volume,
                count = firstOhlc.count,
                ohlcValue = Some(firstOhlc.cryptoValue.value),
                ohlcVolume = Some(firstOhlc.cryptoValue.volume)
            )
        } else if (containersTrades.length == 1 && containersOhlcs.isEmpty) {
            val firstTrades = containersTrades.head
            KrakenCrypto(
                datetime = timestamp,
                value = firstTrades.cryptoValue.value,
                volume = firstTrades.cryptoValue.volume,
                count = firstTrades.count,
                ohlcValue = None,
                ohlcVolume = None
            )
        } else if (containersTrades.isEmpty) {
            val volumeTotal: Double = containersOhlcs.map(_.cryptoValue.volume).sum
            val length = containersOhlcs.length
            val countSum: Long = containersOhlcs.map(_.count.getOrElse(0)).sum
            val countAverage = (countSum/length).toInt
            val cryptoValue: CryptoValue = middleCryptoValue(containersOhlcs, length)
            KrakenCrypto(
                datetime = timestamp,
                value = cryptoValue.value,
                volume = volumeTotal,
                count = Option(countAverage),
                ohlcValue = Option(cryptoValue.value),
                ohlcVolume = Option(volumeTotal)
            )
        } else if (containersOhlcs.isEmpty) {
            val length = containersTrades.length
            val volumeTotal: Double = containersTrades.map(_.cryptoValue.volume).sum
            val cryptoValue: CryptoValue = middleCryptoValue(containersTrades, length)
            KrakenCrypto(
                datetime = timestamp,
                value = cryptoValue.value,
                volume = volumeTotal,
                count = None,
                ohlcValue = None,
                ohlcVolume = None
            )
        } else {
            val lengthOhlc = containersOhlcs.length
            val lengthTrades = containersTrades.length
            val countSum: Long = containersOhlcs.map(_.count.getOrElse(0)).sum
            val countAverage = (countSum/lengthOhlc).toInt
            val volumeOhlcTotal : Double = containersOhlcs.map(_.cryptoValue.volume).sum
            val volumeTotal: Double = containersTrades.map(_.cryptoValue.volume).sum + volumeOhlcTotal
            val cryptoValue: CryptoValue = middleCryptoValue(containersTrades ++ containersOhlcs, lengthOhlc + lengthTrades)
            val ohlcCryptoValue = middleCryptoValue(containersOhlcs, lengthOhlc)
            KrakenCrypto(
                datetime = timestamp,
                value = cryptoValue.value,
                volume = volumeTotal,
                count = Option(countAverage),
                ohlcValue = Option(ohlcCryptoValue.value),
                ohlcVolume = Option(volumeTotal)
            )
        }
    }
    
    private def middleCryptoValue(seq: Seq[CryptoContainer], length: Int): CryptoValue = {
        seq.map(_.cryptoValue).sortWith(_.value > _.value).apply(length / 2)
    }
    
    def fill(cryptoSpreadContainer: CryptoSpreadContainer, numberOfMinutesBetweenTwoElement: Int): Seq[KrakenCrypto] = {
        if (cryptoSpreadContainer.next == null) Seq(cryptoSpreadContainer.toKrakenCrypto) else {
            val tss: Seq[Timestamp] = 
                DateTimes.getTimestamps(
                    cryptoSpreadContainer.timestamp, 
                    cryptoSpreadContainer.next, 
                numberOfMinutesBetweenTwoElement)
            tss.map(ts => cryptoSpreadContainer.copy(timestamp = ts).toKrakenCrypto)
        }
    }
    
    def spread(spark: SparkSession, spacedKrakenCryptos: Dataset[KrakenCrypto], numberOfMinutesBetweenTwoElement: Int): Dataset[KrakenCrypto] = {
        val window = Window.orderBy("datetime")
        val cryptoSpreadContainers: Dataset[CryptoSpreadContainer] =
            spacedKrakenCryptos
                .withColumn("timestamp", col("datetime"))
              .withColumn("next", lead("datetime", 1).over(window))
              .as[CryptoSpreadContainer](CryptoSpreadContainer.encoder(spark))
    
        cryptoSpreadContainers.flatMap(fill(_, numberOfMinutesBetweenTwoElement))(KrakenCrypto.encoder(spark))
    }
    
    def join(spark: SparkSession, trades: Dataset[Crypto], ohlcs: Dataset[Crypto],
             numberOfMinutesBetweenTwoElement: Int = 15): Dataset[KrakenCrypto] = {
        val spacedKrakenCryptos: Dataset[KrakenCrypto] = space(spark, trades, ohlcs, numberOfMinutesBetweenTwoElement)
        spread(spark, spacedKrakenCryptos, numberOfMinutesBetweenTwoElement)
    }
    
    def space(spark: SparkSession, trades: Dataset[Crypto], ohlcs: Dataset[Crypto],
              numberOfMinutesBetweenTwoElement: Int = 15): Dataset[KrakenCrypto] = {
        val adjustDatetime: DateTime => DateTime = getAdjustedDatetime(numberOfMinutesBetweenTwoElement)
        
        def adjustTimestamp(ts: Timestamp): DateTime = adjustSecond(adjustDatetime(DateTimes.fromTimestamp(ts)))
        
        def toContainers(cryptos: Dataset[Crypto], api: String): Dataset[CryptoContainer] = {
            cryptos.map(crypto =>
                CryptoContainer(
                    timestamp = Timestamps.fromDatetime(adjustTimestamp(crypto.cryptoValue.datetime)),
                    api = api,
                    partitionKey = crypto.partitionKey,
                    cryptoValue = crypto.cryptoValue,
                    tradeMode = crypto.tradeMode,
                    count = crypto.count
                ))(CryptoContainer.encoder(spark))
        }
        
        val containersTrades: Dataset[CryptoContainer] = toContainers(trades, "trades")
        val containersOhlcs: Dataset[CryptoContainer] = toContainers(ohlcs, "ohlc")
        
        val grouped: Dataset[(Timestamp, Seq[CryptoContainer])] = containersTrades
          .union(containersOhlcs)
          .withColumn("values", struct("*"))
          .groupBy("timestamp")
          .agg(collect_list("values"))
          .as[(Timestamp, Seq[CryptoContainer])](CryptoContainer.encoderForGroup(spark))
        
        //        grouped.filter(x => x._2.exists(_.api == "ohlc")).take(10).foreach(x => {
        //              val containersTrades: Seq[CryptoContainer] = x._2.filter(_.api == "trades")
        //              containersTrades.foreach(println)
        //              val containersOhlcs: Seq[CryptoContainer] = x._2.filter(_.api == "ohlc")
        //              containersOhlcs.foreach(println)
        //            println("-----")
        //          })
        
        val spacedKrakenCryptos: Dataset[KrakenCrypto] = grouped.map(x => {
            val containersTrades: Seq[CryptoContainer] = x._2.filter(_.api == "trades")
            val containersOhlcs: Seq[CryptoContainer] = x._2.filter(_.api == "ohlc")
            oneKrakenCrypto(x._1, containersTrades, containersOhlcs)
        })(KrakenCrypto.encoder(spark))
        
        spacedKrakenCryptos
    }
    
}
