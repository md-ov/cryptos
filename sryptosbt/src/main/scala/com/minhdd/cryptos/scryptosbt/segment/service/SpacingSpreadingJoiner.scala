package com.minhdd.cryptos.scryptosbt.segment.service

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants.numberOfMinutesBetweenTwoElement
import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey, CryptoValue, KrakenCrypto}
import com.minhdd.cryptos.scryptosbt.segment.service.CryptoContainer.SeqCryptoContainerImplicit
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper.TimestampImplicit
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, lead, struct}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

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
    def toKrakenCrypto = KrakenCrypto(timestamp, value, volume, count, ohlcValue, ohlcVolume)
}

object CryptoContainer {
    def encoder(spark: SparkSession): Encoder[CryptoContainer] = {
        import spark.implicits._
        implicitly[Encoder[CryptoContainer]]
    }
    
    def groupByEncoder(spark: SparkSession): Encoder[(Timestamp, Seq[CryptoContainer])] = {
        import spark.implicits._
        implicitly[Encoder[(Timestamp, Seq[CryptoContainer])]]
    }
    
    def fromOHLCToCryptoContainer(timestamp: Timestamp, ohlc: CryptoContainer): KrakenCrypto = {
        KrakenCrypto(
            datetime = timestamp,
            value = ohlc.cryptoValue.value,
            volume = ohlc.cryptoValue.volume,
            count = ohlc.count,
            ohlcValue = Some(ohlc.cryptoValue.value),
            ohlcVolume = Some(ohlc.cryptoValue.volume)
        )
    }
    
    def fromTRADESToCryptoContainer(timestamp: Timestamp, trades: CryptoContainer): KrakenCrypto = {
        KrakenCrypto(
            datetime = timestamp,
            value = trades.cryptoValue.value,
            volume = trades.cryptoValue.volume,
            count = trades.count,
            ohlcValue = None,
            ohlcVolume = None
        )
    }
    
    implicit class SeqCryptoContainerImplicit(seq: Seq[CryptoContainer]) {
        def middleCryptoValue: CryptoValue = {
            val length = seq.length
            seq.map(_.cryptoValue).sortWith(_.value > _.value).apply(length / 2)
        }
    }
    
}

object CryptoSpreadContainer {
    def encoder(spark: SparkSession): Encoder[CryptoSpreadContainer] = {
        import spark.implicits._
        implicitly[Encoder[CryptoSpreadContainer]]
    }
}

object SpacingSpreadingJoiner {
    
    def join(spark: SparkSession, trades: Dataset[Crypto], ohlcs: Dataset[Crypto]): Dataset[KrakenCrypto] = {
        spread(space(trades, ohlcs))
    }
    
    private def spread(spacedKrakenCryptos: Dataset[KrakenCrypto]): Dataset[KrakenCrypto] = {
        val window = Window.orderBy("datetime")
        implicit val encoder: Encoder[KrakenCrypto] = KrakenCrypto.encoder(spacedKrakenCryptos.sparkSession)
        implicit val groupbyEncoder: Encoder[CryptoSpreadContainer] = CryptoSpreadContainer.encoder(spacedKrakenCryptos.sparkSession)
        
        val ds: Dataset[CryptoSpreadContainer] =
            spacedKrakenCryptos
              .withColumn("timestamp", col("datetime"))
              .withColumn("next", lead("datetime", 1).over(window))
              .as[CryptoSpreadContainer]
        
        ds.flatMap(fill)
    }
    
    private def fill(container: CryptoSpreadContainer): Seq[KrakenCrypto] = {
        if (container.next == null) Seq(container.toKrakenCrypto) else {
            val timestamps: Seq[Timestamp] = DateTimeHelper.getTimestamps(container.timestamp, container.next, numberOfMinutesBetweenTwoElement)
            timestamps.map(ts => container.copy(timestamp = ts).toKrakenCrypto)
        }
    }
    
    private def space(trades: Dataset[Crypto], ohlcs: Dataset[Crypto]): Dataset[KrakenCrypto] = {
        val spark: SparkSession = trades.sparkSession
    
        val containersTrades: Dataset[CryptoContainer] = toContainers(trades, "trades")
        val containersOhlcs: Dataset[CryptoContainer] = toContainers(ohlcs, "ohlc")
        
        val grouped: Dataset[(Timestamp, Seq[CryptoContainer])] =
            containersTrades
              .union(containersOhlcs)
              .withColumn("values", struct("*"))
              .groupBy("timestamp")
              .agg(collect_list("values"))
              .as[(Timestamp, Seq[CryptoContainer])](CryptoContainer.groupByEncoder(spark))
        
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
    
    private def toContainers(cryptos: Dataset[Crypto], api: String): Dataset[CryptoContainer] = {
        cryptos.map(crypto =>
            CryptoContainer(
                timestamp = crypto.cryptoValue.datetime.adjusted,
                api = api,
                partitionKey = crypto.partitionKey,
                cryptoValue = crypto.cryptoValue,
                tradeMode = crypto.tradeMode,
                count = crypto.count
            ))(CryptoContainer.encoder(cryptos.sparkSession))
    }
    
    private def oneKrakenCrypto(timestamp: Timestamp, containersTrades: Seq[CryptoContainer], 
      containersOhlcs: Seq[CryptoContainer]): KrakenCrypto = {
        
        if (containersTrades.isEmpty && containersOhlcs.length == 1) {
            CryptoContainer.fromOHLCToCryptoContainer(timestamp, containersOhlcs.head)
        } else if (containersTrades.length == 1 && containersOhlcs.isEmpty) {
            CryptoContainer.fromTRADESToCryptoContainer(timestamp, containersTrades.head)
        } else if (containersTrades.isEmpty) {
            val volumeTotal: Double = containersOhlcs.map(_.cryptoValue.volume).sum
            val countSum: Long = containersOhlcs.map(_.count.getOrElse(0)).sum
            val countAverage: Int = (countSum / containersOhlcs.length).toInt
            val cryptoValue: CryptoValue = containersOhlcs.middleCryptoValue
            
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
            val cryptoValue: CryptoValue = containersTrades.middleCryptoValue
            
            KrakenCrypto(
                datetime = timestamp,
                value = cryptoValue.value,
                volume = volumeTotal,
                count = None,
                ohlcValue = None,
                ohlcVolume = None
            )
        } else {
            val countSum: Long = containersOhlcs.map(_.count.getOrElse(0)).sum
            val countAverage = (countSum / containersOhlcs.length).toInt
            val volumeOhlcTotal: Double = containersOhlcs.map(_.cryptoValue.volume).sum
            val volumeTotal: Double = containersTrades.map(_.cryptoValue.volume).sum + volumeOhlcTotal
            val cryptoValue: CryptoValue = (containersTrades ++ containersOhlcs).middleCryptoValue
            val ohlcCryptoValue = containersOhlcs.middleCryptoValue
            
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
}
