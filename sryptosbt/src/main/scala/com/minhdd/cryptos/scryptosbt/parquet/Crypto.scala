package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.tools.{Numbers, Timestamps}
import org.joda.time.DateTime

case class CryptoPartitionKey (
    asset: String,
    currency: String,
    provider: String,
    api: String,
    year: String,
    month: String,
    day: String                     
) {
    def getPartitionPath(parquetDir: String) = {
        val separator = if (!parquetDir.contains("\\")) "/" else "\\"
        val fullParquetDir = if (parquetDir.endsWith(separator)) parquetDir else parquetDir + separator
        val path = fullParquetDir + 
          asset + separator + 
          currency + separator + 
          year + separator + month + separator + day + separator + 
          provider + separator +
          api + separator +
          "parquet" 
        path
    }
}

case class CryptoValue (
    datetime : Timestamp,
    value: Double,
    margin: Option[Margin],
    volume: Double                    
)

case class Margin (
    superior: Double,
    inferior: Double
)

case class CryptoPrediction (
    prediction: Double,
    accuracy: Option[Double],
    predictionDt: Timestamp                       
)

object Crypto {
    def parseOHLC(line: String): Seq[Crypto] = {
        val splits: Array[String] = line.split(";")
        val asset: String = splits.apply(0)
        val currency: String = splits.apply(1)
        val provider: String = splits.apply(2)
        val timestampPosition = 5
        val value: String = splits.apply(timestampPosition+1)
        val volume: String = splits.apply(timestampPosition+6)
        val count: String = splits.apply(timestampPosition+7)
        val ts: Timestamps = Timestamps(splits.apply(timestampPosition).toLong*1000)
        val partitionKey = CryptoPartitionKey(
            asset = asset.toUpperCase,
            currency = currency.toUpperCase,
            provider = provider.toUpperCase,
            api = "OHLC",
            year = ts.getYearString, month = ts.getMonthString, day = ts.getDayString
        )
        val processingDt: Timestamp = Timestamps.now
        val cryptoValue = CryptoValue(
            datetime = ts.timestamp,
            value = Numbers.toDouble(value),
            volume = Numbers.toDouble(volume),
            margin = None
        )
        Seq(Crypto(
            partitionKey = partitionKey, 
            cryptoValue = cryptoValue, 
            tradeMode = None,
            count = Some(count.toInt),
            processingDt = processingDt,
            prediction = None
        ))
    }
    def parseTrade(line: String): Seq[Crypto] = {
        val splits: Array[String] = line.split(";")
        val asset: String = splits.apply(0)
        val currency: String = splits.apply(1)
        val provider: String = splits.apply(2)
        val value: String = splits.apply(5)
        val volume: String = splits.apply(6)
        val ts: Timestamps = Timestamps((splits.apply(7).toDouble*1000).toLong)
        val tradeMode: String = splits.apply(8)
        val partitionKey = CryptoPartitionKey(
            asset = asset.toUpperCase,
            currency = currency.toUpperCase,
            provider = provider.toUpperCase,
            api = "TRADES",
            year = ts.getYearString, month = ts.getMonthString, day = ts.getDayString
        )
        val processingDt = Timestamps.now
        val cryptoValue = CryptoValue(
            datetime = ts.timestamp,
            value = Numbers.toDouble(value),
            volume = Numbers.toDouble(volume),
            margin = None
        )
        Seq(Crypto(
            partitionKey = partitionKey,
            cryptoValue = cryptoValue,
            tradeMode = Some(tradeMode),
            count = None,
            processingDt = processingDt,
            prediction = None
        ))
    }
}

case class Crypto
(
  partitionKey : CryptoPartitionKey,
  cryptoValue : CryptoValue,
  tradeMode : Option[String],
  count: Option[Int],
  processingDt : Timestamp,
  prediction : Option[CryptoPrediction]
) {
    def flatten: FlattenCrypto = {
        import partitionKey._
        import cryptoValue._
        FlattenCrypto(
            asset =  asset,
            currency = currency,
            provider = provider,
            year = year,
            month = month,
            day = day,
            processingDt = processingDt,
            datetime = datetime,
            value = value,
            volume = volume,
            prediction = prediction.map(_.prediction),
            accuracy = prediction.flatMap(_.accuracy),
            predictionDt = prediction.map(_.predictionDt)
        )
    }
}

case class FlattenCrypto (
   processingDt : Timestamp,
   asset: String,
   currency: String,
   provider: String,
   year: String,
   month: String,
   day: String,
   datetime : Timestamp,
   value: Double,
   volume: Double,
   prediction: Option[Double],
   accuracy: Option[Double],
   predictionDt: Option[Timestamp]
) {
    def toLine(): String =
        ("" /: this.getClass.getDeclaredFields) { (a, f) =>
            f.setAccessible(true)
            a + ";" + f.get(this)
        }.substring(1)
}

