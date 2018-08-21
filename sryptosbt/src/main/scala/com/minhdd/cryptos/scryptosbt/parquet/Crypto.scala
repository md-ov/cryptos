package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.tools.{Numbers, Timestamps}
import org.joda.time.DateTime

case class CryptoPartitionKey (
    asset: String,
    currency: String,
    provider: String,
    year: String,
    month: String,
    day: String                     
) {
    def getPartitionPath(parquetDir: String) = {
        ""
    }
}
case class CryptoValue (
    datetime : Timestamp,
    value: Double,
    volume: Double,
    count: Int
)
case class CryptoPrediction (
    prediction: Double,
    accuracy: Option[Double],
    predictionDt: Timestamp                       
)

object Crypto {
    def parseLine(line: String): Seq[Crypto] = {
        val splits: Array[String] = line.split(";")
        val asset: String = splits.apply(0)
        val currency: String = splits.apply(1)
        val provider: String = splits.apply(2)
        val timestampPosition = 4
        val value: String = splits.apply(timestampPosition+1)
        val volume: String = splits.apply(timestampPosition+6)
        val count: String = splits.apply(timestampPosition+7)
        val ts: Timestamps = Timestamps(splits.apply(timestampPosition).toLong*1000)
        Seq(
            new Crypto(
                partitionKey = new CryptoPartitionKey(
                    asset = asset.toUpperCase,
                    currency = currency.toUpperCase,
                    provider = provider.toUpperCase,
                    year = ts.getYearString, month = ts.getMonthString, day = ts.getDayString
                ),
                processingDt = new Timestamp(DateTime.now().getMillis),
                cryptoValue = new CryptoValue(
                    datetime = ts.timestamp,
                    value = Numbers.toDouble(value),
                    volume = Numbers.toDouble(volume),
                    count = count.toInt
                ),
                prediction = None
            )
        )
    }
}

case class Crypto
(
  partitionKey : CryptoPartitionKey,
  cryptoValue : CryptoValue,
  processingDt : Timestamp,
  prediction : Option[CryptoPrediction]
) {
    def flatten: FlattenCrypto = {
        import partitionKey._
        import cryptoValue._
        new FlattenCrypto(
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
            count = count,
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
   count: Int,
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