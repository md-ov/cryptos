package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.tools.Numbers
import org.joda.time.DateTime

case class CryptoValue
(
  asset: String,
  currency: String,
  provider: String,
  datetime : Timestamp,
  value: Double,
  volume: Double,
  count: Int,
  prediction: Option[Double],
  accuracy: Option[Double],
  processingDt: Timestamp,
  predictionDt: Timestamp
) {
    
    def toLine(): String =
        ("" /: this.getClass.getDeclaredFields) { (a, f) =>
            f.setAccessible(true)
            a + ";" + f.get(this)      
        }.substring(1)
    
}

object CryptoValue {
    def parseLine(line: String): Seq[CryptoValue] = {
        val splits: Array[String] = line.split(";")
        val asset: String = splits.apply(0)
        val currency: String = splits.apply(1)
        val provider: String = splits.apply(2)
        val timestampPosition = 4
        val timestamp: String = splits.apply(timestampPosition)
        val value: String = splits.apply(timestampPosition+1)
        val volume: String = splits.apply(timestampPosition+6)
        val count: String = splits.apply(timestampPosition+7)
        Seq(
            new CryptoValue(
                processingDt = new Timestamp(DateTime.now().getMillis),
                datetime = new Timestamp(timestamp.toLong*1000),
                value = Numbers.toDouble(value),
                volume = Numbers.toDouble(volume),
                count = count.toInt,
                accuracy = None,
                asset = asset.toUpperCase,
                currency = currency.toUpperCase,
                provider = provider.toUpperCase,
                prediction = None,
                predictionDt = new Timestamp(DateTime.now().getMillis))
        )
    }
}