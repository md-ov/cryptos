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
  prediction: Double,
  accuracy: Double,
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
    def apply(): CryptoValue = 
        new CryptoValue(
            processingDt = new Timestamp(DateTime.now().getMillis), 
            datetime = new Timestamp(DateTime.now().getMillis),
            value = 240.12,
            accuracy = 100.00,
            asset = "BTC",
            currency = "EUR",
            provider = "Minh",
            prediction = 240.12,
            predictionDt = new Timestamp(DateTime.now().getMillis))
    
    
    
    def parseLine(line: String): Seq[CryptoValue] = {
        val splits = line.split(";")
        val timestamp: String = splits.apply(5)
        val value = splits.apply(6)
        val accuracy = splits.apply(3)
        val asset = splits.apply(0)
        val provider = splits.apply(1)
        val currency = "EUR"
        val predictionValue = splits.apply(2)
        Seq(
            new CryptoValue(
                processingDt = new Timestamp(DateTime.now().getMillis),
                datetime = new Timestamp(timestamp.toLong*1000),
                value = Numbers.toDouble(value),
                accuracy = Numbers.toDouble(accuracy),
                asset = asset.toUpperCase,
                currency = currency.toUpperCase,
                provider = provider.toUpperCase,
                prediction = Numbers.toDouble(predictionValue),
                predictionDt = new Timestamp(DateTime.now().getMillis))
        )
    }
}