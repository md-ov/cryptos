package com.minhdd.cryptos.scryptosbt.toparquet

import java.sql.Timestamp

import org.joda.time.DateTime

case class CryptoValue
(
    processingDt: Timestamp,
    timestamp : Timestamp,
    value: Double,
    accuracy: Double,
    asset: String,
    currency: String,
    provider: String,
    prediction: Boolean
)

object CryptoValue {
    def apply(): CryptoValue = 
        new CryptoValue(
            processingDt = new Timestamp(DateTime.now().getMillis), 
            timestamp = new Timestamp(DateTime.now().getMillis),
            value = 240.12,
            accuracy = 100.00,
            asset = "BTC",
            currency = "EUR",
            provider = "Minh",
            prediction = false)
    
    def parseLine(line: String): Seq[CryptoValue] = {
        val splits = line.split(";")
        val timestamp: String = splits.apply(6)
        val value = splits.apply(7)
        val accuracy = splits.apply(3)
        val asset = splits.apply(0)
        val provider = splits.apply(1)
        val currency = "EUR"
        val predictionValue = splits.apply(2)
        Seq(
            new CryptoValue(
                processingDt = new Timestamp(DateTime.now().getMillis),
                timestamp = new Timestamp(timestamp.toLong),
                value = value.toDouble,
                accuracy = 100.00,
                asset = asset.toUpperCase,
                currency = currency.toUpperCase,
                provider = provider.toUpperCase,
                prediction = false), 
            
            new CryptoValue(
                processingDt = new Timestamp(DateTime.now().getMillis),
                timestamp = new Timestamp(timestamp.toLong),
                value = predictionValue.toDouble,
                accuracy = accuracy.toDouble,
                asset = asset.toUpperCase,
                currency = currency.toUpperCase,
                provider = "PREDICTION",
                prediction = true)    
        )
    }
}