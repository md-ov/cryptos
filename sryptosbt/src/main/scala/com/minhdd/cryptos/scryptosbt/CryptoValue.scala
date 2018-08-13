package com.minhdd.cryptos.scryptosbt

import java.sql.Timestamp

import org.joda.time.DateTime

case class CryptoValue
(
    processingDt: String,
    timestamp : Timestamp,
    value: Double,
    precision: Int,
    asset: String,
    currency: String,
    provider: String,
    prediction: Boolean
)

object CryptoValue {
    def apply(): CryptoValue = 
        new CryptoValue(
            processingDt = "", 
            timestamp = new Timestamp(DateTime.now().getMillis),
            value = 240.12,
            precision = 100,
            asset = "BTC",
            currency = "EUR",
            provider = "Minh",
            prediction = false)
}