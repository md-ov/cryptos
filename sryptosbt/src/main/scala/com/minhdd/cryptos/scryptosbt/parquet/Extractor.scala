package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.tools.{DateTimes, Timestamps}
import org.apache.spark.sql.{Dataset, SparkSession}

object Extractor {
    def getOneDayCryptoValue(ss: SparkSession, ds: Dataset[Crypto], key: CryptoPartitionKey): Crypto = {
        val d = DateTimes.getDate(key.year, key.month, key.day)
        Crypto(
            partitionKey = key,
            cryptoValue = CryptoValue(
                datetime = Timestamps.getTimestamp(d, "yyyy-MM-dd"),
                value = 0D,
                volume = 0D,
                margin = Some(Margin(1D, -1D))
            ),
            processingDt = Timestamps.now,
            count = None,
            tradeMode = None,
            prediction = None
        )
    }
}
