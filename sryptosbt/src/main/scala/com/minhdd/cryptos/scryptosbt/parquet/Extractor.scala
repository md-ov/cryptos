package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.tools.Timestamps
import org.apache.spark.sql.SparkSession

object Extractor {
    def getOneDayCryptoValue(ss: SparkSession, d: String, parquetPath: String, key: CryptoPartitionKey): Crypto = {
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
