package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.tools.{DateTimes, Timestamps}
import org.apache.spark.sql.{Dataset, SparkSession}

object Extractor {
    def getOneDayCryptoValue(ss: SparkSession, ds: Dataset[Crypto], key: CryptoPartitionKey): Crypto = {
        import ss.implicits._
        val d: String = DateTimes.getDate(key.year, key.month, key.day)
        val filteredDs: Dataset[Crypto] = ds.filter(_.partitionKey == key)
        val count: Long = filteredDs.count()
        val volumes: Dataset[Double] = filteredDs.map(_.cryptoValue.volume)
        val maxVol: Double = volumes.reduce(maxOfTwoDoubles(_, _))
        val values: Dataset[Double] = filteredDs.map(_.cryptoValue.value)
        val averageValue: Double = values.reduce(_+_) / count
        val maxValue: Double = values.reduce(maxOfTwoDoubles(_, _))
        val minValue: Double = values.reduce(minOfTwoDoubles(_, _))
        Crypto(
            partitionKey = key,
            cryptoValue = CryptoValue(
                datetime = Timestamps.getTimestamp(d, DateTimes.defaultFormat),
                value = averageValue,
                volume = maxVol,
                margin = Some(Margin(maxValue, minValue))
            ),
            processingDt = Timestamps.now,
            count = None,
            tradeMode = None,
            prediction = None
        )
    }
    
    private def maxOfTwoDoubles(val1: Double, val2: Double) = {
        if (val1 > val2) val1 else val2
    }
    
    private def minOfTwoDoubles(val1: Double, val2: Double) = {
        if (val1 < val2) val1 else val2
    }
}
