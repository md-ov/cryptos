package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey, CryptoValue, Margin}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, TimestampHelper}
import org.apache.spark.sql.{Dataset, SparkSession}

object Extractor {
    def oneDayCryptoValue(ss: SparkSession, d: String, ds: Dataset[Crypto], keys: Seq[CryptoPartitionKey]): Crypto = {
        import ss.implicits._
        val filteredDs: Dataset[Crypto] = ds.filter(c => keys.contains(c.partitionKey))
        val count: Long = filteredDs.count()
        val volumes: Dataset[Double] = filteredDs.map(_.cryptoValue.volume)
        val volume: Double = if (keys.exists(_.provider == "ohlc")) {
            val ohlcDs =  filteredDs.filter(_.partitionKey.provider.toLowerCase == "ohlc")
            val vols = ohlcDs.map(_.cryptoValue.volume).filter(_ > 0)
            val ohlcCount = vols.count
            if (ohlcCount > 0) vols.reduce(_ + _) / ohlcCount else 0
        } else 0
        val values: Dataset[Double] = filteredDs.map(_.cryptoValue.value).filter(_ > 0)
        val valuesCount = values.count()
        val averageValue: Double = if (valuesCount > 0) values.reduce(_+_) / valuesCount else 0
        val maxValue: Double = values.reduce(maxOfTwoDoubles(_, _))
        val minValue: Double = values.reduce(minOfTwoDoubles(_, _))
        Crypto(
            partitionKey = fusion(keys),
            cryptoValue = CryptoValue(
                datetime = TimestampHelper.getTimestamp(d, DateTimeHelper.defaultFormat),
                value = averageValue,
                volume = volume,
                margin = Some(Margin(maxValue, minValue))
            ),
            processingDt = TimestampHelper.now,
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

    private def fusion(keys: Seq[CryptoPartitionKey]): CryptoPartitionKey = {

        def getFusionValue(values: Seq[String]): String = {
            if (values.size == 1) values.head else values.mkString(":")
        }

        CryptoPartitionKey(
            asset = getFusionValue(keys.map(_.asset).distinct),
            currency = getFusionValue(keys.map(_.currency).distinct),
            provider = getFusionValue(keys.map(_.provider).distinct),
            api = getFusionValue(keys.map(_.api).distinct),
            year = getFusionValue(keys.map(_.year).distinct),
            month = getFusionValue(keys.map(_.month).distinct),
            day = getFusionValue(keys.map(_.day).distinct)
        )
    }
}
