package com.minhdd.cryptos.scryptosbt.segment.service

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.parquet.ParquetHelper
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper
import org.apache.spark.sql.{Dataset, SparkSession}

object ActualSegment {

    def getActualSegments(spark: SparkSession): Seq[Seq[BeforeSplit]] = {
        getActualSegments(SegmentHelper.getSmallSegments(spark))
    }

    def getActualSegments(smallSegments: Dataset[Seq[BeforeSplit]]): Seq[Seq[BeforeSplit]] = {
        val lastSegment: Seq[BeforeSplit] = smallSegments.collect().sortWith { case (x, y) => x.last.datetime.before(y.last.datetime) }.last
        val lastTimestamp: Timestamp = lastSegment.last.datetime
        println("last ts of small segments : " + lastTimestamp)

        getActualSegments(smallSegments.sparkSession, lastTimestamp)
    }

    def getActualSegments(spark: SparkSession, lastTimestamp: Timestamp): Seq[Seq[BeforeSplit]] = {
        val lastTsHelper: TimestampHelper = TimestampHelper(lastTimestamp.getTime)
        val lastCryptoPartitionKey = CryptoPartitionKey(
            asset = "XBT",
            currency = "EUR",
            provider = "KRAKEN",
            api = "TRADES",
            year = lastTsHelper.getYear,
            month = lastTsHelper.getMonth,
            day = lastTsHelper.getDay)
        val newTrades: Dataset[Crypto] = SegmentHelper.tradesFromLastSegment(spark, lastTimestamp, lastCryptoPartitionKey)
        val newOHLCs: Dataset[Crypto] = ParquetHelper().ohlcCryptoDs(spark).filter(x => !x.cryptoValue.datetime.before(lastTimestamp))

        val actualSegment: Seq[BeforeSplit] = SegmentHelper.toBeforeSplits(spark, newTrades, newOHLCs)
        Splitter.generalCut(Seq(actualSegment))
    }
}
