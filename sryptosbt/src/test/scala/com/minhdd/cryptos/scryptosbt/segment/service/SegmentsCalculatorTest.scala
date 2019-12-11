package com.minhdd.cryptos.scryptosbt.segment.service

import com.minhdd.cryptos.scryptosbt.constants.{evolutionDown, evolutionNone, evolutionUp}
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, KrakenCrypto}
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.{FunSuite, Matchers}

class SegmentsCalculatorTest extends FunSuite with Matchers {
    
    val spark: SparkSession = SparkSession.builder().appName("beforesplits").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val krakenCrypto1 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 02:15:00", "yyyy-MM-dd hh:mm:ss"),
        value = 10535.2D,
        volume = 100D,
        count = None,
        ohlcValue = Option(200.25D),
        ohlcVolume = Option(56.24D)
    )
    
    val krakenCrypto2 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 02:00:00", "yyyy-MM-dd hh:mm:ss"),
        value = 10608.7D,
        volume = 9800D,
        count = None,
        ohlcValue = Option(300.29D),
        ohlcVolume = Option(26.24D)
    )
    
    val krakenCrypto3 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 02:30:00", "yyyy-MM-dd hh:mm:ss"),
        value = 10533.6D,
        volume = 9800D,
        count = None,
        ohlcValue = Option(300.29D),
        ohlcVolume = Option(26.24D)
    )
    
    val krakenCrypto4 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 02:45:00", "yyyy-MM-dd hh:mm:ss"),
        value = 10490.5D,
        volume = 9800D,
        count = None,
        ohlcValue = Option(300.29D),
        ohlcVolume = Option(26.24D)
    )
    
    val krakenCrypto5 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 03:00:00", "yyyy-MM-dd hh:mm:ss"),
        value = 10476.1D,
        volume = 9800D,
        count = None,
        ohlcValue = Option(300.29D),
        ohlcVolume = Option(26.24D)
    )
    
    val krakenCrypto11 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 02:15:00", "yyyy-MM-dd hh:mm:ss"),
        value = 20535.2D,
        volume = 100D,
        count = None,
        ohlcValue = Option(200.25D),
        ohlcVolume = Option(56.24D)
    )
    
    val krakenCrypto1_10535 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 00:00:00", "yyyy-MM-dd hh:mm:ss"),
        value = 10535.2D,
        volume = 100D,
        count = None,
        ohlcValue = Option(200.25D),
        ohlcVolume = Option(56.24D)
    )
    
    val krakenCrypto2_20535 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 00:15:00", "yyyy-MM-dd hh:mm:ss"),
        value = 20535.2D,
        volume = 100D,
        count = None,
        ohlcValue = Option(200.25D),
        ohlcVolume = Option(56.24D)
    )
    
    val krakenCrypto3_20535 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 00:30:00", "yyyy-MM-dd hh:mm:ss"),
        value = 20535.2D,
        volume = 100D,
        count = None,
        ohlcValue = Option(200.25D),
        ohlcVolume = Option(56.24D)
    )
    
    val krakenCrypto3_535 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 00:30:00", "yyyy-MM-dd hh:mm:ss"),
        value = 535.2D,
        volume = 100D,
        count = None,
        ohlcValue = Option(200.25D),
        ohlcVolume = Option(56.24D)
    )
    
    val krakenCrypto4_40535 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 00:45:00", "yyyy-MM-dd hh:mm:ss"),
        value = 40535.2D,
        volume = 100D,
        count = None,
        ohlcValue = Option(200.25D),
        ohlcVolume = Option(56.24D)
    )
    
    val krakenCrypto5_10200 = KrakenCrypto(
        datetime = TimestampHelper.getTimestamp("2019-07-04 02:30:00", "yyyy-MM-dd hh:mm:ss"),
        value = 10200.2D,
        volume = 100D,
        count = None,
        ohlcValue = Option(200.25D),
        ohlcVolume = Option(56.24D)
    )
    
    test("five kraken cryptos") {
        val seqKrakenCrypto = Seq(krakenCrypto1, krakenCrypto2, krakenCrypto3, krakenCrypto4, krakenCrypto5)
        val seqBeforeSplit: Seq[BeforeSplit] = SegmentHelper.toBeforeSplits(seqKrakenCrypto)
        
        seqBeforeSplit should have length 5
        
        seqBeforeSplit.head should have(
            'derive (Some(-82.56666665413003)),
            'secondDerive (Some(44.48271602946816))
        )
        
        seqBeforeSplit.apply(1) should have(
            'derive (Some(-41.72222222114307)),
            'secondDerive (Some(32.07407406663621))
        )
        
        seqBeforeSplit.apply(2) should have(
            'derive (Some(-24.833333332691296)),
            'secondDerive (Some(5.432098765151156))
        )
        
        seqBeforeSplit.apply(3) should have(
            'derive (Some(-31.94444444361804)),
            'secondDerive (Some(5.40740740593949))
        )
        
        seqBeforeSplit.apply(4) should have(
            'derive (Some(-15.100000001748413)),
            'secondDerive (Some(19.61604938169875))
        )
    }
    
    test("two kraken cryptos") {
        val seqKrakenCrypto = Seq(krakenCrypto11, krakenCrypto2)
        val seqBeforeSplit: Seq[BeforeSplit] = SegmentHelper.toBeforeSplits(seqKrakenCrypto)
        
        seqBeforeSplit should have length 2
        
        seqBeforeSplit.head should have(
            'datetime (TimestampHelper.getTimestamp("2019-07-04 02:00:00", "yyyy-MM-dd hh:mm:ss")),
            'value (10608.7D),
            'derive (Some(11028.54444273231)),
            'secondDerive (Some(1.0999999998603016)),
            'variation (9926.5D),
            'evolution (evolutionNone)
        )
        
        seqBeforeSplit.apply(1) should have(
            'datetime (TimestampHelper.getTimestamp("2019-07-04 02:15:00", "yyyy-MM-dd hh:mm:ss")),
            'value (20535.2D),
            'derive (Some(11030.344442732589)),
            'secondDerive (Some(2.9000000001396984)),
            'variation (9926.5D),
            'evolution (evolutionUp)
        )
    }
    
    test("kraken cryptos") {
        
        val seqKrakenCrypto = Seq(krakenCrypto1_10535, krakenCrypto2_20535, krakenCrypto3_20535, krakenCrypto4_40535,
            krakenCrypto5_10200)
        
        val seqBeforeSplit: Seq[BeforeSplit] = SegmentHelper.toBeforeSplits(seqKrakenCrypto)
        
        assert(seqBeforeSplit.map(_.importantChange.get) == Seq(false, false, false, true, true))
        assert(seqBeforeSplit.map(_.evolution) == Seq(evolutionNone, evolutionNone, evolutionNone, evolutionUp, evolutionDown))
        
        val ds: Dataset[BeforeSplit] = spark.createDataset(seqBeforeSplit)(BeforeSplit.encoder(spark))
        ds.show(false)
    }
    
    test("kraken cryptos 2") {
        
        val seqKrakenCrypto = Seq(krakenCrypto1_10535, krakenCrypto2_20535, krakenCrypto3_535, krakenCrypto4_40535, krakenCrypto5_10200)
        
        val seqBeforeSplit: Seq[BeforeSplit] = SegmentHelper.toBeforeSplits(seqKrakenCrypto)
        
        val ds: Dataset[BeforeSplit] = spark.createDataset(seqBeforeSplit)(BeforeSplit.encoder(spark))
        ds.show(false)
        
        assert(seqBeforeSplit.map(_.importantChange.get) == Seq(false, false, true, true, false))
        assert(seqBeforeSplit.map(_.evolution) == Seq(evolutionNone, evolutionNone, evolutionDown, evolutionUp, evolutionNone))
    }
    
    test("getds") {
        val expansionStrucTypePath = "file://" + getClass.getResource("/expansion").getPath
        val df: DataFrame = spark.read.parquet(expansionStrucTypePath)
        df.show(4, false)
    }
}
