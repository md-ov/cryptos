package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey, CryptoValue, Margin}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimes, Timestamps}
import org.scalatest.FunSuite

class ExplorateOHLCTest extends FunSuite {
    test("splitAnalyticsCryptos") {
        val partitionKey = CryptoPartitionKey("", "", "","","","","")
        val cryptoValue1 = CryptoValue(
            datetime = Timestamps.getTimestamp("2013-08-28", DateTimes.defaultFormat),
            value = 97,
            volume = 0,
            margin = None
        )
        val crypto = Crypto(
            partitionKey = partitionKey, 
            cryptoValue = cryptoValue1,
            tradeMode = None,
            count = None,
            processingDt = Timestamps.now,
            prediction = None)
        
        val analytics = Analytics(
            derive = None,
            secondDerive = None,
            numberOfStableDay = None,
            importantChange = None,
            variation = None,
            evolution = None)
        
        val analyticsCrypto1 = AnalyticsCrypto(crypto, analytics)
        val cryptoValue2 = cryptoValue1.copy(datetime = Timestamps.getTimestamp("2013-08-29", DateTimes.defaultFormat))
        val analyticsCrypto2 = AnalyticsCrypto(crypto.copy(cryptoValue = cryptoValue2), analytics)
        val cryptoValue3 = cryptoValue1.copy(datetime = Timestamps.getTimestamp("2013-08-30", DateTimes.defaultFormat))
        val analyticsCrypto3 = AnalyticsCrypto(crypto.copy(cryptoValue = cryptoValue3), analytics)
    
        val analyticsSeq2 = Analytics(
            derive = None,
            secondDerive = None,
            numberOfStableDay = None,
            importantChange = Option(true),
            variation = None,
            evolution = Option("up"))
        
        val cryptoValue1Seq2 = CryptoValue(
            datetime = Timestamps.getTimestamp("2013-08-31", DateTimes.defaultFormat),
            value = 468,
            volume = 0,
            margin = None
        )
        
        val cryptoValue2Seq2 = CryptoValue(
            datetime = Timestamps.getTimestamp("2013-09-01", DateTimes.defaultFormat),
            value = 468,
            volume = 0,
            margin = None
        )
    
        val cryptoValue3Seq2 = CryptoValue(
            datetime = Timestamps.getTimestamp("2013-09-02", DateTimes.defaultFormat),
            value = 468,
            volume = 0,
            margin = None
        )
    
        val analyticsCrypto1Seq2 = 
            AnalyticsCrypto(crypto = crypto.copy(cryptoValue = cryptoValue1Seq2), analytics = analyticsSeq2)
        val analyticsCrypto2Seq2 = 
            AnalyticsCrypto(crypto = crypto.copy(cryptoValue = cryptoValue2Seq2), analytics = analyticsSeq2)
        val analyticsCrypto3Seq2 = 
            AnalyticsCrypto(crypto = crypto.copy(cryptoValue = cryptoValue3Seq2), analytics = analyticsSeq2)
        
        val iterator = Iterator(
            analyticsCrypto1, analyticsCrypto2, analyticsCrypto3,
          analyticsCrypto1Seq2, analyticsCrypto2Seq2, analyticsCrypto3Seq2)
        
        val splitted = ExplorateOHLC.splitAnalyticsCryptos(iterator).toSeq
        
        splitted.foreach(l => {
            println("------")
            l.foreach(a => println(a.crypto.cryptoValue))
        })
        
        assert(splitted.size == 2)
        
    }
}
