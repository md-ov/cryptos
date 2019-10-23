package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.domain.{Analytics, AnalyticsCrypto, Crypto, CryptoPartitionKey, CryptoValue}
import com.minhdd.cryptos.scryptosbt.exploration.Explorates
import com.minhdd.cryptos.scryptosbt.parquet.{CryptoPartitionKey, CryptoValue, Margin}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, TimestampHelper}
import org.scalatest.FunSuite
import com.minhdd.cryptos.scryptosbt.constants.evolutionUp

class ExploratorTest extends FunSuite {
    test("splitAnalyticsCryptos") {
        val partitionKey = CryptoPartitionKey("", "", "","","","","")
        val cryptoValue1 = CryptoValue(
            datetime = TimestampHelper.getTimestamp("2013-08-28", DateTimeHelper.defaultFormat),
            value = 97,
            volume = 0,
            margin = None
        )
        val crypto = Crypto(
            partitionKey = partitionKey, 
            cryptoValue = cryptoValue1,
            tradeMode = None,
            count = None,
            processingDt = TimestampHelper.now,
            prediction = None)
        
        val analytics = Analytics(
            derive = None,
            secondDerive = None,
            numberOfStableDay = None,
            importantChange = None,
            variation = None,
            evolution = None)
        
        val analyticsCrypto1 = AnalyticsCrypto(crypto, analytics)
        val cryptoValue2 = cryptoValue1.copy(datetime = TimestampHelper.getTimestamp("2013-08-29", DateTimeHelper.defaultFormat))
        val analyticsCrypto2 = AnalyticsCrypto(crypto.copy(cryptoValue = cryptoValue2), analytics)
        val cryptoValue3 = cryptoValue1.copy(datetime = TimestampHelper.getTimestamp("2013-08-30", DateTimeHelper.defaultFormat))
        val analyticsCrypto3 = AnalyticsCrypto(crypto.copy(cryptoValue = cryptoValue3), analytics)
    
        val analyticsSeq2 = Analytics(
            derive = None,
            secondDerive = None,
            numberOfStableDay = None,
            importantChange = Option(true),
            variation = None,
            evolution = Option(evolutionUp))
        
        val cryptoValue1Seq2 = CryptoValue(
            datetime = TimestampHelper.getTimestamp("2013-08-31", DateTimeHelper.defaultFormat),
            value = 468,
            volume = 0,
            margin = None
        )
        
        val cryptoValue2Seq2 = CryptoValue(
            datetime = TimestampHelper.getTimestamp("2013-09-01", DateTimeHelper.defaultFormat),
            value = 468,
            volume = 0,
            margin = None
        )
    
        val cryptoValue3Seq2 = CryptoValue(
            datetime = TimestampHelper.getTimestamp("2013-09-02", DateTimeHelper.defaultFormat),
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
        
        val splitted = Explorates.splitAnalyticsCryptos(iterator).toSeq
        
        splitted.foreach(l => {
            println("------")
            l.foreach(a => println(a.crypto.cryptoValue))
        })
        
        assert(splitted.size == 2)
        
    }
}
