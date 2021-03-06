package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey, CryptoValue, Margin}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, TimestampHelper}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class ExtractorTest extends FunSuite {
    
    val ss: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    
    test("testGetOneDayCryptoValue") {
        val key = CryptoPartitionKey(
            asset = "XLM",
            currency = "EUR",
            provider = "KRAKEN",
            api = "OHLC",
            year = "2018",
            month = "08",
            day = "29"
        )
        val ds: Dataset[Crypto] = 
            getPartitionFromPath(ss, "file://" + getClass.getResource("/parquets/parquet").getPath).get
            
        val extractedCrypto: Crypto = Extractor.oneDayCryptoValue(ss, "2018-08-29", ds, Seq(key))
    
        assert(extractedCrypto.partitionKey == key)
        assert(extractedCrypto.cryptoValue == CryptoValue(
            datetime = TimestampHelper.getTimestamp("2018-08-29", DateTimeHelper.defaultFormat),
            value = 0.19770672916666657,
            volume = 0,
            margin = Some(Margin(0.203491, 0.192397))
        ))
    }
    
}
