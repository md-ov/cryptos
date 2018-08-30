package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.parquet.PartitionsIntegrator.getPartitionFromPath
import com.minhdd.cryptos.scryptosbt.tools.Timestamps
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
        val extractedCrypto: Crypto = Extractor.getOneDayCryptoValue(ss, ds, key)
    
        assert(extractedCrypto.partitionKey == key)
        assert(extractedCrypto.cryptoValue == CryptoValue(
            datetime = Timestamps.getTimestamp("2018-08-29", "yyyy-MM-dd"),
            value = 0D,
            volume = 0D,
            margin = Some(Margin(1D, -1D))
        ))
    }
    
}
