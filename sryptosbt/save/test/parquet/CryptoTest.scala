package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey, CryptoValue}
import com.minhdd.cryptos.scryptosbt.domain.Crypto.encoder
import com.minhdd.cryptos.scryptosbt.tools.{FileHelper, TimestampHelper}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class CryptoTest extends FunSuite {
    
    test("testParseLine") {
        val line = "XLM;EUR;kraken;ohlc;2018-04-16T06:00:00GMT+0200;1523894400;0.226500;0.229800;0.220821;0.225699;" +
          "0.226591;1087016.31536331;584"
        val parsed: Crypto = Crypto.parseOHLC(line).apply(0)
        assert(parsed.cryptoValue == CryptoValue(
            value = 0.2265,
            volume = 1087016.31536331,
            datetime = new Timestamp(1523894400000L),
            margin = None
        ))
    
        assert(parsed.partitionKey == CryptoPartitionKey(
            asset = "XLM",
            currency = "EUR",
            provider = "KRAKEN",
            api = "OHLC",
            year = "2018", day = "16", month = "04"
        ))
    
    }
    
    test("getPartitionsUniFromPathFromLastTimestamp") {
        val ss: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "2g")
          .appName("test")
          .master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        
        val ds = Crypto.getPartitionsUniFromPathFromLastTimestamp(
            ss, "file:///", "src//test//resources//parquets", "parquets", "src//test//resources//parquets//parquet",
            TimestampHelper.getTimestamp("2019-04-29-10-30", "yyyy-MM-dd-hh-mm"),
            CryptoPartitionKey(
                asset = "XBT",
                currency = "EUR",
                provider = "KRAKEN",
                api = "TRADES",
                year = "2019",
                month = "04",
                day = "29")
        ).get
        assert(ds.count() == 35006)
        
    }
    
    test("a") {
        val s = "01".toInt
        assert(s == 1)
    }
    
    test("spark read") {
        val ss: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "2g")
          .config("spark.eventLog.dir", "d:\\ws\\spark-events")
          .config("spark.eventLog.enabled", "true")
          .appName("test")
          .master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        val c = ss.read.parquet(FileHelper.getPathForSpark
        ("parquets/XBT/EUR/TRADES/2019/04/29/KRAKEN/TRADES/parquet")).as[Crypto](encoder(ss))
        assert(c.count() == 18127)
        val d = ss.read.parquet(FileHelper.getPathForSpark
        ("parquets/XBT/EUR/TRADES/2019/04/30/KRAKEN/TRADES/parquet")).as[Crypto](encoder(ss))
        assert(d.count() == 19838)
    }
    
}
