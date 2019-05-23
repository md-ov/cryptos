package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.parquet.Crypto.encoder
import com.minhdd.cryptos.scryptosbt.tools.{Files, Timestamps}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class CryptoTest extends FunSuite {
    
    test("testParseLine") {
        val line = "XLM;EUR;kraken;ohlc;2018-04-16T06:00:00GMT+0200;1523894400;0.226500;0.229800;0.220821;0.225699;" +
          "0.226591;1087016.31536331;584"
        val parsed = Crypto.parseOHLC(line).apply(0)
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
        
        val parquetPath: String = CryptoPartitionKey.getTRADESParquetPath(
            parquetsDir = "src//test//resources//parquets", asset = "XBT", currency = "EUR")
        
        val ds = Crypto.getPartitionsUniFromPathFromLastTimestamp(
            ss, "file:///", parquetPath, 
            Timestamps.getTimestamp("3000-04-29-10-30", "yyyy-MM-dd-hh-mm"),
            CryptoPartitionKey(
                asset = "XBT",
                currency = "EUR",
                provider = "KRAKEN",
                api = "TRADES",
                year = "3000",
                month = "04",
                day = "29")
        ).get
        
        ds.show(false)
        
        assert(false)
        
    }
    
    test("a") {
        val s = "01".toInt
        assert(s == 1)
    }
    
    test("spark read") {
        val ss: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "2g")
          .appName("test")
          .master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        val c = ss.read.parquet(Files.getPathForSpark
        ("/parquets/XBT/EUR/TRADES/3000/04/29/KRAKEN/TRADES/parquet")).as[Crypto](encoder(ss))
        
        c.show(false)
    }
    
}
