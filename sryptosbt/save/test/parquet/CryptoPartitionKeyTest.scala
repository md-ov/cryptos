package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.domain.CryptoPartitionKey
import org.scalatest.FunSuite

class CryptoPartitionKeyTest extends FunSuite {
    val cryptoPartitionKey = CryptoPartitionKey(
        asset = "XLM",
        currency = "EUR",
        provider = "KRAKEN",
        api = "OHLC",
        year = "2018",
        month = "08",
        day = "20"
    )

    test("testGetPartitionPath") {
        val path = cryptoPartitionKey.getPartitionPath("file:///home/mdao/minh/git/cryptos/data/parquets/")
        assert (path == "file:///home/mdao/minh/git/cryptos/data/parquets/XLM/EUR/OHLC/2018/08/20/KRAKEN/OHLC/parquet")
    }

    test("testGetPartitionPath 2") {
        val path = cryptoPartitionKey.getPartitionPath("file:///home/mdao/minh/git/cryptos/data/parquets")
        assert (path == "file:///home/mdao/minh/git/cryptos/data/parquets/XLM/EUR/OHLC/2018/08/20/KRAKEN/OHLC/parquet")
    }


    test("testGetPartitionPath for windows") {
        val path = cryptoPartitionKey.getPartitionPath("file:///D:\\ws\\cryptos\\data\\parquets\\parquet")
        assert (path == "file:///D:\\ws\\cryptos\\data\\parquets\\parquet\\XLM\\EUR\\OHLC\\2018\\08\\20\\KRAKEN\\OHLC" +
          "\\parquet")
    }
    
    test("getOHLCPath for windows") {
        val path = cryptoPartitionKey.getOHLCPath("file:///D:\\ws\\cryptos\\data\\parquets\\parquet")
        assert (path == "file:///D:\\ws\\cryptos\\data\\parquets\\parquet\\XLM\\EUR\\OHLC\\parquet")
    }
}
