package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import org.scalatest.FunSuite

class CryptoTest extends FunSuite {
    
    test("testParseLine") {
        val line = "XLM;EUR;kraken;2018-04-16T06:00:00GMT+0200;1523894400;0.226500;0.229800;0.220821;0.225699;0.226591;1087016.31536331;584"
        val parsed = Crypto.parseLine(line).apply(0)
        assert(parsed.cryptoValue == new CryptoValue(
            value = 0.2265,
            volume = 1087016.31536331,
            count = 584,
            datetime = new Timestamp(1523894400000L)
        ))
    
        assert(parsed.partitionKey == new CryptoPartitionKey(
            asset = "XLM",
            currency = "EUR",
            provider = "KRAKEN",
            year = "2018", day = "16", month = "04"
        ))
    
    }
    
}
