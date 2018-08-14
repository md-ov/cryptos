package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import org.scalatest.FunSuite

class CryptoValueTest extends FunSuite {
    
    test("testParseLine") {
        val line = "XLM;EUR;kraken;2018-04-16T06:00:00GMT+0200;1523894400;0.226500;0.229800;0.220821;0.225699;0.226591;1087016.31536331;584"
        val parsed = CryptoValue.parseLine(line).apply(0)
        assert(parsed == new CryptoValue(
            asset = "XLM",
            currency = "EUR",
            provider = "KRAKEN",
            datetime = new Timestamp(1523894400000L),
            year = "2018", day = "16", month = "04",
            value = 0.2265,
            volume = 1087016.31536331,
            count = 584,
            prediction = None, accuracy = None, processingDt = parsed.processingDt, predictionDt = parsed.predictionDt
        ))
    }
    
}
