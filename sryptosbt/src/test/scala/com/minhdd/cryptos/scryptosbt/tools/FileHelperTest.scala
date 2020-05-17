package com.minhdd.cryptos.scryptosbt.tools

import com.minhdd.cryptos.scryptosbt.domain.CryptoPartitionKey
import org.scalatest.FunSuite

class FileHelperTest extends FunSuite {
    
    test("alldirs") {
        val allDirs: Seq[String] = FileHelper.getAllDir("src/test/resources/parquets")
        assert(allDirs.size == 5)
    }
    
    test("testGetAllDirFromLastTimestamp") {
        val allDirs: Seq[String] = FileHelper.getAllDirFromLastTimestamp(
            path = "src/test/resources/parquets",
            ts = TimestampHelper.getTimestamp("2019-04-29-10-30", "yyyy-MM-dd-hh-mm"),
            cryptoPartitionKey = CryptoPartitionKey(
                asset = "XBT",
                currency = "EUR",
                provider = "KRAKEN",
                api = "TRADES",
                year = "2019",
                month = "04",
                day = "29"))
        assert(allDirs.size == 1)
    }
    
    test("first line") {
        def filePath = getClass.getResource("/csv/7.csv").getPath
        println(filePath)
        def firstLine: Option[String] = FileHelper.firstLine(filePath)
        assert(firstLine.isEmpty)
    
        def filePath2 = getClass.getResource("/csv/1.csv").getPath
        def firstLine2: Option[String] = FileHelper.firstLine(filePath2)
        assert(firstLine2.get == "aaa")
    }
    
}
