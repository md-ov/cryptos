package com.minhdd.cryptos.scryptosbt.tools

import com.minhdd.cryptos.scryptosbt.parquet.CryptoPartitionKey
import org.scalatest.FunSuite

class FilesTest extends FunSuite {
    
    test("alldirs") {
        val allDirs: Seq[String] = Files.getAllDir("src//test//resources//parquets")
        assert(allDirs.size == 5)
    }
    
    test("testGetAllDirFromLastTimestamp") {
        val allDirs: Seq[String] = Files.getAllDirFromLastTimestamp(
            path = "src//test//resources//parquets",
            ts = Timestamps.getTimestamp("2019-04-29-10-30", "yyyy-MM-dd-hh-mm"),
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
        def firstLine: Option[String] = Files.firstLine(filePath)
        assert(firstLine.isEmpty)
    
        def filePath2 = getClass.getResource("/csv/1.csv").getPath
        def firstLine2: Option[String] = Files.firstLine(filePath2)
        assert(firstLine2.get == "aaa")
    }
    
}
