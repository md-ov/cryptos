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
            ts = Timestamps.getTimestamp("3000-04-29-10-30", "yyyy-MM-dd-hh-mm"),
            cryptoPartitionKey = CryptoPartitionKey(
            asset = "XBT",
            currency = "EUR",
            provider = "KRAKEN",
            api = "TRADES",
            year = "3000",
            month = "04",
            day = "29"))
    
        assert(allDirs.size == 5)
}
    
}
