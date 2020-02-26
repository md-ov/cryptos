package com.minhdd.cryptos.scryptosbt

object env {
    val pathDelimiter = "/"
    
    //Windows
    val env = "win"
    val prefixPath = "file:///"
    val dataDirectory = "C:/ws/ov/cryptos/data"
    val dataDirectoryy = "C:/ws/ov/cryptos/data"
//    val parquetPath = s"$dataDirectoryy\\parquets"
    val parquetsPath = s"$dataDirectoryy${pathDelimiter}parquets"
    val todayPath = s"$dataDirectoryy/parquets/XBT/EUR/TRADES/today/parquet"
    
    //MAC
    //val env = "mac"
    //val prefixPath = ""
    //val pathDelimiter = "/"
    //val dataDirectory = "/Users/minhdungdao/ws/data/cryptos"
    //val parquetsPath = s"${dataDirectory}/parquets"
    //val todayPath = s"${dataDirectory}/parquets/XBT/EUR/TRADES/today/parquet"
}
