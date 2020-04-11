package com.minhdd.cryptos.scryptosbt

object env {
  val pathDelimiter = "/"

  val props = mac

  val env = props.env
  val prefixPath = ""
  val dataDirectory = props.dataDirectory
  val parquetsPath = props.parquetsPath
  val todayPath = props.todayPath

  object mac {
    val env = "mac"
    val tmpDirectory = "/Users/minhdungdao/ws/tmp"
    val dataDirectory = "/Users/minhdungdao/ws/data/cryptos"
    val parquetsPath = s"${dataDirectory}/parquets"
    val todayPath = s"${dataDirectory}/parquets/XBT/EUR/TRADES/today/parquet"
  }

  object dell {
    val env = "win"
//    val prefixPath = "file:///"
    val dataDirectory = "C:/ws/ov/cryptos/data"
    val dataDirectoryy = "C:/ws/ov/cryptos/data"
    //    val parquetPath = s"$dataDirectoryy\\parquets"
    val parquetsPath = s"$dataDirectoryy${pathDelimiter}parquets"
    val todayPath = s"$dataDirectoryy/parquets/XBT/EUR/TRADES/today/parquet"
  }
}
