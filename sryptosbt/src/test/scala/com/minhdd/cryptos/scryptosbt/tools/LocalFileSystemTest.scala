package com.minhdd.cryptos.scryptosbt.tools

import org.scalatest.FunSuite

class LocalFileSystemTest extends FunSuite {
  test("get children") {
    val folders = LocalFileSystem.getChildren("/Users/minhdungdao/ws/data/cryptos/parquets/XBT/EUR/OHLC/parquet")
    folders.foreach(println)
    println(folders.size)
    assert(folders.forall(_.toLong > 0))
    assert(folders.nonEmpty)
  }
}
