package com.minhdd.cryptos.scryptosbt.model.app

import com.minhdd.cryptos.scryptosbt.constants.{directoryNow, numberOfMinutesBetweenTwoElement}
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import org.apache.spark.sql.SparkSession

object TestMac {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.read.parquet(s"$dataDirectory/segments/small/$numberOfMinutesBetweenTwoElement/$directoryNow").show(5)
    spark.read.parquet(s"$dataDirectory/parquets/XBT/EUR/OHLC/parquet").show(5)
  }
}
