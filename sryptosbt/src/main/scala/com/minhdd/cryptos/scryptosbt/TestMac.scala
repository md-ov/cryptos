package com.minhdd.cryptos.scryptosbt

import com.minhdd.cryptos.scryptosbt.constants.{directoryNow, numberOfMinutesBetweenTwoElement}
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.segment.app.ActualSegment.getActualSegments
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, SparkHelper, TimestampHelper}
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

//    spark.read.parquet(s"$dataDirectory/segments/small/$numberOfMinutesBetweenTwoElement/$directoryNow").show(5)
//    spark.read.parquet(s"$dataDirectory/parquets/XBT/EUR/OHLC/parquet").show(5)

    val actualSegments: Seq[Seq[BeforeSplit]] = getActualSegments
    SparkHelper.csvFromSeqBeforeSplit(spark, s"${env.mac.tmpDirectory}/actualsegments-${DateTimeHelper.now}.csv", actualSegments.flatten.take(3))
  }
}
