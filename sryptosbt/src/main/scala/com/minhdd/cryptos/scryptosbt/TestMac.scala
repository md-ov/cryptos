package com.minhdd.cryptos.scryptosbt

import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.parquet.ParquetHelper
import com.minhdd.cryptos.scryptosbt.segment.service.ActualSegment.getActualSegments
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

    testOHLC(spark)
//    testActualSegment(spark)
  }

  private def testOHLC(spark: SparkSession): Unit = {
    val parquet = ParquetHelper().ohlcCryptoDs(spark)
    println(parquet.count)
  }

  private def testActualSegment(spark: SparkSession): Unit = {
    val actualSegments: Seq[Seq[BeforeSplit]] = getActualSegments(spark)
    println(actualSegments.size)
    SparkHelper.csvFromSeqBeforeSplit(spark, s"${env.mac.tmpDirectory}/actualsegments-${DateTimeHelper.now}.csv", actualSegments.flatten.take(3))
  }
}
