package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.env._
import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.parquet.ParquetHelper
import com.minhdd.cryptos.scryptosbt.segment.service.{ActualSegment, SegmentHelper, Splitter}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, TimestampHelper}
import org.apache.spark.sql.{Dataset, SparkSession}

//3
//after ToSmallSegments
object CompleteSmallSegments {
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    def main(args: Array[String]): Unit = {
        val smallSegmentsPath = s"$dataDirectory/segments/small/$smallSegmentsFolder"
        val smallSegments: Dataset[Seq[BeforeSplit]] = spark.read.parquet(smallSegmentsPath).as[Seq[BeforeSplit]]
        val actualSegments: Dataset[Seq[BeforeSplit]] = spark.createDataset(ActualSegment.getActualSegments(smallSegments))

        val allSmalls: Dataset[Seq[BeforeSplit]] = actualSegments.filter(_.last.isEndOfSegment).union(smallSegments)

        val newTs = DateTimeHelper.now
        println(newTs)

        val outputSegmentsPath = s"$dataDirectory/segments/small/$numberOfMinutesBetweenTwoElement/$newTs"
        allSmalls.write.parquet(outputSegmentsPath)
    }
}
