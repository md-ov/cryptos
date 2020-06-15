package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.segment.service.Splitter
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper
import org.apache.spark.sql.{Dataset, SparkSession}

object Viewer {

    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    def main(args: Array[String]): Unit = {
//        viewSegments
        viewHowCutSmallSegments
    }

    def viewHowCutSmallSegments: Unit = {
        val start: Timestamp = TimestampHelper.getTimestamp("2020-06-02 01:30:00")
        val end: Timestamp = TimestampHelper.getTimestamp("2020-06-07 16:00:00")
        val seq: Seq[BeforeSplit] = ActualSegment.getBeforeSplits(start, end)
        import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.{SeqDoubleImplicit}
        val linear: Boolean = seq.map(_.value).linear(constants.relativeMinDelta)
        val cuts: Seq[Seq[BeforeSplit]] = Splitter.cutWhenNotLinear(seq)
        println("when linear true then it must not be cutable")
        println(linear)
        println(cuts.size)
    }

    def viewSegments: Unit = {
        val smalls: Dataset[Seq[BeforeSplit]] =
            spark.read.parquet(s"$dataDirectory/segments/small/$smallSegmentsFolder").as[Seq[BeforeSplit]]
        smalls.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).sort("_2").show(false)
        println(smalls.count())
    }
}
