package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.segment.app.ActualSegment.getActualSegments
import com.minhdd.cryptos.scryptosbt.segment.service.Splitter
import com.minhdd.cryptos.scryptosbt.tools.{SparkHelper, TimestampHelper}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.minhdd.cryptos.scryptosbt.segment.service.SegmentHelper.linear

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
        viewSegments
//        viewHowCutSmallSegments
//        viewActualSegments
    }

    def viewActualSegments = {
        def actualSegments: Seq[Seq[BeforeSplit]] = getActualSegments

        println(actualSegments.size)
        //        println(actualSegments.last.size)
        //        println(actualSegments.last.head.datetime)
        //        println(actualSegments.last.last.datetime)
    }

    // to view segment which end at 2020-05-11 20:15:00 you have to add 15s to the end timestamp
    def viewHowCutSmallSegments: Unit = {
        val start: Timestamp = TimestampHelper.getTimestamp("2020-07-09 17:15:00")
        val end: Timestamp = TimestampHelper.getTimestamp("2020-07-19 23:15:00")
        val seq: Seq[BeforeSplit] = ActualSegment.getBeforeSplits(start, end).dropRight(1)
        import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.{SeqDoubleImplicit}
        val linear: Boolean = seq.map(_.value).linear(constants.relativeMinDelta)
        val cuts: Seq[Seq[BeforeSplit]] = Splitter.generalCut(Seq(seq))
        cuts.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).foreach(println)
        println("when linear true then it must not be cutable")
        println("when linear false then it must be cutable but sometime by hard cut but not by simple cut")
        println("linear : " + linear)
        println("cuts size : " + cuts.size)
//        SparkHelper.csvFromSeqBeforeSplit(spark, "/Users/minhdungdao/Desktop/seq.csv", seq)
    }

    def viewSegments: Unit = {
        val smalls: Dataset[Seq[BeforeSplit]] =
            spark.read.parquet(s"$dataDirectory/segments/small/$smallSegmentsFolder").as[Seq[BeforeSplit]]
//        smalls.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).sort("_1").show(false)

        val numberOfVeryShortSegments = smalls.filter(_.size < 10).count
        println("number of very short segments : " + numberOfVeryShortSegments)

        println("size of small segments: " + smalls.count())
        println("==============")
        val notlinears: Dataset[(Int, Timestamp, Timestamp)] = smalls.filter(x => !linear(x)).map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).sort("_2")

        val numberOfNonLinears = notlinears.count()
        println("non linear : " + numberOfNonLinears)
        if (numberOfNonLinears > 0) notlinears.show()

    }
}
