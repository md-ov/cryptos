package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.segment.service.ActualSegment.getActualSegments
import com.minhdd.cryptos.scryptosbt.segment.service.{SegmentHelper, Splitter}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, SparkHelper, TimestampHelper}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.minhdd.cryptos.scryptosbt.segment.service.SegmentHelper.linear
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, LocalDateTimes}

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
        viewHowCutSmallSegments("2013-08-28 02:00:00", "2013-09-27 01:45:00", "yyyy-MM-dd HH:mm:ss")
//        viewActualSegments
    }

    // to view segment which end at 2020-05-11 20:15:00 you have to add 15s to the end timestamp
    def viewHowCutSmallSegments(start: String, end: String, format: String): Unit = {
        val startLocalDate: DateTime = DateTime.parse(start, DateTimeFormat.forPattern(format))
        val endLocalDate: DateTime = DateTime.parse(end, DateTimeFormat.forPattern(format))

        val seq: Seq[BeforeSplit] = SegmentHelper.getBeforeSplits(spark, startLocalDate, endLocalDate)
//        SparkHelper.csvFromSeqBeforeSplit(spark, "/Users/minhdungdao/Desktop/seq20130919", seq)
        import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.SeqDoubleImplicit
        val linear: Boolean = seq.map(_.value).linear(constants.relativeMinDelta)
        println("linear : " + linear)

        val cuts: Seq[Seq[BeforeSplit]] = Splitter.generalCut(Seq(seq))
        println("cuts size : " + cuts.size)
        println("all linear : " + cuts.forall(_.map(_.value).linear(constants.relativeMinDelta)))
        cuts.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).foreach(println)
        println("when linear true then it must not be cutable")
        println("when linear false then it must be cutable but sometime by hard cut but not by simple cut")
    }

    def viewSegments: Unit = {
        val smalls: Dataset[Seq[BeforeSplit]] =
            spark.read.parquet(s"$dataDirectory/segments/small/15/20200819181046").as[Seq[BeforeSplit]]
        smalls.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).sort("_2").show(10, false)

        println("segments not ending")
        smalls.filter(x => ! x.last.isEndOfSegment).map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).show(10,false)

        val numberOfVeryShortSegments = smalls.filter(_.size == 2).count
        println("number of 2 elements segments : " + numberOfVeryShortSegments)
        smalls.filter(_.size == 2).map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).show(10)

        println("size of small segments: " + smalls.count())
        println("==============")
        val notlinears: Dataset[(Int, Timestamp, Timestamp)] = smalls.filter(x => !linear(x)).map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).sort("_2")

        val numberOfNonLinears = notlinears.count()
        println("non linear : " + numberOfNonLinears)
        if (numberOfNonLinears > 0) notlinears.show()

    }

    def viewActualSegments = {
        def actualSegments: Seq[Seq[BeforeSplit]] = getActualSegments(spark)

        println(actualSegments.size)
        //        println(actualSegments.last.size)
        //        println(actualSegments.last.head.datetime)
        //        println(actualSegments.last.last.datetime)
    }
}
