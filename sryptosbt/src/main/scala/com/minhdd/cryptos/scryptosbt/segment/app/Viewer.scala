package com.minhdd.cryptos.scryptosbt.segment.app

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto}
import com.minhdd.cryptos.scryptosbt.parquet.ParquetHelper
import com.minhdd.cryptos.scryptosbt.segment.service.ActualSegment.getActualSegments
import com.minhdd.cryptos.scryptosbt.segment.service.{SegmentHelper, Splitter}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, SparkHelper, TimestampHelper}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.minhdd.cryptos.scryptosbt.segment.service.SegmentHelper.linear
import com.minhdd.cryptos.scryptosbt.segment.service.Splitter.getCutPointsWhenLinear
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

    val ohlcDs: Dataset[Crypto] = ParquetHelper().ohlcCryptoDs(spark).persist

    def main(args: Array[String]): Unit = {
//        viewSegments("15/20200923072332")
        viewHowCutSmallSegments("2020-11-02 13:45:00", "2020-11-04 09:00:00", "/Users/minhdungdao/Desktop/seq20201102-2") //add 15 minutes to the "end"
//        viewHowCutSmallSegmentsBetweenOneTimestampAndToday("2020-11-03 07:00:00", "2020-11-04 07:15:00", "/Users/minhdungdao/Desktop/seq20201103")
//        viewActualSegments
    }

    def viewHowCutSmallSegments(start: String, end: String, csvOutputPath: String): Unit = {
        val seq: Seq[BeforeSplit] = SegmentHelper.getBeforeSplits(spark, start, end, ohlcDs)
        viewIt(seq, csvOutputPath)
    }

    def viewHowCutSmallSegmentsBetweenOneTimestampAndToday(start: String, end: String, csvOutputPath: String): Unit = {
        val seq: Seq[BeforeSplit] = SegmentHelper.getBeforeSplitsBetweenBeginTimestampAndNow(spark, start, end, ohlcDs)
        viewIt(seq, csvOutputPath)
    }

    def viewIt(seq: Seq[BeforeSplit], csvOutputPath: String): Unit = {
//        SparkHelper.csvFromSeqBeforeSplit(spark, csvOutputPath, seq)
        import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.SeqDoubleImplicit
        val linear: Boolean = seq.map(_.value).linear(constants.relativeMinDelta)
        println("linear : " + linear)

        val cuts: Seq[Seq[BeforeSplit]] = Splitter.generalCut(Seq(seq))
        println("cuts size : " + cuts.size)
        println("all linear : " + cuts.forall(_.map(_.value).linear(constants.relativeMinDelta)))
        cuts.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).foreach(println)

        println("when linear false then it must be cutable but sometime by hard cut but not by simple cut")
        println("when linear true then it must not be cutable")
        println("but sometimes it is cut")
        val cuts2: Seq[Seq[BeforeSplit]] = Splitter.cutWithTwoPointsMax(seq, getCutPointsWhenLinear(seq))
        cuts2.map(seq => (seq.size, seq.head.datetime, seq.last.datetime)).foreach(println)
    }

    def viewSegments(path: String): Unit = {
        val smalls: Dataset[Seq[BeforeSplit]] =
            spark.read.parquet(s"$dataDirectory/segments/small/$path").as[Seq[BeforeSplit]]

        println(smalls.count())
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
