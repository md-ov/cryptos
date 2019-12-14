package com.minhdd.ntb

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source
case class Stat(indicator: String, d: String, count: Double)
case class MonthDayCount(month: Int, day: Option[Int], count: Double)
case class RawStat(year: Int, month: Option[Int], indicator: String, data: Seq[MonthDayCount]) {
    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    def toStats(): Seq[Stat] = {
        data.map(monthdayCount => {
            Stat(
                d = LocalDate.of(year, monthdayCount.month, monthdayCount.day.get).format(fmt),
                indicator = indicator,
                count = monthdayCount.count
            )
        })
    }
}

object Stats {
    def main(args: Array[String]): Unit = {
        val d1807 = "D:\\tmp\\s\\201807.csv"
        val d1808 = "D:\\tmp\\s\\201808.csv"
        val d1809 = "D:\\tmp\\s\\201809.csv"
        val d1810 = "D:\\tmp\\s\\201810.csv"
        val d1811 = "D:\\tmp\\s\\201811.csv"
        val d1812 = "D:\\tmp\\s\\201812.csv"
        val d1901 = "D:\\tmp\\s\\201901.csv"
        val d1902 = "D:\\tmp\\s\\201902.csv"
        val d1903 = "D:\\tmp\\s\\201903.csv"
        val d1904 = "D:\\tmp\\s\\201904.csv"
        val d1905 = "D:\\tmp\\s\\201905.csv"
        val d1906 = "D:\\tmp\\s\\201906.csv"
        val d1907 = "D:\\tmp\\s\\201907.csv"
        val d1908 = "D:\\tmp\\s\\201908.csv"
        val d1909 = "D:\\tmp\\s\\201909.csv"
        val d18 = "D:\\tmp\\s\\2018.csv"
        val d19 = "D:\\tmp\\s\\2019.csv"
        
        val r1807: Seq[RawStat] = toRawStatSeq(d1807, Some(7), 2018)
        val r1808: Seq[RawStat] = toRawStatSeq(d1808, Some(8), 2018)
        val r1809: Seq[RawStat] = toRawStatSeq(d1809, Some(9), 2018)
        val r1810: Seq[RawStat] = toRawStatSeq(d1810, Some(10), 2018)
        val r1811: Seq[RawStat] = toRawStatSeq(d1811, Some(11), 2018)
        val r1812: Seq[RawStat] = toRawStatSeq(d1812, Some(12), 2018)
        val r1901: Seq[RawStat] = toRawStatSeq(d1901, Some(1), 2019)
        val r1902: Seq[RawStat] = toRawStatSeq(d1902, Some(2), 2019)
        val r1903: Seq[RawStat] = toRawStatSeq(d1903, Some(3), 2019)
        val r1904: Seq[RawStat] = toRawStatSeq(d1904, Some(4), 2019)
        val r1905: Seq[RawStat] = toRawStatSeq(d1905, Some(5), 2019)
        val r1906: Seq[RawStat] = toRawStatSeq(d1906, Some(6), 2019)
        val r1907: Seq[RawStat] = toRawStatSeq(d1907, Some(7), 2019)
        val r1908: Seq[RawStat] = toRawStatSeq(d1908, Some(8), 2019)
        val r1909: Seq[RawStat] = toRawStatSeq(d1909, Some(9), 2019)
        val r18 : Seq[RawStat] = toRawStatSeq(d18, None, 2018, toMonthAndCount)
        val r19 : Seq[RawStat] = toRawStatSeq(d19, None, 2019, toMonthAndCount)
        
        println(r1807.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r18.filter(_.indicator == "Visits").head.data.filter(_.month == 7).head.count)
        
        println(r1808.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r18.filter(_.indicator == "Visits").head.data.filter(_.month == 8).head.count)
        
        println(r1809.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r18.filter(_.indicator == "Visits").head.data.filter(_.month == 9).head.count)
        
        println(r1810.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r18.filter(_.indicator == "Visits").head.data.filter(_.month == 10).head.count)
        
        println(r1811.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r18.filter(_.indicator == "Visits").head.data.filter(_.month == 11).head.count)
        
        println(r1812.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r18.filter(_.indicator == "Visits").head.data.filter(_.month == 12).head.count)
        
        println(r1901.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r19.filter(_.indicator == "Visits").head.data.filter(_.month == 1).head.count)
        
        println(r1902.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r19.filter(_.indicator == "Visits").head.data.filter(_.month == 2).head.count)
        
        println(r1903.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r19.filter(_.indicator == "Visits").head.data.filter(_.month == 3).head.count)
        
        println(r1904.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r19.filter(_.indicator == "Visits").head.data.filter(_.month == 4).head.count)
        
        println(r1905.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r19.filter(_.indicator == "Visits").head.data.filter(_.month == 5).head.count)
    
        println(r1906.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r19.filter(_.indicator == "Visits").head.data.filter(_.month == 6).head.count)
    
        println(r1907.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r19.filter(_.indicator == "Visits").head.data.filter(_.month == 7).head.count)
    
        println(r1908.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r19.filter(_.indicator == "Visits").head.data.filter(_.month == 8).head.count)
    
        println(r1909.filter(_.indicator == "Visits").head.data.map(_.count).sum + " == " +
          r19.filter(_.indicator == "Visits").head.data.filter(_.month == 9).head.count)
    
    
        val s = Seq(r1807, r1808, r1809, r1810, r1811, r1812, r1901, r1902, r1903, r1904, r1905, r1906, r1907, r1908,
            r1909)
        val allStats: Seq[Stat] = s.flatMap(_.flatMap(_.toStats()))
        
        writeToCSV(allStats)
    }
    
    def toRawStatSeq(filePath: String, month: Option[Int], year: Int, toDateAndCount: String => MonthDayCount = toMonthDayAndCount)
    : Seq[RawStat] = {
        val fileContents: Iterator[String] = Source.fromFile(filePath).getLines
        var i = 0
        var lines : Seq[String] = Seq()
        while (fileContents.hasNext) {
            i = i + 1
            val s = fileContents.next()
            if (i > 10 && !Seq(' ', '#').contains(s.headOption.getOrElse(' '))) {
                lines = lines :+ s
            }
        }
        
        implicit class MyImplicitClass(s: Seq[String]) {
            def splitWith(toSplit: String => Boolean): Seq[Seq[String]]  = {
                val splitPositions = s.indices.filter(p => toSplit(s.apply(p)))
                val beginAndEndPositions: Seq[(Int, Int)] = splitPositions.indices.map(i => {
                    if (i<splitPositions.length-1) {
                        (splitPositions.apply(i), splitPositions.apply(i+1))
                    } else {
                        (splitPositions.apply(i), s.length)
                    }
                })
                beginAndEndPositions.map(e => {
                    s.slice(e._1, e._2)
                })
            }
        }
        
        val splits: Seq[Seq[String]] = lines.splitWith(_.startsWith("Date Range,"))
        
        val rawStats: Seq[RawStat] = splits.map(s => {
            val header: String = s.head
            val indicator : String = header.split(",").last
            val raws: Seq[String] = s.drop(1)
            val splitRaws: Seq[MonthDayCount] = raws.map(toDateAndCount)
            RawStat(year, month, indicator, splitRaws)
        })
        
        rawStats
    }
    
    def toMonthDayAndCount(s: String): MonthDayCount = {
        val splits = s.split(",")
        val ds: String = splits.head
        val count = splits.apply(1).toDouble
        val monthAndDay: Array[String] = ds.split(" ").apply(1).split("/")
        val month: Int = monthAndDay.apply(0).toInt
        val day: Int = monthAndDay.apply(1).toInt
        MonthDayCount(month, Some(day), count)
    }
    def toMonthAndCount(s: String): MonthDayCount = {
        val splits = s.split(",")
        val ds: String = splits.head
        val count = splits.apply(1).toDouble
        val month: Int = ds.split("/").apply(0).toInt
        MonthDayCount(month, None, count)
    }
    
    def writeToCSV(stats: Seq[Stat]) = {
        val ss: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "2g")
          .appName("stats")
          .master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        import ss.implicits._
        val ds: Dataset[Stat] = ss.createDataset(stats)
        
        ds.coalesce(1).write.option("delimiter", ";").csv("D:\\tmp\\s\\0.csv")
    }
    
    
}
