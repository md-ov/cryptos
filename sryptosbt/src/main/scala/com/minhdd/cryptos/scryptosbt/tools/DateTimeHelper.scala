package com.minhdd.cryptos.scryptosbt.tools

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.minhdd.cryptos.scryptosbt.constants
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper.{DateTimeImplicit, dtfOut}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object DateTimeHelper {

    val defaultFormat = "yyyy-MM-dd"
    
    val dtfOut: DateTimeFormatter = DateTimeFormat.forPattern(defaultFormat)
    
    val defaultDateFormat = new SimpleDateFormat(defaultFormat)
    
    def getYear(date: String) = date.substring(0, 4)
    
    def getMonth(date: String) = date.substring(5, 7)
    
    def getDay(date: String) = date.substring(8, 10)

    def now: String = DateTimeFormat.forPattern("yyyyMMddHHmmss").print(DateTime.now())

    def getTime(date: String): Long = {
        getTime(date, DateTimeHelper.defaultFormat)
    }
    
    def getTime(dateString: String, format: String): Long = {
        val date: Date = new SimpleDateFormat(format).parse(dateString)
        date.getTime
    }
    
    def toDate(s: String): Date = defaultDateFormat.parse(s)
    
    def getDate(year: String, month: String, day: String): String = {
        year + "-" + month + "-" + day
    }
    
    def getDates(startDate: String, endDate: String): Seq[String] = {
        val dateTimes = getDates(Seq(), new DateTime(toDate(startDate)), new DateTime(toDate(endDate)))
        dateTimes.map(dtfOut.print)
    }
    
    def getTimestamps(startDate: Timestamp, endDate: Timestamp, minutesGap: Int): Seq[Timestamp] = {
        val dateTimes: Seq[DateTime] = getDates(Seq(), new DateTime(startDate), new DateTime(endDate), minutesGap)
        dateTimes.map(_.toTimestamp)
    }
    
    private def getDates(dates: Seq[DateTime], next: DateTime, end: DateTime): Seq[DateTime] = {
        val newDates: Seq[DateTime] = dates :+ next
        if (next == end) newDates else getDates(newDates, next.plusDays(1), end)
    }
    
    private def getDates(dates: Seq[DateTime], next: DateTime, end: DateTime, minutesGap: Int): Seq[DateTime] = {
        val newDates: Seq[DateTime] = dates :+ next
        val nextNext = next.plusMinutes(minutesGap)
        if (nextNext.isAfter(end) || nextNext == end) newDates else getDates(newDates, nextNext, end, minutesGap)
    }
    
    implicit class DateTimeImplicit(input: DateTime) {
        
        def toTimestamp: Timestamp = new Timestamp(input.getMillis)
        
        def getAdjustedDatetime: DateTime = {
            val minutes = input.getMinuteOfHour
            val delta: Int = minutes % constants.numberOfMinutesBetweenTwoElement
            input.minusMinutes(delta)
        }
        
        def getAdjustedSecond: DateTime = {
            val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm'Z'")
            DateTime.parse(formatter.print(input), formatter)
        }
    }
    
}

case class TimestampHelper(timestamp: Timestamp, datetime: DateTime) {
    
    def getYear: String = {
        datetime.getYear.toString
    }
    
    def getMonth: String = {
        NumberHelper.twoDigit(datetime.monthOfYear.get.toString)
    }
    
    def getDay: String = {
        NumberHelper.twoDigit(datetime.dayOfMonth.get.toString)
    }
}

object TimestampHelper {
    
    val oneDayTimestampDelta = 86400000
    
    val defaultFormat = "yyyy-MM-dd hh:mm:ss"
    
    def apply(timestamp: Long): TimestampHelper = {
        new TimestampHelper(new Timestamp(timestamp), new DateTime(timestamp))
    }

    def now: Timestamp = new Timestamp(DateTime.now().getMillis)
    
    def getString(ts: Timestamp) : String = dtfOut.print(new DateTime(ts))

    def getTimestamp(dateString: String): Timestamp = getTimestamp(dateString, defaultFormat)
    
    def getTimestamp(dateString: String, format: String): Timestamp = {
        val time = DateTimeHelper.getTime(dateString, format)
        new Timestamp(time)
    }
    
    implicit class TimestampImplicit(input: Timestamp) {
        
        def afterOrSame(other: Timestamp): Boolean = !input.before(other)
        
        def toDateTime: DateTime = new DateTime(input)
        
        def adjusted: Timestamp = toDateTime.getAdjustedDatetime.getAdjustedSecond.toTimestamp
    }
}
