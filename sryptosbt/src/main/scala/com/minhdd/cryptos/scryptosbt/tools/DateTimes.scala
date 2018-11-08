package com.minhdd.cryptos.scryptosbt.tools

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter


object DateTimes {
    val defaultFormat = "yyyy-MM-dd"
    
    val dtfOut: DateTimeFormatter = DateTimeFormat.forPattern(defaultFormat)
    
    val defaultDateFormat = new SimpleDateFormat(defaultFormat)
    
    def getYear(date: String) =  date.substring(0,4)
    def getMonth(date: String) =  date.substring(5,7)
    def getDay(date: String) =  date.substring(8,10)
    
    def getTime(date: String): Long = {
        getTime(date, DateTimes.defaultFormat)
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
        val dateTimes = getDates(Seq(), new DateTime(startDate), new DateTime(endDate), minutesGap)
        dateTimes.map(Timestamps.fromDatetime)
    }
    
    private def getDates(dates: Seq[DateTime], next: DateTime, end: DateTime): Seq[DateTime] = {
        val newDates: Seq[DateTime] = dates :+ next
        if (next == end) newDates else getDates(newDates, next.plusDays(1), end )
    }
    
    private def getDates(dates: Seq[DateTime], next: DateTime, end: DateTime, minutesGap: Int): Seq[DateTime] = {
        val newDates: Seq[DateTime] = dates :+ next
        val nextNext = next.plusMinutes(minutesGap)
        if (nextNext.isAfter(end) || nextNext == end) newDates else getDates(newDates, nextNext, end, minutesGap)
    }
    
    def fromTimestamp(timestamp: Timestamp): DateTime = {
        new DateTime(timestamp)
    }
}
