package com.minhdd.cryptos.scryptosbt.tools

import java.text.SimpleDateFormat
import java.util.Date

object DateTimes {
    val defaultFormat = "yyyy-MM-dd"
    val oneDayTimestampDelta = 86400000
    
    val defaultDateFormat = new SimpleDateFormat(defaultFormat)
    
    def getYear(date: String) =  date.substring(0,4)
    def getMonth(date: String) =  date.substring(4,6)
    def getDay(date: String) =  date.substring(6,8)
    
    def getTime(date: String): Long = {
        getTime(date, DateTimes.defaultFormat)
    }
    
    def getTime(dateString: String, format: String): Long = {
        val date: Date = new SimpleDateFormat(format).parse(dateString)
        date.getTime
    }
    
    def getDate(year: String, month: String, day: String): String = {
        year + "-" + month + "-" + day
    }
    
    def getDates(startDate: String, endDate: String): Seq[String] = {
        val date: Date = defaultDateFormat.parse(startDate)
        val startTimestamp: Long = getTime(startDate)
        val endTimestamp: Long = getTime(endDate)
        val times: Seq[Long] = getTimes(Seq(), startTimestamp, oneDayTimestampDelta, endTimestamp)
        times.map(defaultDateFormat.format)
    }
    
    private def getTimes(dates: Seq[Long], next: Long, delta: Long, end: Long): Seq[Long] = {
        val newDates: Seq[Long] = dates :+ next
        if (next == end) newDates else getTimes(newDates, next + delta, delta, end)
    }
}
