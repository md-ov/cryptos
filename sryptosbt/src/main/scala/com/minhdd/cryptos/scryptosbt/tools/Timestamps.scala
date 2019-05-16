package com.minhdd.cryptos.scryptosbt.tools

import java.sql.Timestamp

import org.joda.time.DateTime

case class Timestamps(timestamp: Timestamp, datetime: DateTime) {
    
    def getYearString: String = {
        datetime.getYear.toString
    }
    
    def getMonthString: String = {
        Numbers.twoDigit(datetime.monthOfYear.get.toString)
    }
    
    def getDayString: String = {
        Numbers.twoDigit(datetime.dayOfMonth.get.toString)
    }
}

object Timestamps {
    
    val oneDayTimestampDelta = 86400000 
    
    def apply(timestamp: Long): Timestamps = {
        new Timestamps(new Timestamp(timestamp), new DateTime(timestamp))
    }
    
    def now: Timestamp = {
        new Timestamp(DateTime.now().getMillis)
    }
    
    def fromDatetime(dt: DateTime) = {
        new Timestamp(dt.getMillis)
    }
    
    def getTimestamp(dateString: String, format: String): Timestamp = {
        val time = DateTimes.getTime(dateString, format)
        new Timestamp(time)
    }
    
    def fromTimestampsLong(l: Long): Timestamp = {
        new Timestamp(l * 1000)
    }
    
    def afterOrSame(refTs: Timestamp, ts: Timestamp): Boolean = {
        !ts.before(refTs)
    }
    
}