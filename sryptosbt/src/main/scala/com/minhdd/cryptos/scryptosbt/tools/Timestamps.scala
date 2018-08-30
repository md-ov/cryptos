package com.minhdd.cryptos.scryptosbt.tools

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

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
    def apply(timestamp: Long): Timestamps = {
        new Timestamps(new Timestamp(timestamp), new DateTime(timestamp))
    }
    
    def getTime(s: String): Long = {
        getTime(s, "yyyy-MM-dd")
    }
    
    def getTime(s: String, format: String): Long = {
        val date: Date = new SimpleDateFormat(format).parse(s)
        date.getTime
    }
}