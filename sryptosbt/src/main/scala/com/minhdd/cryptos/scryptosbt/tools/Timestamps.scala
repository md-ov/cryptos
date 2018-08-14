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
    def apply(timestamp: Long): Timestamps = {
        new Timestamps(new Timestamp(timestamp), new DateTime(timestamp))
    }
}