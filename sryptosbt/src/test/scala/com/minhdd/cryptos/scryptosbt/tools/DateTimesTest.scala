package com.minhdd.cryptos.scryptosbt.tools

import org.scalatest.FunSuite

class DateTimesTest extends FunSuite {
    test("get dates") {
        val start = "2018-07-21"
        val end = "2018-08-02"
        val dates = DateTimes.getDates(start, end)
        assert(dates.size == 13)
    }
    
    test("get dates 2") {
        val start = "2018-03-24"
        val end = "2018-03-26"
        val dates = DateTimes.getDates(start, end)
        assert(dates.size == 3)
    }
    
    test("get timestamp") {
        val start = Timestamps.getTimestamp("2018-03-24", "yyyy-MM-dd")
        val end = Timestamps.getTimestamp("2018-03-29", "yyyy-MM-dd")
        val tss = DateTimes.getTimestamps(start, end, 60*24)
        tss.foreach(println)
    }

}
