package com.minhdd.cryptos.scryptosbt.tools

import org.scalatest.FunSuite
import TimestampHelper.TimestampImplicit

class DateTimeHelperTest extends FunSuite {
    test("get dates") {
        val start = "2018-07-21"
        val end = "2018-08-02"
        val dates = DateTimeHelper.getDates(start, end)
        assert(dates.size == 13)
    }
    
    test("get dates 2") {
        val start = "2018-03-24"
        val end = "2018-03-26"
        val dates = DateTimeHelper.getDates(start, end)
        assert(dates.size == 3)
    }
    
    test("get timestamp") {
        val start = TimestampHelper.getTimestamp("2018-03-24", "yyyy-MM-dd")
        val end = TimestampHelper.getTimestamp("2018-03-29", "yyyy-MM-dd")
        val tss = DateTimeHelper.getTimestamps(start, end, 60*24)
        tss.foreach(println)
    }
    
    test("testGetTimestamp 1") {
        val date = "2017-05-08"
        val ts = DateTimeHelper.getTime(date)
        assert(ts == 1494194400000L)
    }
    
    test("after or same") {
        val tsref = TimestampHelper.getTimestamp("3000-04-29-10-30", "yyyy-MM-dd-hh-mm")
        val ts1 = TimestampHelper.getTimestamp("3000-04-29-10-30", "yyyy-MM-dd-hh-mm")
        val ts2 = TimestampHelper.getTimestamp("3000-04-29-10-31", "yyyy-MM-dd-hh-mm")
        
        assert(ts1.afterOrSame(tsref))
        assert(ts2.afterOrSame(tsref))
    }

}
