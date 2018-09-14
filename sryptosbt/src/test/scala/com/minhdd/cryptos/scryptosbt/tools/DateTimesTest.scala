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

}
