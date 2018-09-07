package com.minhdd.cryptos.scryptosbt.parquet

import org.scalatest.FunSuite
import ToParquetsFromCSV._
import com.minhdd.cryptos.scryptosbt.tools.DateTimes

class ToParquetsFromCSVTest extends FunSuite {

    test("testIsDateOk") {
        val min = "2017-09-29"
        val max = "2018-02-15"
        assert(isDateOk("2017-11-13", max, min))
        assert(isDateOk(min, max, min))
        assert(isDateOk(max, max, min))
        assert(!isDateOk("2017-09-28", max, min))
        assert(!isDateOk("2018-02-16", max, min))
    }
    
    test("date before after") {
        assert(DateTimes.toDate("2017-08-05").before(DateTimes.toDate("2017-08-06")))
        assert(DateTimes.toDate("2017-08-06").after(DateTimes.toDate("2017-08-05")))
        assert(!DateTimes.toDate("2017-08-06").after(DateTimes.toDate("2017-08-06")))
    }

    test("nextSmallerDate") {
        val orderedDates = Seq(
            "2017-08-05", "2017-08-05", "2017-08-05", 
            "2017-08-06", "2017-08-06", 
            "2017-08-07", "2017-08-07", "2017-08-07", "2017-08-07",
            "2017-08-08", "2017-08-08", "2017-08-08", "2017-08-08", "2017-08-08"
        )
        assert(nextSmallerDate(Nil, "2017-08-01") == "2017-08-01")
        assert(nextSmallerDate(orderedDates, "2017-08-01") == "2017-08-05")
        assert(nextSmallerDate(orderedDates, "2017-08-04") == "2017-08-05")
        assert(nextSmallerDate(orderedDates, "2017-08-05") == "2017-08-05")
        assert(nextSmallerDate(orderedDates, "2017-08-06") == "2017-08-05")
        assert(nextSmallerDate(orderedDates, "2017-08-07") == "2017-08-06")
        assert(nextSmallerDate(orderedDates, "2017-08-08") == "2017-08-07")
        assert(nextSmallerDate(orderedDates, "2017-08-09") == "2017-08-08")
        assert(nextSmallerDate(orderedDates, "2017-08-15") == "2017-08-08")
    }

}
