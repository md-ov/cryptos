package com.minhdd.cryptos.scryptosbt.parquet

import org.scalatest.FunSuite
import ToParquetsFromCSV._
import com.minhdd.cryptos.scryptosbt.tools.DateTimes

class ToParquetsFromCSVTest extends FunSuite {

    
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
