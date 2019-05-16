package com.minhdd.cryptos.scryptosbt.tools

import org.scalatest.FunSuite

class TimestampsTest extends FunSuite {
    
    test("testGetTimestamp 1") {
        val date = "2017-05-08"
        val ts = DateTimes.getTime(date)
        assert(ts == 1494194400000L)
    }
    
    test("after or same") {
        val tsref = Timestamps.getTimestamp("3000-04-29-10-30", "yyyy-MM-dd-hh-mm")
        val ts1 = Timestamps.getTimestamp("3000-04-29-10-30", "yyyy-MM-dd-hh-mm")
        val ts2 = Timestamps.getTimestamp("3000-04-29-10-31", "yyyy-MM-dd-hh-mm")
        
        assert(Timestamps.afterOrSame(tsref, ts1))
        assert(Timestamps.afterOrSame(tsref, ts2))
    }
    
}
