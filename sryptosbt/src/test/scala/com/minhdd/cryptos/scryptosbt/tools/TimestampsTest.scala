package com.minhdd.cryptos.scryptosbt.tools

import org.scalatest.FunSuite

class TimestampsTest extends FunSuite {
    
    test("testGetTimestamp 1") {
        val date = "2017-05-08"
        val ts = Timestamps.getTime(date)
        assert(ts == 1494194400000L)
    }
    
}
