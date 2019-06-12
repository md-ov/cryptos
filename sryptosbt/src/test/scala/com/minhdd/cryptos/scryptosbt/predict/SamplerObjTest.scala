package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.exploration.SamplerObj
import org.joda.time.DateTime
import org.scalatest.FunSuite
import org.joda.time.format.DateTimeFormat

class SamplerObjTest extends FunSuite {
    
    test("adjusted") {
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
        val datetime = DateTime.parse("2018-10-24 17:38", formatter)
        val adjusted = SamplerObj.getAdjustedDatetime(15)(datetime)
        assert(formatter.print(adjusted) == "2018-10-24 17:30")
    }
    
    test("adjust second") {
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
        val datetime: DateTime = DateTime.parse("2017-08-01 18:00:00.616", formatter)
        val adjusted = SamplerObj.adjustSecond(datetime)
        assert(formatter.print(adjusted) == "2017-08-01 18:00:00.000")
    }
    
}
