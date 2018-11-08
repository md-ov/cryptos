package com.minhdd.cryptos.scryptosbt.predict

import org.joda.time.DateTime
import org.scalatest.FunSuite

class SamplerObjTest extends FunSuite {
    
    test("adjusted") {
        import org.joda.time.format.DateTimeFormat
        
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
        
        val datetime = DateTime.parse("2018-10-24 17:38", formatter)
        
        val adjusted = SamplerObj.getAdjustedDatetime(15)(datetime)
        
        assert(formatter.print(adjusted) == "2018-10-24 17:30")
    }
    
}
