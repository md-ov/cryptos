package com.minhdd.cryptos.scryptosbt.tools

import org.scalatest.FunSuite

class SliceTest extends FunSuite {
    
    test("aaaa") {
        val seq = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)
        
        val slice = seq.slice(3, 5)
        val last = seq.apply(5)
        
        val newS = slice :+ last
        
        println(newS)
    }
    
}
