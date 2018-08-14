package com.minhdd.cryptos.scryptosbt.tools

object Numbers {
    def toDouble(s: String): Double = {
        s.toDouble
    }
    
    def twoDigit(i: String): String = {
        if (i.length == 1) "0" + i
        else i.toString
    }
}
