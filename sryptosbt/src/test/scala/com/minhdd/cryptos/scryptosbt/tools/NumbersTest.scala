package com.minhdd.cryptos.scryptosbt.tools

import org.scalatest.FunSuite

class NumbersTest extends FunSuite {
    test("test") {
        val input = "948.0"
        val d: Double = Numbers.toDouble(input)
        assert(d == 948.0)
    }
}
