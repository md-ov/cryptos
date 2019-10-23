package com.minhdd.cryptos.scryptosbt.tools

import org.scalatest.FunSuite
import NumberHelper.SeqDoubleImplicit

class NumberHelperTest extends FunSuite {
    
    test("test") {
        val input = "948.0"
        val d: Double = input.toDouble
        assert(d == 948.0)
    }
    
    test("seq 0") {
        val values = Seq(1D, 2D)
        
        val seqMax = values.getMax(4, 4)
        val seqMin = values.getMin(4, 4)
        
        assert(seqMax == Seq(1, 1))
        assert(seqMin == Seq(0, 0))
    }
    
    test("seq 0 fast") {
        val values = Seq(1D, 2D)
        
        val seqMax = values.getMaxFast(4, 4)
        val seqMin = values.getMinFast(4, 4)
        
        assert(seqMax == Seq(1, 1))
        assert(seqMin == Seq(0, 0))
    }
    
    test("seq 1") {
        val values = Seq(1D, 6D, 4D)
        
        val seqMax = values.getMax(4, 4)
        val seqMin = values.getMin(4, 4)
        
        assert(seqMax == Seq(1, 1, 1))
        assert(seqMin == Seq(0, 0, 0))
    }
    
    test("seq 1 fast") {
        val values = Seq(1D, 6D, 4D)
        
        val seqMax = values.getMaxFast(4, 4)
        val seqMin = values.getMinFast(4, 4)
        
        assert(seqMax == Seq(1, 1, 1))
        assert(seqMin == Seq(0, 0, 0))
    }
    
    test("seq 2") {
        val values = Seq(1D, 6D, 4D, 9D)
        
        val seqMax = values.getMax(3, 3)
        val seqMin = values.getMin(3, 3)
        
        assert(seqMax == Seq(1, 3, 3, 3))
        assert(seqMin == Seq(0, 0, 0, 2))
    }
    
    test("seq 2 fast") {
        val values = Seq(1D, 6D, 4D, 9D)
        
        val seqMax = values.getMaxFast(3, 3)
        val seqMin = values.getMinFast(3, 3)
        
        assert(seqMax == Seq(1, 3, 3, 3))
        assert(seqMin == Seq(0, 0, 0, 2))
    }
    
    test("max seq 3") {
        val values = Seq(10D, 6D, 4D, 9D)
        
        val seqMax = values.getMax(3, 3)
        val seqMin = values.getMin(3, 3)
        
        assert(seqMax == Seq(0, 0, 0, 3))
        assert(seqMin == Seq(2, 2, 2, 2))
    }
    
    test("seq 3 fast") {
        val values = Seq(10D, 6D, 4D, 9D)
        
        val seqMax = values.getMaxFast(3, 3)
        val seqMin = values.getMinFast(3, 3)
        
        assert(seqMax == Seq(0, 0, 0, 3))
        assert(seqMin == Seq(2, 2, 2, 2))
    }
    
    test("random") {
        val values: Seq[Double] = Seq(4, 25, 11, 82, 83, 67, 98, 62, 10, 77, 15, 91, 68, 43, 93, 42, 72, 49, 57, 45,
            89, 13,
            0, 41, 31, 69, 86, 29, 47, 18, 90, 54, 66, 20, 87, 46, 30, 28, 35, 96, 85, 58, 63, 36, 70, 23, 59, 27,
            79, 38, 53, 78, 16, 37, 76, 39, 12, 80, 84, 61, 14, 60, 88, 32, 51, 74, 40, 94, 17, 71, 65, 26, 19, 50, 73, 56, 99, 75, 95, 34)
        
        val seqMax = values.getMax(3, 3)
        val seqMin = values.getMin(3, 3)
        
        assert(seqMax == Seq(1, 3, 4, 4, 6, 6, 6, 6, 6, 11, 11, 11, 14, 14, 14, 14, 14, 16, 20, 20, 20, 20, 20, 25, 26,
            26, 26, 26, 30, 30, 30, 30, 30, 34, 34, 34, 34, 39, 39, 39, 39, 39, 40, 44, 44, 44, 48, 48, 48, 48, 48, 51, 51, 51, 54, 57, 58, 58, 58, 58, 62, 62, 62, 62, 62, 67, 67, 67, 67, 67, 69, 69, 74, 74, 76, 76, 76, 76, 76, 78))
        assert(seqMin == Seq(0, 0, 0, 2, 2, 7, 8, 8, 8, 8, 8, 10, 10, 15, 15, 15, 15, 15, 19, 21, 22, 22, 22, 22, 22, 27, 27, 29, 29, 29, 29, 29, 33, 33, 33, 33, 37, 37, 37, 37, 38, 43, 43, 45, 45, 45, 45, 45, 47, 47, 52, 52, 52, 52, 56, 56, 56, 56, 56, 60, 60, 60, 60, 63, 63, 63, 68, 68, 68, 68, 68, 72, 72, 72, 72, 73, 75, 79, 79, 79))
    }
    
    test("random fast") {
        val values: Seq[Double] = Seq(4, 25, 11, 82, 83, 67, 98, 62, 10, 77, 15, 91, 68, 43, 93, 42, 72, 49, 57, 45,
            89, 13,
            0, 41, 31, 69, 86, 29, 47, 18, 90, 54, 66, 20, 87, 46, 30, 28, 35, 96, 85, 58, 63, 36, 70, 23, 59, 27,
            79, 38, 53, 78, 16, 37, 76, 39, 12, 80, 84, 61, 14, 60, 88, 32, 51, 74, 40, 94, 17, 71, 65, 26, 19, 50, 73, 56, 99, 75, 95, 34)
        
        val seqMax = values.getMaxFast(3, 3)
        val seqMin = values.getMinFast(3, 3)
        
        assert(seqMax == Seq(1, 3, 4, 4, 6, 6, 6, 6, 6, 11, 11, 11, 14, 14, 14, 14, 14, 16, 20, 20, 20, 20, 20, 25, 26,
            26, 26, 26, 30, 30, 30, 30, 30, 34, 34, 34, 34, 39, 39, 39, 39, 39, 40, 44, 44, 44, 48, 48, 48, 48, 48, 51, 51, 51, 54, 57, 58, 58, 58, 58, 62, 62, 62, 62, 62, 67, 67, 67, 67, 67, 69, 69, 74, 74, 76, 76, 76, 76, 76, 78))
        assert(seqMin == Seq(0, 0, 0, 2, 2, 7, 8, 8, 8, 8, 8, 10, 10, 15, 15, 15, 15, 15, 19, 21, 22, 22, 22, 22, 22, 27, 27, 29, 29, 29, 29, 29, 33, 33, 33, 33, 37, 37, 37, 37, 38, 43, 43, 45, 45, 45, 45, 45, 47, 47, 52, 52, 52, 52, 56, 56, 56, 56, 56, 60, 60, 60, 60, 63, 63, 63, 68, 68, 68, 68, 68, 72, 72, 72, 72, 73, 75, 79, 79, 79))
    }
}
