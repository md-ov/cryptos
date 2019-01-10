package com.minhdd.cryptos.scryptosbt.tools

object Statistics {
    def standardDeviation(s: Seq[Double]):Double = {
        def avg(s: Seq[Double]): Double = s.sum / s.size
        
        def variance(s: Seq[Double]): Double = {
            val avgDouble: Double = avg(s)
            s.map(d => math.pow(d - avgDouble, 2)).sum / s.size
        }
        math.sqrt(variance(s))
    }
}
