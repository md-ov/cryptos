package com.minhdd.cryptos.scryptosbt.predict

case class AnalyticsSegment(
    begin: AnalyticsCrypto,
    beginEvolution: String,
    end: AnalyticsCrypto,
    endEvolution: String,
    sameEvolution: Boolean,
    numberOfElement: Int) {
}

object AnalyticsSegment {
    def apply(seq: Seq[AnalyticsCrypto]): AnalyticsSegment = {
        val headC = seq.head
        val endC = seq.last
        val beginEvolution = headC.analytics.evolution.get
        val endEvolution = endC.analytics.evolution.get
        val sameEvolution = beginEvolution == endEvolution
    
        new AnalyticsSegment(
            begin = headC,
            beginEvolution = beginEvolution,
            end = endC,
            endEvolution = endEvolution,
            sameEvolution = sameEvolution,
            numberOfElement = seq.size
        )
    }
}
