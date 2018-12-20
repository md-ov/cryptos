package com.minhdd.cryptos.scryptosbt.predict

import java.sql.Timestamp

case class AnalyticsSegment(
    begin: AnalyticsCrypto,
    beginEvolution: String,
    end: AnalyticsCrypto,
    endEvolution: String,
    sameEvolution: Boolean,
    numberOfElement: Int)

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

case class RegularSegment(
                           begin : AnalyticsSegment,
                           end : AnalyticsSegment,
                           beginTimestamp1 : Timestamp,
                           beginTimestamp2 : Timestamp,
                           endTimestamp1 : Timestamp,
                           endTimestamp2 : Timestamp,
                           beginValue : Double,
                           endValue : Double,
                           segments : Seq[AnalyticsSegment],
                           days : Long,
                           pattern: String)

object RegularSegment {
    def apply(segments: Seq[AnalyticsSegment]): RegularSegment = {
        val beginSegment = segments.head
        val endSegment = segments.last
        val beginTimestamp1 = beginSegment.begin.crypto.cryptoValue.datetime
        val beginTimestamp2 = beginSegment.end.crypto.cryptoValue.datetime
        val beginValue = beginSegment.begin.crypto.cryptoValue.value
        val endTimestamp1 = endSegment.begin.crypto.cryptoValue.datetime
        val endTimestamp2 = endSegment.end.crypto.cryptoValue.datetime
        val endValue = endSegment.begin.crypto.cryptoValue.value
        val days = (endTimestamp1.getTime - beginTimestamp2.getTime) / 86400000
        val pattern = 
            beginSegment.beginEvolution + " - " + beginSegment.endEvolution + 
              " | " + beginSegment.endEvolution + " | " + 
          endSegment.beginEvolution + " - " + endSegment.endEvolution
        new RegularSegment(beginSegment, endSegment, beginTimestamp1, beginTimestamp2, 
            endTimestamp1, endTimestamp2, beginValue, endValue, segments, days, pattern)
    }
}
