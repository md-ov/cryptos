package com.minhdd.cryptos.scryptosbt.domain

import java.sql.Timestamp
import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.SeqDoubleImplicit

case class AnalyticsSegment(
                             begin: AnalyticsCrypto,
                             beginEvolution: String,
                             beginVariation: Double,
                             beginVolume: Double,
                             end: AnalyticsCrypto,
                             endEvolution: String,
                             endVariation: Double,
                             endVolume: Double,
                             standardDeviationVolume: Double,
                             sameEvolution: Boolean,
                             numberOfElement: Int)

object AnalyticsSegment {
    def apply(seq: Seq[AnalyticsCrypto]): AnalyticsSegment = {
        val headC = seq.head
        val endC = seq.last
        val beginEvolution = headC.analytics.evolution.get
        val beginVariation = headC.analytics.variation.get
        val beginVolume = headC.crypto.cryptoValue.volume
        val endEvolution = endC.analytics.evolution.get
        val endVariation = endC.analytics.variation.get
        val endVolume = endC.crypto.cryptoValue.volume
        val sameEvolution = beginEvolution == endEvolution
        val standardDeviationVolume = seq.map(_.crypto.cryptoValue.volume).standardDeviation
        
        new AnalyticsSegment(
            begin = headC,
            beginEvolution = beginEvolution,
            beginVariation = beginVariation,
            beginVolume = beginVolume,
            end = endC,
            endEvolution = endEvolution,
            endVariation = endVariation,
            endVolume = endVolume,
            standardDeviationVolume = standardDeviationVolume,
            sameEvolution = sameEvolution,
            numberOfElement = seq.size
        )
    }
}

case class RegularSegment(
                           begin: AnalyticsSegment,
                           end: AnalyticsSegment,
                           beginTimestamp1: Timestamp,
                           beginTimestamp2: Timestamp,
                           endTimestamp1: Timestamp,
                           endTimestamp2: Timestamp,
                           beginValue: Double,
                           endValue: Double,
                           beginVariation: Double,
                           endVariation: Double,
                           segments: Seq[AnalyticsSegment],
                           days: Long,
                           numberOfSegment: Int,
                           pattern: String,
                           evolution: String,
                           variationsEcartType: Double)


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
        val variations: Seq[Double] = segments.map(s => s.beginVariation)
        val ecartTypeOfVariations = variations.standardDeviation
        new RegularSegment(
            begin = beginSegment, end = endSegment,
            beginTimestamp1 = beginTimestamp1, beginTimestamp2 = beginTimestamp2,
            endTimestamp1 = endTimestamp1, endTimestamp2 = endTimestamp2,
            beginValue = beginValue, endValue = endValue,
            beginVariation = beginSegment.beginVariation,
            endVariation = endSegment.beginVariation,
            segments = segments,
            numberOfSegment = segments.size,
            days = days,
            pattern = pattern,
            evolution = beginSegment.endEvolution,
            variationsEcartType = ecartTypeOfVariations)
    }
}
