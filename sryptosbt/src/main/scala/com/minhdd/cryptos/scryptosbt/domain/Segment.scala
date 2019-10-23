package com.minhdd.cryptos.scryptosbt.domain

import com.minhdd.cryptos.scryptosbt.tools.Statistics

case class Segment (
                     begin: BeforeSplit,
                     end: BeforeSplit,
                     standardDeviationVolume: Double,
                     sameEvolution: Boolean,
                     numberOfElement: Int,
                     averageVolume: Double,
                     averageVariation: Double,
                     standardDeviationVariation: Double,
                     averageDerive: Double,
                     standardDeviationDerive: Double,
                     averageSecondDerive: Double,
                     standardDeviationSecondDerive: Double,
                     averageCount: Double,
                     standardDeviationCount: Double
                   )

object Segment {
    def apply(seq: Seq[BeforeSplit], last: BeforeSplit): Segment = {
        val begin = seq.head
        new Segment(
            begin = begin,
            end = last,
            numberOfElement = seq.size,
            sameEvolution = begin.evolution == last.evolution,
            standardDeviationVolume = Statistics.standardDeviation(seq.map(_.volume)),
            averageVolume = Statistics.avg(seq.map(_.volume)),
            averageVariation = Statistics.avg(seq.map(_.variation)),
            standardDeviationVariation = Statistics.standardDeviation(seq.map(_.variation)),
            averageDerive = Statistics.avg(seq.flatMap(_.derive)),
            standardDeviationDerive = Statistics.standardDeviation(seq.flatMap(_.derive)),
            averageSecondDerive = Statistics.avg(seq.flatMap(_.secondDerive)),
            standardDeviationSecondDerive = Statistics.standardDeviation(seq.flatMap(_.secondDerive)),
            averageCount = Statistics.avg(seq.filter(_.count.isDefined).map(_.count.get.toDouble)),
            standardDeviationCount = Statistics.standardDeviation(seq.filter(_.count.isDefined).map(_.count.get.toDouble))
        )
    }
    
    def segments(seq: Seq[BeforeSplit]): Seq[Segment] = {
        def size = seq.size
        (2 to size).map(i => {
            val s = seq.take(i)
            Segment(s, seq.last)
        })
    }
    
}