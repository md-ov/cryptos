package com.minhdd.cryptos.scryptosbt.domain

import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.SeqDoubleImplicit

case class Segment(
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
            standardDeviationVolume = seq.map(_.volume).standardDeviation,
            averageVolume = seq.map(_.volume).avg,
            averageVariation = seq.map(_.variation).avg,
            standardDeviationVariation = seq.map(_.variation).standardDeviation,
            averageDerive = seq.flatMap(_.derive).avg,
            standardDeviationDerive = seq.flatMap(_.derive).standardDeviation,
            averageSecondDerive = seq.flatMap(_.secondDerive).avg,
            standardDeviationSecondDerive = seq.flatMap(_.secondDerive).standardDeviation,
            averageCount = seq.filter(_.count.isDefined).map(_.count.get.toDouble).avg,
            standardDeviationCount = seq.filter(_.count.isDefined).map(_.count.get.toDouble).standardDeviation
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