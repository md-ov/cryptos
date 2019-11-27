package com.minhdd.cryptos.scryptosbt.domain

import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.SeqDoubleImplicit
import com.minhdd.cryptos.scryptosbt.constants.{evolutionNone, evolutionUp, evolutionDown}

case class Segment(
    begin: BeforeSplit,
    end: Option[BeforeSplit],
    evolutionDirection: String,
    standardDeviationVolume: Double,
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
    def apply(seq: Seq[BeforeSplit], last: Option[BeforeSplit]): Segment = {
        val begin = seq.head
        val evolutionDirection = if (last.isEmpty){
            evolutionNone
        } else if (last.get.value > begin.value) {
            evolutionUp
        } else {
            evolutionDown
        }
        new Segment(
            begin = begin,
            end = last,
            evolutionDirection = evolutionDirection,
            numberOfElement = seq.size,
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
        val last = seq.last
        val lastOption = if (last.isEndOfSegment) Option(last) else None
        
        //why 2 to size : a segment has at least 2 elements and at most size elements
        (2 to seq.size).map(i => {
            val s = seq.take(i)
            Segment(s, lastOption)
        })
    }
    
}