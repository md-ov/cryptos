package com.minhdd.cryptos.scryptosbt.service.segment

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.{DoubleImplicit, SeqDoubleImplicit}

object Splitter {
    def toSplit(b: BeforeSplit): Boolean = b.importantChange.getOrElse(false)
    
    def toBigSegmentsAndLastTimestamp(beforeSplits: Seq[BeforeSplit]): (Seq[Seq[BeforeSplit]], Timestamp) = {
        val zipped: Seq[(BeforeSplit, Int)] = beforeSplits.zipWithIndex
        val splitIndices: Seq[Int] = zipped.filter(x => toSplit(x._1)).map(_._2)
        
        val beginAndEndIndices: Seq[(Int, Int)] = splitIndices.foldLeft((0, Seq.empty[(Int, Int)])) {
            (acc, newP) => (newP, acc._2 ++ Seq((acc._1, newP)))
        }._2
        val bigSegments: Seq[Seq[BeforeSplit]] = beginAndEndIndices.map(x => beforeSplits.slice(x._1, x._2 + 1))
        
        val lastTimestamp: Timestamp = beforeSplits.apply(splitIndices.last).datetime
        (bigSegments, lastTimestamp)
    }
    
    def toSmallSegments(seq: Seq[BeforeSplit]): Seq[Seq[BeforeSplit]] = {
        if (seq.size <= 2 || seq.map(_.value).linear(constants.relativeMinDelta)) {
            Seq(seq)
        } else {
            val length = seq.length
            val variationsWithFirstPoint: Seq[(Double, Int)] = seq.map(_.value.relativeVariation(seq.head.value)).zipWithIndex
            val superiorMaybeSplit: (Double, Int) = variationsWithFirstPoint.maxBy(_._1)
            val inferiorMaybeSplit: (Double, Int) = variationsWithFirstPoint.minBy(_._1)
            
            val superiorSplit: Option[Int] = 
                if (superiorMaybeSplit._1 >= constants.relativeMinDelta && superiorMaybeSplit._1 > variationsWithFirstPoint.last._1) {
                Option(superiorMaybeSplit._2)
            } else {
                None
            }
            val inferiorSplit: Option[Int] = 
                if (inferiorMaybeSplit._1.abs >= constants.relativeMinDelta && inferiorMaybeSplit._1 < variationsWithFirstPoint.last._1) {
                Option(inferiorMaybeSplit._2)
            } else {
                None
            }
    
            val splitPoints: Seq[Int] = Seq(superiorSplit, inferiorSplit).flatten.sortWith(_ < _)
    
            if (splitPoints.length == 1) {
                Seq(seq.slice(0, splitPoints.head + 1), seq.slice(splitPoints.head, length))
            } else if (splitPoints.length == 2) {
                Seq(seq.slice(0, splitPoints.head + 1), seq.slice(splitPoints.head, splitPoints.last + 1), seq.slice(splitPoints.last, length))
            } else {
                Seq(seq)
            }
        }
    }
}
