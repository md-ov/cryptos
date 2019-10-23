package com.minhdd.cryptos.scryptosbt.service.segment

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit

object Splitter {
    def toSplit(b: BeforeSplit): Boolean = b.importantChange.getOrElse(false)
    
    def toBigSegmentsAndLastTimestamp(beforeSplits: Seq[BeforeSplit]): (Seq[Seq[BeforeSplit]], Timestamp) = {
        val zipped: Seq[(BeforeSplit, Int)] = beforeSplits.zipWithIndex
        val splitIndices: Seq[Int] = zipped.filter(x => toSplit(x._1)).map(_._2)
        
        val beginAndEndIndices: Seq[(Int, Int)] = splitIndices.foldLeft((0, Seq.empty[(Int,Int)])){
            (acc, newP) => (newP, acc._2 ++ Seq((acc._1, newP)))
        }._2
        val bigSegments: Seq[Seq[BeforeSplit]] = beginAndEndIndices.map(x => beforeSplits.slice(x._1, x._2+1))
    
        val lastTimestamp: Timestamp = beforeSplits.apply(splitIndices.last).datetime
        (bigSegments, lastTimestamp)
    }
}
