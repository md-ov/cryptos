package com.minhdd.cryptos.scryptosbt.service

import com.minhdd.cryptos.scryptosbt.exploration.BeforeSplit

object Splitter {
    def toSplit(b: BeforeSplit): Boolean = b.importantChange.getOrElse(false)
    
    def toBigSegments(beforeSplits: Seq[BeforeSplit]): Seq[Seq[BeforeSplit]] = {
        val zipped: Seq[(BeforeSplit, Int)] = beforeSplits.zipWithIndex
        val splitPositions: Seq[Int] = zipped.filter(x =>
            toSplit(x._1) || x._2 == beforeSplits.length-1
        ).map(_._2)
        val seqq: Seq[(Int, Int)] = splitPositions.foldLeft((0, Seq.empty[(Int,Int)])){
            (acc, newP) => (newP, acc._2 ++ Seq((acc._1, newP)))
        }._2
//        seqq.foreach(x => {
//            print(x)
//            print("---")
//            println(beforeSplits.apply(x._2))
//        })
        seqq.map(x => beforeSplits.slice(x._1, x._2+1))
    }
}
