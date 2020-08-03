package com.minhdd.cryptos.scryptosbt.segment.service

import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.tools.SeqHelper
import SegmentHelper.linear

object HardSearcher {
  private def hardSearch(seq: Seq[BeforeSplit], offset: Int, numberOfSplit: Int = 2): Seq[Int] = {
    println("hardSearch : " + numberOfSplit)
    println(seq.head.datetime, seq.last.datetime)
    val splits: Seq[(Seq[BeforeSplit], Int)] = SeqHelper.splitWithOffset(seq, offset, numberOfSplit)
    val potentialCutPoints: Seq[Int] = splits.flatMap(x => getSimpleCutPointsWithOffset(x._1, x._2)).sortWith(_ < _)
    println("potential cut points : " + potentialCutPoints + " - " + potentialCutPoints.map(seq.apply(_).datetime))
    val cuts: Seq[Seq[BeforeSplit]] = Splitter.cutManyPoints(seq, potentialCutPoints)
    if (cuts.forall(linear)) {
      println("all linear with : " + potentialCutPoints)
      potentialCutPoints
    } else {
      if (numberOfSplit >= seq.length / 2) {
        Nil
      } else {
        hardSearch(seq, offset, numberOfSplit + 1)
      }
    }
  }

  def getSimpleCutPointsWithOffset(seq: Seq[BeforeSplit], offset: Int): Seq[Int] = {
    println("get simple cut points : " + seq.head.datetime + " - " + seq.last.datetime + " - offset : " + offset)
    Splitter.getSimpleCutPoints(seq).map(_ + offset)
  }

}
