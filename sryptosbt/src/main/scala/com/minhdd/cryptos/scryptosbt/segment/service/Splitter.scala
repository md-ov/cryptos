package com.minhdd.cryptos.scryptosbt.segment.service

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.{DoubleImplicit, SeqDoubleImplicit}
import com.minhdd.cryptos.scryptosbt.tools.SeqHelper
import org.apache.spark.sql.Dataset
import SegmentHelper.linear

object Splitter {
  def toSplit(b: BeforeSplit): Boolean = b.importantChange.getOrElse(false)

  def toBigSegmentsAndLastTimestamp(beforeSplits: Seq[BeforeSplit]): (Seq[Seq[BeforeSplit]], Timestamp) = {
    val zipped: Seq[(BeforeSplit, Int)] = beforeSplits.zipWithIndex
    val splitIndices: Seq[Int] = zipped.filter(x => toSplit(x._1)).map(_._2)

    val beginAndEndIndices: Seq[(Int, Int)] = splitIndices.foldLeft((0, Seq.empty[(Int, Int)])) {
      (acc, newP) => (newP, acc._2 ++ Seq((acc._1, newP)))
    }._2
    val bigSegments: Seq[Seq[BeforeSplit]] = beginAndEndIndices.map(x => {
      val last = beforeSplits.apply(x._2).copy(isEndOfSegment = true)
      beforeSplits.slice(x._1, x._2) :+ last
    })

    val lastTimestamp: Timestamp = beforeSplits.apply(splitIndices.last).datetime
    (bigSegments, lastTimestamp)
  }

  def generalCut(seq: Seq[Seq[BeforeSplit]]): Seq[Seq[BeforeSplit]] = {
    if (seq.size == 1 && seq.head.size == 1) {
      seq
    } else {
      if (seq.forall(linear)) {
        seq.flatMap(s => cutWithTwoPointsMax(s, getCutPointsWhenLinear(s)))
      } else {
        val cuts: Seq[Seq[BeforeSplit]] = seq.flatMap(s => {
          if (linear(s)) {
            Seq(s)
          } else {
            val cutPoints: Seq[Int] = getCutPoints(s)
            cutManyPoints(s, cutPoints)
          }
        })
        generalCut(cuts)
      }
    }
  }

  def generalCut(ds: Dataset[Seq[BeforeSplit]]): Dataset[Seq[BeforeSplit]] = {
    import ds.sparkSession.implicits._
    ds.flatMap(s => generalCut(Seq(s)))
  }


  private def getSimpleCutPoints(seq: Seq[BeforeSplit], offset: Int): Seq[Int] = {
    println("get simple cut points : " + seq.head.datetime + " - " + seq.last.datetime + " - offset : " + offset)
    getSimpleCutPoints(seq).map(_ + offset)
  }

  private def cutManyPoints(seq: Seq[BeforeSplit], cutPoints: Seq[Int]): Seq[Seq[BeforeSplit]] = {
    if (cutPoints.isEmpty) {
      Seq(seq)
    } else {
      val lastPoint: Int = cutPoints.last
      val cuts = cutOnePoint(seq, lastPoint)
      cutManyPoints(cuts.head, cutPoints.dropRight(1)) :+ cuts.last
    }
  }

  private def cutOnePoint(seq: Seq[BeforeSplit], point: Int): Seq[Seq[BeforeSplit]] = {
    val splitPointElement = seq.apply(point).copy(isEndOfSegment = true)
    Seq(seq.slice(0, point) :+ splitPointElement, seq.slice(point, seq.length))
  }

  def simpleCut(seq: Seq[BeforeSplit]): Seq[Seq[BeforeSplit]] = {
    if (seq.size <= 2 || linear(seq)) {
      Seq(seq)
    } else {
      cutWithTwoPointsMax(seq, getCutPoints(seq))
    }
  }


  private def cutWithTwoPointsMax(seq: Seq[BeforeSplit], cutPoints: Seq[Int]): Seq[Seq[BeforeSplit]] = {
    val length = seq.length

    if (cutPoints.isEmpty) {
      Seq(seq)
    } else if (cutPoints.length == 1) {
      cutOnePoint(seq, cutPoints.head)
    } else if (cutPoints.length == 2) {
      val splitPointElement1 = seq.apply(cutPoints.head).copy(isEndOfSegment = true)
      val splitPointElement2 = seq.apply(cutPoints.last).copy(isEndOfSegment = true)
      Seq(seq.slice(0, cutPoints.head) :+ splitPointElement1,
        seq.slice(cutPoints.head, cutPoints.last) :+ splitPointElement2,
        seq.slice(cutPoints.last, length))
    } else {
      println("There are more than Two points to cut")
      Seq(seq)
    }
  }

  /**
   *
   * get cut points methods
   *
   */

  private def getCutPoints(seq: Seq[BeforeSplit]): Seq[Int] = {
    val splitPoints: Seq[Int] = getSimpleCutPoints(seq).sortWith(_ < _)

    if (splitPoints.nonEmpty) {
      splitPoints
    } else {
      hardSearch(seq, 0, 2).filter(p => linear(cutOnePoint(seq, p).head)).sortWith(_ > _).headOption.toSeq
    }
  }

  private def getSimpleCutPoints(seq: Seq[BeforeSplit]): Seq[Int] = {
    if (seq.size <= 2) {
      Nil
    } else {
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

      if (superiorSplit.isEmpty && inferiorSplit.isEmpty) {
        if (superiorMaybeSplit._1 - inferiorMaybeSplit._1 >= constants.relativeMinDelta) {
          Seq(superiorMaybeSplit._2, inferiorMaybeSplit._2)
            .filterNot(x => x == 0)
            .filterNot(x => x == seq.length - 1)
        } else {
          Nil
        }
      } else {
        Seq(superiorSplit, inferiorSplit).flatten
      }
    }
  }

  private def hardSearch(seq: Seq[BeforeSplit], offset: Int, numberOfSplit: Int = 2): Seq[Int] = {
    println("hardSearch : " + numberOfSplit)
    println(seq.head.datetime, seq.last.datetime)
    val splits: Seq[(Seq[BeforeSplit], Int)] = SeqHelper.splitWithOffset(seq, offset, numberOfSplit)
    val potentialCutPoints: Seq[Int] = splits.flatMap(x => getSimpleCutPoints(x._1, x._2)).sortWith(_ < _)
    println("potential cut points : " + potentialCutPoints + " - " + potentialCutPoints.map(seq.apply(_).datetime))
    val cuts: Seq[Seq[BeforeSplit]] = cutManyPoints(seq, potentialCutPoints)
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

  def getCutPointsWhenLinear(seq: Seq[BeforeSplit]): Seq[Int] = {
    val variationsWithFirstPoint: Seq[(Double, Int)] = seq.map(_.value.relativeVariation(seq.head.value)).zipWithIndex
    val superiorMaybeSplit: (Double, Int) = variationsWithFirstPoint.maxBy(_._1)
    val inferiorMaybeSplit: (Double, Int) = variationsWithFirstPoint.minBy(_._1)

    val superiorSplit: Option[Int] =
      if (superiorMaybeSplit._1 >= constants.relativeMinDelta && variationsWithFirstPoint.last._2 - superiorMaybeSplit._2 >= constants.numberOfCryptoForStability) {
        Option(superiorMaybeSplit._2)
      } else {
        None
      }

    val inferiorSplit: Option[Int] =
      if (inferiorMaybeSplit._1.abs >= constants.relativeMinDelta && variationsWithFirstPoint.last._2 - inferiorMaybeSplit._2 >= constants.numberOfCryptoForStability) {
        Option(inferiorMaybeSplit._2)
      } else {
        None
      }

    Seq(superiorSplit, inferiorSplit).flatten.sortWith(_ < _)
  }
}
