package com.minhdd.cryptos.scryptosbt.segment.service

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants
import com.minhdd.cryptos.scryptosbt.domain.BeforeSplit
import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.DoubleImplicit
import org.apache.spark.sql.Dataset
import SegmentHelper.linear
import com.minhdd.cryptos.scryptosbt.tools.SeqHelper
import constants.numberOfCryptoForStability
import org.joda.time.DateTime

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

  def generalCut(ds: Dataset[Seq[BeforeSplit]]): Dataset[Seq[BeforeSplit]] = {
    import ds.sparkSession.implicits._
    ds.flatMap(s => generalCut(Seq(s)))
  }

  def generalCut(seq: Seq[Seq[BeforeSplit]]): Seq[Seq[BeforeSplit]] = {
    if (seq.size <= 1 && seq.head.size <= 1) {
      seq
    } else {
      if (seq.forall(linear)) {
//        seq.flatMap(s => cutWithTwoPointsMax(s, getCutPointsWhenLinear(s)))
        seq
      } else {
        val cuts: Seq[Seq[BeforeSplit]] = seq.flatMap(s => {
          if (linear(s)) {
            Seq(s)
          } else {
            getCutPointsAndCut(s)
          }
        })
        generalCut(cuts)
      }
    }
  }

  private def getCutPointsAndCut(seq: Seq[BeforeSplit]): Seq[Seq[BeforeSplit]] = {
    val splitPoints: Seq[Int] = getSimpleCutPoints(seq).sortWith(_ < _)

    if (splitPoints.nonEmpty) {
      if (splitPoints.size == 1) {
        cutOnePoint(seq, splitPoints.head)
      } else {
        cutManyPoints(seq, splitPoints)
      }
    } else {
      val slidePoint: Seq[Int] = if (seq.head.value > seq.last.value) {
        windowSlideFromRightSearch(seq, seq.size)
      } else {
        windowSlideFromLeftSearch(seq, 0)
      }
      val point: Seq[Int] = if (slidePoint.nonEmpty) {
        slidePoint
      } else {
        hardSearch(seq)
      }.filter(p => linear(cutOnePoint(seq, p).head)).sortWith(_ > _).headOption.toSeq

      if (point.isEmpty) {
        println("problem not cut point found for : " + seq.size + " - " + seq.head.datetime)
        Seq(seq)
      } else {
        cutOnePoint(seq, point.head)
      }
    }
  }

  //begin cut methods

  def cutManyPoints(seq: Seq[BeforeSplit], cutPoints: Seq[Int]): Seq[Seq[BeforeSplit]] = {
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

  def cutWithTwoPointsMax(seq: Seq[BeforeSplit], cutPoints: Seq[Int]): Seq[Seq[BeforeSplit]] = {
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

  //end cut methods

  private def windowSlideFromRightSearch(seq: Seq[BeforeSplit], previousSplitPosition: Int): Seq[Int] = {
    if (previousSplitPosition <= numberOfCryptoForStability) {
      Nil
    } else {
      val newSplitPosition: Int = previousSplitPosition - numberOfCryptoForStability
//        println("windowSlideFromRightSearch with head : " + seq.head.datetime + " - " + newSplitPosition)
      recursiveSlideSearch(newSplitPosition, windowSlideFromRightSearch, seq)
    }
  }

  private def windowSlideFromLeftSearch(seq: Seq[BeforeSplit], previousSplitPosition: Int): Seq[Int] = {
    if (seq.length - previousSplitPosition <= numberOfCryptoForStability) {
      Nil
    } else {
      val newSplitPosition = previousSplitPosition + numberOfCryptoForStability
      recursiveSlideSearch(newSplitPosition, windowSlideFromLeftSearch, seq)
    }
  }

  private def recursiveSlideSearch(newSplitPosition: Int, searchFunction: (Seq[BeforeSplit], Int) => Seq[Int], seq: Seq[BeforeSplit]): Seq[Int] = {
    val (smallerSeq1, smallerSeq2): (Seq[BeforeSplit], Seq[BeforeSplit]) = seq.splitAt(newSplitPosition)

    val splitPoints: Seq[Int] =
      Seq((smallerSeq1 :+ smallerSeq2.head, 0), (smallerSeq2, smallerSeq1.size))
        .flatMap(x => getSimpleCutPointsWithOffset(x._1, x._2)).sortWith(_ < _)

    if (splitPoints.nonEmpty) {
      splitPoints
    } else {
      searchFunction(seq, newSplitPosition)
    }
  }

  private def hardSearch(seq: Seq[BeforeSplit], numberOfSplit: Int = 2): Seq[Int] = {
//    println("hardSearch : " + seq.size + " - "  + numberOfSplit)
//    println(seq.head.datetime, seq.last.datetime)
    val splits: Seq[(Seq[BeforeSplit], Int)] = SeqHelper.splitWithOffset(seq, 0, numberOfSplit)
    val potentialCutPoints: Seq[Int] = splits.flatMap(x => getSimpleCutPointsWithOffset(x._1, x._2)).sortWith(_ < _)
//    println("potential cut points : " + potentialCutPoints + " - " + potentialCutPoints.map(seq.apply(_).datetime))
    val cuts: Seq[Seq[BeforeSplit]] = Splitter.cutManyPoints(seq, potentialCutPoints)
    if (cuts.forall(linear)) {
//      println("all linear with : " + potentialCutPoints)
      potentialCutPoints
    } else {
      if (numberOfSplit >= seq.length / 2) {
        veryHardSearch(seq, Nil)
      } else {
        hardSearch(seq, numberOfSplit + 1)
      }
    }
  }


  /****** very hard search ***/

  private def veryHardSearch(seq: Seq[BeforeSplit], cutPoints: Seq[Int]): Seq[Int] = {
//    println("very hardSearch : " + seq.size + " - "  + cutPoints)
    val cuts: Seq[Seq[BeforeSplit]] = cutManyPoints(seq, cutPoints)
    if (cuts.forall(linear)) {
      cutPoints
    } else {

      val newPositions: Seq[Int] =
        cuts.indices.filter(i => !linear(cuts(i))).flatMap(notLinearPosition => {
          val notLinear: Seq[BeforeSplit] = cuts(notLinearPosition)
          val offset = if (notLinearPosition == 0) 0 else cutPoints(notLinearPosition - 1)
          pointsToVeryHardCut(notLinear, offset)
        })

      if (newPositions.isEmpty) {
        cutPoints
      } else {
        val newCutPoints: Seq[Int] = Seq(cutPoints, newPositions).flatten.sortWith { case (x, y) => x < y }
        veryHardSearch(seq, newCutPoints)
      }
    }
  }

  private def pointsToVeryHardCut(seq: Seq[BeforeSplit], offset: Int, numberOfSplit: Int = 2): Seq[Int] = {
    val splits: Seq[(Seq[BeforeSplit], Int)] = SeqHelper.splitWithOffset(seq, offset, numberOfSplit)
    val cutPoints: Seq[Int] = splits.flatMap(x => getOneCutPoint(x._1, x._2))
    if (cutPoints.isEmpty) {
      if (numberOfSplit >= seq.length/2) {
        Nil
      } else {
        pointsToVeryHardCut(seq, offset, numberOfSplit + 1)
      }
    } else {
      cutPoints
    }
  }

  private def getOneCutPoint(seq: Seq[BeforeSplit], offset: Int): Option[Int] = {
    if (seq.size <= 2) {
      None
    } else {
      val points: Seq[Int] = getSimpleCutPoints(seq)
      if (points.isEmpty) {
        None
      } else {
        Option(points.head + offset)
      }
    }
  }
  /****** fin very hard search ***/

  def getSimpleCutPointsWithOffset(seq: Seq[BeforeSplit], offset: Int): Seq[Int] = {
//    println("get simple cut points : " + seq.head.datetime + " - " + seq.last.datetime + " - offset : " + offset)
    getSimpleCutPoints(seq).map(_ + offset)
  }

  def getSimpleCutPoints(seq: Seq[BeforeSplit]): Seq[Int] = {
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

  def getCutPointsWhenLinear(seq: Seq[BeforeSplit]): Seq[Int] = {
    if (seq.isEmpty) {
      Nil
    } else {
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


  // FOR TEST ONLY
  def simpleCut(seq: Seq[BeforeSplit]): Seq[Seq[BeforeSplit]] = {
    if (seq.size <= 2 || linear(seq)) {
      Seq(seq)
    } else {
      getCutPointsAndCut(seq)
    }
  }

}
