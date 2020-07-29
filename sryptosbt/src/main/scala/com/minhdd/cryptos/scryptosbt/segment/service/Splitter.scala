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

    def generalCut(ds: Dataset[Seq[BeforeSplit]]): Dataset[Seq[BeforeSplit]] = {
        import ds.sparkSession.implicits._
        val count: Long = ds.count

        val smallers: Dataset[Seq[BeforeSplit]] =
            ds.flatMap(s => if (linear(s)) {
                cutWithTwoPointsMax(s, getCutPointsWhenLinear(s))
            } else {
                cutWithTwoPointsMax(s, getCutPoints(s))
            })

        if (smallers.count > count) {
            generalCut(smallers)
        } else {
            ds.flatMap(s => if (!linear(s)) hardCut(s, Nil) else Seq(s))
        }
    }

    def generalCut(seq: Seq[Seq[BeforeSplit]]): Seq[Seq[BeforeSplit]] = {
        if (seq.size == 1 && seq.head.size == 1) {
            seq
        } else {
            val smallers: Seq[Seq[BeforeSplit]] =
                seq.flatMap(s => if (linear(s)) {
//                    println("seq is linear => getCutPointsWhenLinear " + s.head.datetime + " -> " + s.last.datetime)
                    cutWithTwoPointsMax(s, getCutPointsWhenLinear(s))
                } else {
//                    println("seq is not linear => getCutPoints " + s.head.datetime + " -> " + s.last.datetime)
                    cutWithTwoPointsMax(s, getCutPoints(s))
                })

            if (smallers.length > seq.length) {
//                println("it is cut so continue" + seq.length + " -> " + smallers.length)
                generalCut(smallers)
            } else {
//                println("it is not cut after some simple search so hardcut")
                seq.flatMap(s => if (!linear(s)) hardCut(s, Nil) else Seq(s))
            }
        }
    }

    private def hardCut(seq: Seq[BeforeSplit], cutPoints: Seq[Int]): Seq[Seq[BeforeSplit]] = {
//        println("hardcut : " + seq.head.datetime + " -> " + seq.last.datetime + " with points : " + cutPoints + " - " + cutPoints.map(x => seq.apply(x).datetime))
        val cuts: Seq[Seq[BeforeSplit]] = cutManyPoints(seq, cutPoints)
        if (cuts.forall(linear)) {
//            println("hardcut : all is linear so return cuts")
            cuts
        } else {
//            println("hardcut : there is some segment not linear")
//            println("ignore cut points : " + cutPoints)
            val firstNotLinear: Int = cuts.indices.find(i => !linear(cuts(i))).get
            val notLinear: Seq[BeforeSplit] = cuts(firstNotLinear)
//            println("not linear : " + notLinear.head.datetime + " -> " + notLinear.last.datetime)
            val offset = if (firstNotLinear == 0) 0 else cutPoints(firstNotLinear - 1)
            val newPositions: Seq[Int] = pointsToHardCut(notLinear, offset)
            if (newPositions.isEmpty) {
//                println("can not cut for this segment : "+ notLinear.head.datetime + " -> " + notLinear.last.datetime)
                cuts
            } else {
                val newCutPoints: Seq[Int] = Seq(newPositions).flatten.sortWith { case (x, y) => x < y }
                hardCut(seq, newCutPoints)
            }
        }
    }

    private def pointsToHardCut(seq: Seq[BeforeSplit], offset: Int, numberOfSplit: Int = 2): Seq[Int] = {
        val splits: Seq[(Seq[BeforeSplit], Int)] = SeqHelper.splitWithOffset(seq, offset, numberOfSplit)
        val cutPoints: Seq[Int] = splits.flatMap(x => getOneCutPoint(x._1, x._2))
        if (cutPoints.isEmpty) {
            if (numberOfSplit >= seq.length/2) {
                Nil
            } else {
                pointsToHardCut(seq, offset, numberOfSplit + 1)
            }
        } else {
            cutPoints
        }
    }

    private def getOneCutPoint(seq: Seq[BeforeSplit], offset: Int): Option[Int] = {
        if (seq.size <= 2) {
            None
        } else {
            val points: Seq[Int] = getCutPoints(seq)
            if (points.isEmpty) {
                None
            } else {
                Option(points.head + offset)
            }
        }
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

        if (cutPoints.length == 1) {
            cutOnePoint(seq, cutPoints.head)
        } else if (cutPoints.length == 2) {
            val splitPointElement1 = seq.apply(cutPoints.head).copy(isEndOfSegment = true)
            val splitPointElement2 = seq.apply(cutPoints.last).copy(isEndOfSegment = true)
            Seq(seq.slice(0, cutPoints.head) :+ splitPointElement1,
                seq.slice(cutPoints.head, cutPoints.last) :+ splitPointElement2,
                seq.slice(cutPoints.last, length))
        } else {
            Seq(seq)
        }
    }

    /**
     *
     * get cut points methods
     *
     */

    private def getCutPoints(seq: Seq[BeforeSplit]): Seq[Int] = {
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

        val splitPoints: Seq[Int] = if (superiorSplit.isEmpty && inferiorSplit.isEmpty) {
            if (superiorMaybeSplit._1 - inferiorMaybeSplit._1 >= constants.relativeMinDelta) {
                Seq(superiorMaybeSplit._2, inferiorMaybeSplit._2)
                  .filterNot(x => x == 0)
                  .filterNot(x => x == seq.length - 1)
                  .sortWith(_ < _)
            } else {
                Nil
            }
        } else {
            Seq(superiorSplit, inferiorSplit).flatten.sortWith(_ < _)
        }

        splitPoints
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
