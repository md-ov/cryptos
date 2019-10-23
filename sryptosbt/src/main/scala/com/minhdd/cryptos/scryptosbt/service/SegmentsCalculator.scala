package com.minhdd.cryptos.scryptosbt.service

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, KrakenCrypto, Segment}
import com.minhdd.cryptos.scryptosbt.tools.{Derivative, SeqDoubleHelper}

object SegmentsCalculator {
    
    def get(krakenCryptos: Seq[KrakenCrypto]): Seq[Segment] = {
        toSegments(toBeforeSplits(krakenCryptos))
    }
    
    def toSegments(beforeSplits: Seq[BeforeSplit]): Seq[Segment] = {
        val splits = split(beforeSplits)
        splits.map(beforeSplits => Segment(beforeSplits, beforeSplits.last))
    }
    

    
    def toBeforeSplits(krakenCryptos: Seq[KrakenCrypto]): Seq[BeforeSplit] = {
        if (krakenCryptos.isEmpty) {
            Seq()
        } else if (krakenCryptos.length == 1) {
            val krakenCrypto = krakenCryptos.head
            val beforeSplit = BeforeSplit(
                datetime = krakenCrypto.datetime,
                value = krakenCrypto.value,
                evolution = evolutionNone,
                variation = 0D,
                derive = Some(0D),
                secondDerive = Some(0),
                ohlc_value = krakenCrypto.ohlcValue,
                ohlc_volume = krakenCrypto.ohlcVolume,
                volume = krakenCrypto.volume,
                count = krakenCrypto.count,
                importantChange = None)
            Seq(beforeSplit)
        } else {
            val sortedKrakenCryptos: Seq[KrakenCrypto] = krakenCryptos.sortWith((k1, k2) => k1.datetime.before(k2.datetime))
            val seqTimestamp: Seq[Timestamp] = sortedKrakenCryptos.map(_.datetime)
            val seqValues: Seq[Double] = sortedKrakenCryptos.map(_.value)
            val premierDerives: Seq[Double] = Derivative.deriveTs(seqTimestamp, seqValues)
            val secondDerives: Seq[Double] = Derivative.deriveTs(seqTimestamp, premierDerives)
            val seqMaxPosition: Seq[Int] =
                SeqDoubleHelper.getMaxFast(seqValues, numberOfCryptoOnOneWindow/2, numberOfCryptoOnOneWindow/2)
            val seqMinPosition: Seq[Int] =
                SeqDoubleHelper.getMinFast(seqValues, numberOfCryptoOnOneWindow/2, numberOfCryptoOnOneWindow/2)
            val seqVariation: Seq[Double] = getVariations(seqValues, seqMaxPosition, seqMinPosition)
            val seqExtremum: Seq[Boolean] = getExtremums(seqValues, seqMaxPosition, seqMinPosition)
            val seqImportantChange: Seq[Boolean] = getImportantChanges(seqValues, seqExtremum, seqVariation)
            val seqEvolution: Seq[String] = getEvolutions(seqImportantChange, seqMaxPosition, seqMinPosition)
    
            sortedKrakenCryptos.indices.map(i => BeforeSplit(
                datetime = seqTimestamp.apply(i),
                value = seqValues.apply(i),
                evolution = seqEvolution.apply(i),
                variation = seqVariation.apply(i),
                derive = Option(premierDerives.apply(i)),
                secondDerive = Option(secondDerives.apply(i)),
                ohlc_value = sortedKrakenCryptos.apply(i).ohlcValue,
                ohlc_volume = sortedKrakenCryptos.apply(i).ohlcVolume,
                volume = sortedKrakenCryptos.apply(i).volume,
                count = sortedKrakenCryptos.apply(i).count,
                importantChange = Option(seqImportantChange.apply(i))
            ))
        }
    }
    
    def getVariations(seqValues: Seq[Double], seqMaxPosition: Seq[Int], seqMinPosition: Seq[Int]): Seq[Double] = {
        val indices = seqValues.indices

        indices.map(i => {
            val maxPosition: Int = seqMaxPosition.apply(i)
            val minPosition: Int = seqMinPosition.apply(i)
            if (minPosition <= maxPosition) {
                seqValues(maxPosition) - seqValues(minPosition)
            } else {
                seqValues(minPosition) - seqValues(maxPosition)
            }
        })
    }
    
    def getExtremums(seqValues: Seq[Double], seqMaxPosition: Seq[Int], seqMinPosition: Seq[Int]): Seq[Boolean] = {
        seqValues.indices.map(i => seqMaxPosition(i) == i || seqMinPosition(i) == i)
    }
    
    def getEvolutions(seqImportantChange: Seq[Boolean], seqMaxPosition: Seq[Int], seqMinPosition: Seq[Int]): Seq[String] = {
        if (seqImportantChange.length < 2) {
            seqImportantChange.map(_ => evolutionNone)
        } else {
            seqImportantChange.indices.map(i => {
                if (i == 0) {
                    evolutionNone
                } else if (seqImportantChange(i) && seqMaxPosition(i) == i) {
                    evolutionUp
                } else if (seqImportantChange(i) && seqMinPosition(i) == i) {
                    evolutionDown
                } else {
                    evolutionNone
                }
            })
        }
    }
    
    def getImportantChanges(seqValues:Seq[Double], seqExtremum: Seq[Boolean], seqVariation: Seq[Double]): Seq[Boolean] = {
        seqExtremum.indices.map(i => seqExtremum(i) && math.abs(seqVariation(i)/seqValues(i)) > relativeMinDelta)
    }
    
    private def split(beforeSplits: Seq[BeforeSplit]): Seq[Seq[BeforeSplit]] = {
        if (beforeSplits.isEmpty) {
            Seq()
        } else if (beforeSplits.size <= 2 ) {
            Seq(beforeSplits)
        } else {
            ???
        }
    }
    
    private def split(iterator: Iterator[BeforeSplit]): Iterator[Seq[BeforeSplit]] = {
        if (iterator.hasNext) {
            new Iterator[Seq[BeforeSplit]] {
                var last: BeforeSplit = iterator.next
                override def hasNext: Boolean = iterator.hasNext
                override def next(): Seq[BeforeSplit] = {
                    var nextSeq: Seq[BeforeSplit] = Seq(last)
                    var cut = false
                    while (iterator.hasNext && !cut) {
                        val actual = iterator.next()
                        nextSeq = nextSeq :+ actual
                        if (actual.value != last.value) {
                            val importantChange = actual.importantChange
                            if (importantChange.isDefined && importantChange.get) {
                                cut = true
                            }
                        }
                        last = actual
                    }
                    nextSeq
                }
            }
        } else {
            Iterator[Seq[BeforeSplit]]()
        }
    }
}
