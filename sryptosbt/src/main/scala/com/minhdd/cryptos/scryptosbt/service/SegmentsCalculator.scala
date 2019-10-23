package com.minhdd.cryptos.scryptosbt.service

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, KrakenCrypto}
import com.minhdd.cryptos.scryptosbt.tools.Derivative
import com.minhdd.cryptos.scryptosbt.tools.NumberHelper.SeqDoubleImplicit

object SegmentsCalculator {
    
    def toBeforeSplits(krakenCryptos: Seq[KrakenCrypto]): Seq[BeforeSplit] = {
        if (krakenCryptos.isEmpty) {
            Seq()
        } else if (krakenCryptos.length == 1) {
            Seq(BeforeSplit(krakenCryptos.head))
        } else {
            val sortedKrakenCryptos: Seq[KrakenCrypto] = krakenCryptos.sortWith((k1, k2) => k1.datetime.before(k2.datetime))
            val timestamps: Seq[Timestamp] = sortedKrakenCryptos.map(_.datetime)
            val values: Seq[Double] = sortedKrakenCryptos.map(_.value)
            val premierDerives: Seq[Double] = Derivative.deriveTs(timestamps, values)
            val secondDerives: Seq[Double] = Derivative.deriveTs(timestamps, premierDerives)
            val maxIndices: Seq[Int] = values.getMaxFast(numberOfCryptoOnOneWindow / 2, numberOfCryptoOnOneWindow / 2)
            val minIndices: Seq[Int] = values.getMinFast(numberOfCryptoOnOneWindow / 2, numberOfCryptoOnOneWindow / 2)
            val variations: Seq[Double] = getVariations(values, maxIndices, minIndices)
            val isExtremums: Seq[Boolean] = getIsExtremums(values, maxIndices, minIndices)
            val importantChanges: Seq[Boolean] = getImportantChanges(values, isExtremums, variations)
            val seqEvolution: Seq[String] = getEvolutions(importantChanges, maxIndices, minIndices)
            
            sortedKrakenCryptos.indices.map(i => BeforeSplit(
                datetime = timestamps.apply(i),
                value = values.apply(i),
                evolution = seqEvolution.apply(i),
                variation = variations.apply(i),
                derive = Option(premierDerives.apply(i)),
                secondDerive = Option(secondDerives.apply(i)),
                ohlc_value = sortedKrakenCryptos.apply(i).ohlcValue,
                ohlc_volume = sortedKrakenCryptos.apply(i).ohlcVolume,
                volume = sortedKrakenCryptos.apply(i).volume,
                count = sortedKrakenCryptos.apply(i).count,
                importantChange = Option(importantChanges.apply(i))
            ))
        }
    }
    
    private def getVariations(values: Seq[Double], maxIndices: Seq[Int], minIndices: Seq[Int]): Seq[Double] = {
        values.indices.map(i => {
            val maxPosition: Int = maxIndices.apply(i)
            val minPosition: Int = minIndices.apply(i)
            if (minPosition <= maxPosition) {
                values(maxPosition) - values(minPosition)
            } else {
                values(minPosition) - values(maxPosition)
            }
        })
    }
    
    private def getIsExtremums(values: Seq[Double], maxIndices: Seq[Int], minIndices: Seq[Int]): Seq[Boolean] = {
        values.indices.map(i => maxIndices(i) == i || minIndices(i) == i)
    }
    
    private def getEvolutions(importantChanges: Seq[Boolean], maxIndices: Seq[Int], minPositions: Seq[Int]): Seq[String] = {
        if (importantChanges.length < 2) {
            importantChanges.map(_ => evolutionNone)
        } else {
            importantChanges.indices.map(i => {
                if (i == 0) {
                    evolutionNone
                } else if (importantChanges(i) && maxIndices(i) == i) {
                    evolutionUp
                } else if (importantChanges(i) && minPositions(i) == i) {
                    evolutionDown
                } else {
                    evolutionNone
                }
            })
        }
    }
    
    private def getImportantChanges(values: Seq[Double], isExtremums: Seq[Boolean], variations: Seq[Double]): Seq[Boolean] = {
        isExtremums.indices.map(i => isExtremums(i) && math.abs(variations(i) / values(i)) > relativeMinDelta)
    }
    
    private def split(beforeSplits: Seq[BeforeSplit]): Seq[Seq[BeforeSplit]] = {
        if (beforeSplits.isEmpty) {
            Seq()
        } else if (beforeSplits.size <= 2) {
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
