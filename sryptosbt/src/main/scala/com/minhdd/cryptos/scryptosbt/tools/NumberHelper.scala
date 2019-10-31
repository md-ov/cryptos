package com.minhdd.cryptos.scryptosbt.tools

import com.minhdd.cryptos.scryptosbt.tools.Implicits.TryImplicit

import scala.util.Try

object NumberHelper {
    
    def twoDigit(s: String): String = {
        if (s.length == 1) "0" + s
        else s.toString
    }
    
    def fromStringToInt(s: String): Option[Int] = {
        Try {
            s.toInt
        }.mapException(e => new Exception(s"Not a number : $s", e)).toOption
    }
    
    implicit class SeqDoubleImplicit(input: Seq[Double]) {
        
        def avg: Double = input.sum / input.size
        
        def variance: Double = input.map(d => math.pow(d - input.avg, 2)).sum / input.size
        
        def standardDeviation: Double = math.sqrt(input.variance)
        
        def getMax(windowLeftSize: Int, windowRightSize: Int): Seq[Int] = {
            input.indices.map(i => {
                val maxLeftPosition: Int = getMaxLeftPosition(i, windowLeftSize)
                val maxRightPosition: Int = getMaxRightPosition(i, windowRightSize)
                getMaxPosition(maxLeftPosition, maxRightPosition)
            })
        }
        
        def getMin(windowLeftSize: Int, windowRightSize: Int): Seq[Int] = {
            input.indices.map(i => {
                val minLeftPosition: Int = getMinLeftPosition(i, windowLeftSize)
                val minRightPosition: Int = getMinRightPosition(i, windowRightSize)
                getMinPosition(minLeftPosition, minRightPosition)
            })
        }
        
        def getMaxFast(windowLeftSize: Int, windowRightSize: Int): Seq[Int] = {
            if (windowLeftSize == 1) {
                getMaxRightFast(windowRightSize)
            } else if (windowRightSize == 1) {
                getMaxLeftFast(windowLeftSize)
            } else {
                getMaxFast(
                    getMaxLeftFast(windowLeftSize),
                    getMaxRightFast(windowRightSize)
                )
            }
        }
        
        def getMinFast(windowLeftSize: Int, windowRightSize: Int): Seq[Int] = {
            if (windowLeftSize == 1) {
                getMinRightFast(windowRightSize)
            } else if (windowRightSize == 1) {
                getMinLeftFast(windowLeftSize)
            } else {
                getMinFast(
                    getMinLeftFast(windowLeftSize),
                    getMinRightFast(windowRightSize)
                )
            }
        }
        
        private def getMaxLeftPosition(actualPosition: Int, windowLeftSize: Int): Int = {
            if (actualPosition == 0) {
                0
            } else {
                var maxLeft = actualPosition
                for (j <- actualPosition - windowLeftSize + 1 until actualPosition) {
                    if (j >= 0 && input.apply(j) > input(maxLeft)) maxLeft = j
                }
                maxLeft
            }
        }
        
        private def getMaxRightPosition(actualPosition: Int, windowRightSize: Int) = {
            if (actualPosition == input.length - 1) {
                input.length - 1
            } else {
                var maxRight = actualPosition
                for (j <- actualPosition + 1 until actualPosition + windowRightSize) {
                    if (j < input.length && input.apply(j) > input(maxRight)) maxRight = j
                }
                maxRight
            }
        }
        
        private def getMinLeftPosition(actualPosition: Int, windowLeftSize: Int): Int = {
            if (actualPosition == 0) {
                0
            } else {
                var minLeft = actualPosition
                for (j <- actualPosition - windowLeftSize + 1 until actualPosition) {
                    if (j >= 0 && input.apply(j) < input(minLeft)) minLeft = j
                }
                minLeft
            }
        }
        
        private def getMinRightPosition(actualPosition: Int, windowRightSize: Int) = {
            if (actualPosition == input.length - 1) {
                input.length - 1
            } else {
                var minRight = actualPosition
                for (j <- actualPosition + 1 until actualPosition + windowRightSize) {
                    if (j < input.length && input.apply(j) < input(minRight)) minRight = j
                }
                minRight
            }
        }
        
        private def getMaxFast(maxLeftPosition: Seq[Int], maxRightPosition: Seq[Int]): Seq[Int] = {
            input.indices.map(i => getMaxPosition(maxLeftPosition(i), maxRightPosition(i)))
        }
        
        private def getMinFast(minLeftPosition: Seq[Int], minRightPosition: Seq[Int]): Seq[Int] = {
            input.indices.map(i => getMinPosition(minLeftPosition(i), minRightPosition(i)))
        }
        
        private def getMaxLeftFast(windowLeftSize: Int): Seq[Int] = {
            if (windowLeftSize == 1) {
                input.indices
            } else {
                val previous = getMaxLeftFast(windowLeftSize - 1)
                input.indices.map(i => getMaxPosition(previous(i), i - windowLeftSize + 1))
            }
        }
        
        private def getMinLeftFast(windowLeftSize: Int): Seq[Int] = {
            if (windowLeftSize == 1) {
                input.indices
            } else {
                val previous = getMinLeftFast(windowLeftSize - 1)
                input.indices.map(i => getMinPosition(previous(i), i - windowLeftSize + 1))
            }
        }
        
        private def getMaxRightFast(windowRightSize: Int): Seq[Int] = {
            if (windowRightSize == 1) {
                input.indices
            } else {
                val previous = getMaxRightFast(windowRightSize - 1)
                input.indices.map(i => getMaxPosition(previous(i), i + windowRightSize - 1))
            }
        }
        
        private def getMinRightFast(windowRightSize: Int): Seq[Int] = {
            if (windowRightSize == 1) {
                input.indices
            } else {
                val previous = getMinRightFast(windowRightSize - 1)
                input.indices.map(i => getMinPosition(previous(i), i + windowRightSize - 1))
            }
        }
        
        private def getMinPosition(position1: Int, position2: Int): Int = {
            if (position1 < 0 || position1 >= input.length) {
                position2
            } else if (position2 < 0 || position2 >= input.length) {
                position1
            } else if (input(position1) < input(position2)) {
                position1
            } else {
                position2
            }
        }
        
        private def getMaxPosition(position1: Int, position2: Int): Int = {
            if (position1 < 0 || position1 >= input.length) {
                position2
            } else if (position2 < 0 || position2 >= input.length) {
                position1
            } else if (input(position1) > input(position2)) {
                position1
            } else {
                position2
            }
        }
        
        def linear(margin: Double): Boolean = {
            val head: Double = input.head
            val last: Double = input.last
            
            input.indices.forall(i => {
                i == 0 ||
                  (last > head && input.apply(i) > input.apply(i - 1)) ||
                  (last < head && input.apply(i) < input.apply(i - 1)) ||
                  (input.apply(i).relativeVariation(input.apply(i - 1)) <= margin)
            })
        }
    }
    
    implicit class DoubleImplicit(input: Double) {
        def relativeVariation(other: Double): Double = {
            math.abs(input - other) / input
        }
    }
}