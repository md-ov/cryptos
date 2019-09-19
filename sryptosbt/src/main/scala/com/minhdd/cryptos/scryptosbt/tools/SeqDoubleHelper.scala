package com.minhdd.cryptos.scryptosbt.tools

object SeqDoubleHelper {
    
    private def getMaxLeftPosition(seqValues: Seq[Double], actualPosition: Int, windowLeftSize: Int): Int = {
        if (actualPosition == 0) {
            0
        } else {
            var maxLeft = actualPosition
            for (j <- actualPosition - windowLeftSize + 1 until actualPosition) {
                if (j >= 0 && seqValues.apply(j) > seqValues(maxLeft)) maxLeft = j
            }
            maxLeft
        }
    }
    
    private def getMaxRightPosition(seqValues: Seq[Double], actualPosition: Int, windowRightSize: Int) = {
        if (actualPosition == seqValues.length - 1) {
            seqValues.length - 1
        } else {
            var maxRight = actualPosition
            for (j <- actualPosition + 1 until actualPosition + windowRightSize) {
                if (j < seqValues.length && seqValues.apply(j) > seqValues(maxRight)) maxRight = j
            }
            maxRight
        }
    }
    
    def getMax(seqValues: Seq[Double], windowLeftSize: Int, windowRightSize: Int): Seq[Int] = {
        seqValues.indices.map(i => {
            val maxLeftPosition: Int = getMaxLeftPosition(seqValues, i, windowLeftSize)
            val maxRightPosition: Int = getMaxRightPosition(seqValues, i, windowRightSize)
            getMaxPosition(seqValues, maxLeftPosition, maxRightPosition)
        })
    }
    
    private def getMinLeftPosition(seqValues: Seq[Double], actualPosition: Int, windowLeftSize: Int): Int = {
        if (actualPosition == 0) {
            0
        } else {
            var minLeft = actualPosition
            for (j <- actualPosition - windowLeftSize + 1 until actualPosition) {
                if (j >= 0 && seqValues.apply(j) < seqValues(minLeft)) minLeft = j
            }
            minLeft
        }
    }
    
    private def getMinRightPosition(seqValues: Seq[Double], actualPosition: Int, windowRightSize: Int) = {
        if (actualPosition == seqValues.length - 1) {
            seqValues.length - 1
        } else {
            var minRight = actualPosition
            for (j <- actualPosition + 1 until actualPosition + windowRightSize) {
                if (j < seqValues.length && seqValues.apply(j) < seqValues(minRight)) minRight = j
            }
            minRight
        }
    }
    
    def getMin(seqValues: Seq[Double], windowLeftSize: Int, windowRightSize: Int): Seq[Int] = {
        seqValues.indices.map(i => {
            val minLeftPosition: Int = getMinLeftPosition(seqValues, i, windowLeftSize)
            val minRightPosition: Int = getMinRightPosition(seqValues, i, windowRightSize)
            getMinPosition(seqValues, minLeftPosition, minRightPosition)
        })
    }
    
    def getMaxFast(seqValues: Seq[Double], windowLeftSize: Int, windowRightSize: Int): Seq[Int] = {
        if (windowLeftSize == 1) {
            getMaxRightFast(seqValues, windowRightSize)
        } else if (windowRightSize == 1) {
            getMaxLeftFast(seqValues, windowLeftSize)
        } else {
            getMaxFast(
                seqValues,
                getMaxLeftFast(seqValues, windowLeftSize),
                getMaxRightFast(seqValues, windowRightSize)
            )
        }
    }
    
    def getMinFast(seqValues: Seq[Double], windowLeftSize: Int, windowRightSize: Int): Seq[Int] = {
        if (windowLeftSize == 1) {
            getMinRightFast(seqValues, windowRightSize)
        } else if (windowRightSize == 1) {
            getMinLeftFast(seqValues, windowLeftSize)
        } else {
            getMinFast(
                seqValues,
                getMinLeftFast(seqValues, windowLeftSize),
                getMinRightFast(seqValues, windowRightSize)
            )
        }
    }
    
    private def getMaxFast(seqValues: Seq[Double], maxLeftPosition: Seq[Int], maxRightPosition: Seq[Int]): Seq[Int] = {
        seqValues.indices.map(i => getMaxPosition(seqValues, maxLeftPosition(i), maxRightPosition(i)))
    }
    
    private def getMinFast(seqValues: Seq[Double], minLeftPosition: Seq[Int], minRightPosition: Seq[Int]): Seq[Int] = {
        seqValues.indices.map(i => getMinPosition(seqValues, minLeftPosition(i), minRightPosition(i)))
    }
    
    private def getMaxLeftFast(seqValues: Seq[Double], windowLeftSize: Int): Seq[Int] = {
        if (windowLeftSize == 1) {
            seqValues.indices
        } else {
            val previous = getMaxLeftFast(seqValues, windowLeftSize-1)
            seqValues.indices.map(i => getMaxPosition(seqValues, previous(i), i - windowLeftSize + 1))
        }
    }
    
    private def getMinLeftFast(seqValues: Seq[Double], windowLeftSize: Int): Seq[Int] = {
        if (windowLeftSize == 1) {
            seqValues.indices
        } else {
            val previous = getMinLeftFast(seqValues, windowLeftSize-1)
            seqValues.indices.map(i => getMinPosition(seqValues, previous(i), i - windowLeftSize + 1))
        }
    }
    
    private def getMaxRightFast(seqValues: Seq[Double], windowRightSize: Int): Seq[Int] = {
        if (windowRightSize == 1) {
            seqValues.indices
        } else {
            val previous = getMaxRightFast(seqValues, windowRightSize-1)
            seqValues.indices.map(i => getMaxPosition(seqValues, previous(i), i + windowRightSize - 1))
        }
    }
    
    private def getMinRightFast(seqValues: Seq[Double], windowRightSize: Int): Seq[Int] = {
        if (windowRightSize == 1) {
            seqValues.indices
        } else {
            val previous = getMinRightFast(seqValues, windowRightSize-1)
            seqValues.indices.map(i => getMinPosition(seqValues, previous(i), i + windowRightSize - 1))
        }
    }
    
    private def getMinPosition(seqValues: Seq[Double], position1: Int, position2: Int): Int = {
        if (position1 < 0 || position1 >= seqValues.length) {
            position2
        } else if (position2 < 0 || position2 >= seqValues.length) {
            position1
        } else if (seqValues(position1) < seqValues(position2)) {
            position1
        } else {
            position2
        }
    }
    
    private def getMaxPosition(seqValues: Seq[Double], position1: Int, position2: Int): Int = {
        if (position1 < 0 || position1 >= seqValues.length) {
            position2
        } else if (position2 < 0 || position2 >= seqValues.length) {
            position1
        } else if (seqValues(position1) > seqValues(position2)) {
            position1
        } else {
            position2
        }
    }
}
