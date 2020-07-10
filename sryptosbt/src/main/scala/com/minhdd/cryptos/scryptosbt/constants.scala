package com.minhdd.cryptos.scryptosbt

object constants {
    val beforesplits = "beforesplits"
    val evolutionNone = "-"
    val evolutionUp = "up"
    val evolutionDown = "down"
    val numberOfMinutesBetweenTwoElement: Int = 15
    // sampling every x minutes, 4 jours on one window
    val numberOfCryptoOnOneWindow: Int = (4 * 24 * 60 / numberOfMinutesBetweenTwoElement)
    
    val minDeltaValue = 150
    val relativeMinDelta = 0.02
    
    val smallSegmentsFolder = "15/20200710165924"
}
