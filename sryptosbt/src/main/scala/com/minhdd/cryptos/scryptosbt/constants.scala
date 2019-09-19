package com.minhdd.cryptos.scryptosbt

object constants {
    val dataDirectory = "D:\\ws\\cryptos\\data"
    val beforesplits = "beforesplits"
    val evolutionNone = "-"
    val evolutionUp = "up"
    val evolutionDown = "down"
    
    val numberOfCryptoOnOneWindow: Int = 4 * 24 *60 / 15 // sampling every 15 minutes, 4 jours on one window
    val minDeltaValue = 150
    val relativeMinDelta = 0.02
}
