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

  private val numberOfMinutesForStability: Int = 60 * 12
  val numberOfCryptoForStability = (60 * 4 / numberOfMinutesBetweenTwoElement)

  //    val smallSegmentsFolder = "15/20200724164032"
  //    val smallSegmentsFolder = "15/20200819181046"

  val smallSegmentsFolder = "15/20201108103917"

}
