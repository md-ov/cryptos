package com.minhdd.cryptos.scryptosbt.model.domain

case class Rates(
  truePositiveOnPositive: Double,
  trueNegativeOnNegative: Double,
  positiveRate: Double,
  nagativeRate: Double,
  trueRate: Double
)

