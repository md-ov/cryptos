package com.minhdd.cryptos.scryptosbt.domain

case class Rates(truePositiveOnPositive: Double,
                  trueNegativeOnNegative: Double,
                  positiveRate: Double,
                  negativeRate: Double,
                  trueRate: Double)

