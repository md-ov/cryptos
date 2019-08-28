package com.minhdd.cryptos.scryptosbt.domain

import com.minhdd.cryptos.scryptosbt.parquet.Crypto


case class Analytics(
                      derive: Option[Double], 
                      secondDerive: Option[Double],
                      numberOfStableDay: Option[Long],
                      importantChange: Option[Boolean],
                      variation: Option[Double],
                      evolution: Option[String]
                    )

case class AnalyticsCrypto (crypto: Crypto, analytics: Analytics)

object AnalyticsCrypto {
    def apply(crypto: Crypto, derive: Double): AnalyticsCrypto =
        AnalyticsCrypto(crypto, new Analytics(Option(derive), None, None, None, None, None))
}