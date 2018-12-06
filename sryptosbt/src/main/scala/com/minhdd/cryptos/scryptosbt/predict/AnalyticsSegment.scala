package com.minhdd.cryptos.scryptosbt.predict

case class AnalyticsSegment(
    begin: AnalyticsCrypto,
    end: AnalyticsCrypto,
    numberOfElement: Int) {
}

object AnalyticsSegment {
    def apply(cryptos: Seq[AnalyticsCrypto]): AnalyticsSegment = 
        new AnalyticsSegment(
            begin = cryptos.head, 
            end = cryptos.last, 
            numberOfElement = cryptos.size
        )
}
