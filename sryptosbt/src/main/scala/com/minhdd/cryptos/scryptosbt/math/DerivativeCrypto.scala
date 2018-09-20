package com.minhdd.cryptos.scryptosbt.math

import com.minhdd.cryptos.scryptosbt.parquet.Crypto
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object DerivativeCrypto {
    def toX(c: Crypto) = c.cryptoValue.datetime.getTime.toDouble
    def toY(c: Crypto) = c.cryptoValue.value
    def encoder(ss: SparkSession): Encoder[(Crypto, Double)] = {
        import ss.implicits._
        implicitly[Encoder[(Crypto, Double)]]
    }
    
    def derive(d: Dataset[Crypto], ss: SparkSession): Dataset[(Crypto, Double)] =
        d.mapPartitions(iterator => new Iterator[(Crypto, Double)]{
            var previous: Option[Crypto] = None
            var actual: Option[Crypto] = if (iterator.hasNext) Some(iterator.next) else None
            override def hasNext: Boolean = actual.isDefined
            override def next(): (Crypto, Double) = {
                val actualGet: Crypto = actual.get
                val nextt: Option[Crypto] = if (iterator.hasNext) Some(iterator.next) else None
                val derived: Double =
                    if (previous.isEmpty) {
                        if (nextt.isEmpty) 1D else {
                            val nextGet = nextt.get
                            previous = actual
                            actual = nextt
                            (toY(nextGet) - toY(actualGet))/(toX(nextGet) - toX(actualGet)) - ( toX(nextGet) - toX(actualGet) )
                        }
                    } else if (nextt.isEmpty) {
                        val previousGet = previous.get
                        previous = actual
                        actual = nextt
                        (toY(actualGet) - toY(previousGet))/(toX(actualGet) - toX(previousGet)) + (toX(actualGet) - toX(previousGet))
                    } else {
                        val nextGet = nextt.get
                        val previousGet = previous.get
                        previous = actual
                        actual = nextt
                        (toY(nextGet) - toY(previousGet)) / (toX(nextGet) - toX(previousGet))
                    }
                
                return (actualGet, derived)
            }
        })(encoder(ss))
}
