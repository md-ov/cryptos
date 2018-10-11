package com.minhdd.cryptos.scryptosbt.predict

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoValue, Margin}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object DerivativeCrypto {
    def toX(c: Crypto) = c.cryptoValue.datetime.getTime.toDouble / 1000000
    def toY(c: Crypto) = c.cryptoValue.value
    def encoder(ss: SparkSession): Encoder[AnalyticsCrypto] = {
        import ss.implicits._
        implicitly[Encoder[AnalyticsCrypto]]
    }
    
    def deriveWithWindow(d: Dataset[Crypto], ss: SparkSession): Dataset[AnalyticsCrypto] = {
        val first = d.first()
        val deriveColumnName = "derive"
        val datetimeColumnName = "cryptoValue.datetime"
        val valueColumnName = "cryptoValue.value"
        import org.apache.spark.sql.expressions.Window
        val window = Window.orderBy(datetimeColumnName)
        import org.apache.spark.sql.functions.{lag, lead}
        d.withColumn(deriveColumnName,
            (lead(valueColumnName, 1).over(window) - lag(valueColumnName, 1).over(window))
              /(lead(datetimeColumnName, 1).over(window) - lag(datetimeColumnName, 1).over(window))
        ).map(row => {
            val derived = row.getAs[Double](deriveColumnName)
            val crypto = first.copy(cryptoValue = CryptoValue(
                datetime = row.getAs[Timestamp](datetimeColumnName),
                value = row.getAs[Double](valueColumnName),
                margin = row.getAs[Option[Margin]]("cryptoValue.margin"),
                volume = row.getAs[Double]("cryptoValue.volume")
            ))
            AnalyticsCrypto(crypto, derived)
        })(encoder(ss))
    }
    
    def derive(d: Dataset[Crypto], ss: SparkSession): Dataset[AnalyticsCrypto] =
        d.mapPartitions(iterator => new Iterator[AnalyticsCrypto]{
            var previous: Option[Crypto] = None
            var actual: Option[Crypto] = if (iterator.hasNext) Some(iterator.next) else None
            override def hasNext: Boolean = actual.isDefined
            override def next(): AnalyticsCrypto = {
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
                
                return AnalyticsCrypto(actualGet, derived)
            }
        })(encoder(ss))
}
