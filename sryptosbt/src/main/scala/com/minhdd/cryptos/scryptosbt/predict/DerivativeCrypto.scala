package com.minhdd.cryptos.scryptosbt.predict

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoValue, Margin}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

object DerivativeCrypto {
    def toX(c: Crypto) = c.cryptoValue.datetime.getTime.toDouble / 1000000
    def toY(c: Crypto) = c.cryptoValue.value
    def encoder(ss: SparkSession): Encoder[(Crypto, Double)] = {
        import ss.implicits._
        implicitly[Encoder[(Crypto, Double)]]
    }
    
    def deriveWithWindow(d: Dataset[Crypto], ss: SparkSession): DataFrame = {
        val deriveColumnName = "derive"
        val datetimeColumnName = "datetime"
        val valueColumnName = "value"
        val first = d.first()
        
        import org.apache.spark.sql.expressions.Window
        val window = Window.orderBy(datetimeColumnName)
        
        import org.apache.spark.sql.functions.{lag, lead, col}
        import ss.implicits._
        
        d.map(c => (c, c.cryptoValue.datetime.getTime.toDouble / 1000000))
          .withColumnRenamed("_2", datetimeColumnName)
          .withColumnRenamed("_1", "crypto")
          .withColumn(valueColumnName, col("crypto.cryptoValue.value"))
          .withColumn(deriveColumnName,
            (lead(valueColumnName, 1).over(window) - lag(valueColumnName, 1).over(window))
              /
               (lead(datetimeColumnName, 1).over(window) - lag(datetimeColumnName, 1).over(window))
        )
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
                
                (actualGet, derived)
            }
        })(encoder(ss))
}
