package com.minhdd.cryptos.scryptosbt.math


import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

object Derivative {
    def derive(i: Seq[Double], j: Seq[Double]): Seq[Double] = {
        i.indices.map(indice => {
            val x = i.apply(indice)
            val y = j.apply(indice)
            val previousX = if (indice == 0) x else i.apply(indice - 1)
            val previousY = if (indice == 0) y else j.apply(indice - 1)
            val nextX = if (indice == i.length - 1) x else i.apply(indice + 1)
            val nextY = if (indice == i.length - 1) y else j.apply(indice + 1)
            if (indice == 0) ((nextY - y) / (nextX - x)) - (nextX - x)
            else if (indice == i.length - 1) ( (y - previousY) / (x - previousX) ) + (x - previousX)
            else (nextY - previousY) / (nextX - previousX)
        })
    }
}

object DerivativeDatasetDoubleDouble {
    type DD = (Double, Double)
    def toX(dd: DD) = dd._1
    def toY(dd: DD) = dd._2
    def encoder(ss: SparkSession): Encoder[(DD, Double)] = {
        import ss.implicits._
        implicitly[Encoder[(DD, Double)]]
    }
    
    def derive(d: Dataset[DD], ss: SparkSession): Dataset[(DD, Double)] =
        d.mapPartitions(iterator => new Iterator[(DD, Double)]{
            var previous: Option[DD] = None
            var actual: Option[DD] = if (iterator.hasNext) Some(iterator.next) else None
            override def hasNext: Boolean = actual.isDefined
            override def next(): (DD, Double) = {
                val actualGet: DD = actual.get
                val nextt: Option[DD] = if (iterator.hasNext) Some(iterator.next) else None
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
    
    def derive2(d: Dataset[DD], ss: SparkSession): Dataset[(DD, Double)] = {
        d.mapPartitions(p => {
            val v: Seq[DD] = p.toVector
            val r: Seq[(DD, Double)] = v.indices.map(indice => {
                val actual = v.apply(indice)
                val x: Double = toX(actual)
                val y: Double = toY(actual)
                val previousX = if (indice == 0) x else toX(v.apply(indice - 1))
                val previousY = if (indice == 0) y else toY(v.apply(indice - 1))
                val nextX = if (indice == v.length - 1) x else toX(v.apply(indice + 1))
                val nextY = if (indice == v.length - 1) y else toY(v.apply(indice + 1))
                val derived: Double = if (indice == 0) (nextY - y) / (nextX - x) - (nextX - x)
                else if (indice == v.length - 1) (y - previousY) / (x - previousX) + (x - previousX)
                else (nextY - previousY) / (nextX - previousX)
                (v.apply(indice), derived)
            })
            r.toIterator
        })(encoder(ss))
    }
    
    def deriveWithWindow(d: Dataset[DD], ss: SparkSession) = {
        import org.apache.spark.sql.expressions.Window
        val window = Window.orderBy("_1")
        import org.apache.spark.sql.functions.{lag, lead}
        
        d.withColumn("derive", 
            (lead("_2", 1).over(window) - lag("_2", 1).over(window))
              /(lead("_1", 1).over(window) - lag("_1", 1).over(window))
        ).collect().map(_.getAs[Double]("derive")).toSeq
    }
}
