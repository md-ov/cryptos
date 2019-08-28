package com.minhdd.cryptos.scryptosbt.tools

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class DerivativeTest extends FunSuite {
    
    test("test derivative seq") {
        val x = (-20 until 20).map(_.toDouble)
        val y = x.map(d => d * d + d)
        val expected: Seq[Double] = x.map(d => 2 * d + 1)
        val derived: Seq[Double] = Derivative.derive(x, y)
        println(expected)
        println(derived)
        assert(derived == expected)
    }
    
    test("test derivative 2 dataset") {
        val ss: SparkSession = SparkSession.builder().appName("derivation").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        
        val s: Seq[(Double, Double)] = (-20 until 20).map(_.toDouble).map(d => (d, d * d + d))
        val expected: Seq[Double] = s.map(d => 2 * d._1 + 1)
        println(expected)
        import ss.implicits._
        val ds: Dataset[(Double, Double)] = ss.createDataset(s)
        val derived: Seq[Double] = DerivativeDatasetDoubleDouble.derive2(ds, ss).map(_._2).collect().toSeq
        println(derived)
        assert(derived == expected)
    }
    
    test("test derivative dataset") {
        val ss: SparkSession = SparkSession.builder().appName("derivation").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        
        val s: Seq[(Double, Double)] = (-20 until 20).map(_.toDouble).map(d => (d, d * d + d))
        val expected: Seq[Double] = s.map(d => 2 * d._1 + 1)
        println(expected)
        import ss.implicits._
        val ds: Dataset[(Double, Double)] = ss.createDataset(s)
        val derived: Seq[Double] = DerivativeDatasetDoubleDouble.derive(ds.coalesce(1), ss).map(_._2).collect().toSeq
        println(derived)
        assert(derived == expected)
    }
    
    test("test derivative dataset with window") {
        val ss: SparkSession = SparkSession.builder().appName("derivation-with-window").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        
        val s: Seq[(Double, Double)] = (-20 until 20).map(_.toDouble).map(d => (d, d * d + d))
        val expected: Seq[Double] = s.map(d => 2 * d._1 + 1)
        println(expected)
        import ss.implicits._
        val ds: Dataset[(Double, Double)] = ss.createDataset(s)
        val derived: Seq[Double] = DerivativeDatasetDoubleDouble.deriveWithWindow(ds, ss)
        println(derived)
        assert(derived.slice(1, 39) == expected.slice(1, 39))
    }
    
}
