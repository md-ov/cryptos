package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.parquet.PartitionsIntegrator.getPartitionFromPath
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class PartitionsIntegratorTest extends FunSuite {

    val ss: SparkSession = SparkSession.builder().appName("toParquet").master("local[*]").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    test("testGetPartitionFromPath test path does not exist") {
        val a: Option[Dataset[Crypto]] = getPartitionFromPath(ss, "aaaaaa")
        assert(a.isEmpty)
    }

    test("test partition found") {
        val a: Option[Dataset[Crypto]] = getPartitionFromPath(ss, "file://" + getClass.getResource("/parquets/parquet").getPath)
        assert(a.nonEmpty)
        assert(a.get.count == 720)
    }

    test("test partition not found") {
        println(getClass.getResource("/parquets/nothing").getPath)
        val a: Option[Dataset[Crypto]] = getPartitionFromPath(ss, "file://" + getClass.getResource("/parquets/nothing").getPath)
        assert(a.isEmpty) 
    }


}
