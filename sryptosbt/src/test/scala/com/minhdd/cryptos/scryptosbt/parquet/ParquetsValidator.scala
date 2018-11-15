package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.parquet.Crypto.getPartitionFromPath
import com.minhdd.cryptos.scryptosbt.tools.Files
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class ParquetsValidator 
  extends FunSuite 
{
    
    val ss: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    
    def countParquetsLine(path: String): Long = {
        val allDirs: Seq[String] = Files.getAllDir(path)
        val allDs: Seq[Dataset[Crypto]] = allDirs.flatMap(getPartitionFromPath(ss,_))
        allDs.map(_.count()).sum
    }
    
}
