package com.minhdd.cryptos.scryptosbt.parquet

import java.io.File

import com.minhdd.cryptos.scryptosbt.parquet.Crypto.getPartitionFromPath
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.FunSuite

class ParquetsValidator 
  extends FunSuite 
{
    
    val ss: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    
    def getRecursiveDirs(directory: File): Seq[File] = {
        if (directory.isFile) {
            Seq()
        } else if (directory.getName == "parquet") {
            Seq(directory)
        } else {
            val list = directory.listFiles()
            list.map(getRecursiveDirs).reduce(_ ++ _)
        }
    }
    
    def getAllDir(path: String): Seq[String] = {
        val d = new File(path)
        getRecursiveDirs(d).map(_.getAbsolutePath)
    }
    
    def countParquetsLine(path: String): Long = {
        val allDirs: Seq[String] = getAllDir(path)
        val allDs: Seq[Dataset[Crypto]] = allDirs.flatMap(getPartitionFromPath(ss,_))
        allDs.map(_.count()).sum
    }
    test("test") {
//    def test() {
        val allLine: Long = countParquetsLine("D:\\ws\\cryptos\\data\\parquets\\BCH")
        assert(allLine == 2496545L) // 2498766 =? 2496 * 1000 + 545
    }
    
}
