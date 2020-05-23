package com.minhdd.cryptos.scryptosbt.tools

import com.minhdd.cryptos.scryptosbt.domain.{BeforeSplit, Crypto}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkHelper {
    
    def merge(srcPath: String, dstPath: String): Unit =  {
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(hadoopConfig)
        FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
    }

    def csvFromDS(ds: Dataset[_], csvPath: String): Unit = {
        ds.coalesce(1)
          .write.format("com.databricks.spark.csv")
          .save(csvPath)
        merge(csvPath, csvPath + ".csv")
    }
    
    def csvFromDSCrypto(ss: SparkSession, csvPath: String, ds: Dataset[Crypto]): Unit = {
        import ss.implicits._
        val dsString: Dataset[String] = ds.map(_.flatten.toLine())
        csvFromDS(dsString, csvPath)
    }
    
    def csvFromSeqBeforeSplit(ss: SparkSession, csvPath: String, seq: Seq[BeforeSplit]): Unit = {
        import ss.implicits._
        val ds: Dataset[BeforeSplit] = ss.createDataset(seq)
        val dsString: Dataset[String] = ds.map(_.toLine)
        csvFromDS(dsString, csvPath)
    }
    
    def csvFromSeqCrypto(spark: SparkSession, csvPath: String, seq: Seq[Crypto]): Unit = {
        import spark.implicits._

        val ds: Dataset[Crypto] = spark.createDataset(seq)
        import spark.implicits._
        val dsString: Dataset[String] = ds.map(_.flatten.toLine())
        csvFromDS(dsString, csvPath)
    }
    
    def csvFromDataframe(csvPath: String, df: DataFrame): Unit = {
        df.coalesce(1).write.option("delimiter", ";").csv(csvPath)
    }
}
