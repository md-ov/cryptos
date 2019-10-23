package com.minhdd.cryptos.scryptosbt.tools

import com.minhdd.cryptos.scryptosbt.domain.Crypto
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Sparks {
    
    def merge(srcPath: String, dstPath: String): Unit =  {
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(hadoopConfig)
        FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
    }
    
    
    def parquetFromDs(ds: Dataset[_], parquetPath: String) = {
        ds.write.parquet(parquetPath)
    }
    
    def csvFromDS(ds: Dataset[_], csvPath: String) = {
        ds.coalesce(1)
          .write.format("com.databricks.spark.csv")
          .save(csvPath)
        merge(csvPath, csvPath + ".csv")
    }
    
    def csvFromDSCrypto(ss: SparkSession, csvPath: String, ds: Dataset[Crypto]) = {
        import ss.implicits._
        val dsString: Dataset[String] = ds.map(_.flatten.toLine())
        csvFromDS(dsString, csvPath)
    }
    
    def csvFromSeqCrypto(ss: SparkSession, csvPath: String, seq: Seq[Crypto]) = {
        val ds: Dataset[Crypto] = ss.createDataset(seq)(Crypto.encoder(ss))
        import ss.implicits._
        val dsString: Dataset[String] = ds.map(_.flatten.toLine())
        csvFromDS(dsString, csvPath)
    }
    
    def csvFromDataframe(csvPath: String, df: DataFrame) = {
        df.coalesce(1).write.option("delimiter", ";").csv(csvPath)
    }
    
}
