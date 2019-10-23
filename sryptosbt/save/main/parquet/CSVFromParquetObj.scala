package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.tools.SparkHelper
import com.minhdd.cryptos.scryptosbt.CSVFromParquet
import com.minhdd.cryptos.scryptosbt.domain.Crypto
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object CSVFromParquetObj {

    def merge(srcPath: String, dstPath: String): Unit =  {
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(hadoopConfig)
        FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
    }
    
    def run(args: CSVFromParquet, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        import ss.implicits._
        
        val ds: Dataset[Crypto] = ss.read.parquet(args.parquetPath).as[Crypto]
        ds.toDF().show(false)
    
        SparkHelper.csvFromDSCrypto(ss, args.csvpath, ds)
    
        "status|SUCCESS"
    }
}
