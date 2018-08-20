package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.parquet.ParquetFromCSVObj.encoder
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object Test {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        import ss.implicits._
    
        val ds1: Dataset[Crypto] = ss.read.parquet("file:///home/mdao/minh/git/cryptos/data/parquets/parquet").as[Crypto]
        val ds2: Dataset[Crypto] = ss.read.parquet("file:///home/mdao/minh/git/cryptos/data/parquets/parquet").as[Crypto]
//        val ds2: Dataset[Crypto] = ss.read.parquet("file:///D:\\ws\\cryptos\\data\\parquets\\parquet2").as[Crypto]
        
//        val map = toPartitions(ds1, ds2)
//        val ds11: RDD[(CryptoPartitionKey, Crypto)] = ds1.toJavaRDD.rdd.map(c => ((c.partitionKey), c))
        
        val keys = getPartitionKeys(ss, ds1)
        println(keys)
        
        println(ds1.count())
        println(ds2.count())
        val ds = ds1.union(ds2)
        println(ds.count())
        ds.toDF().show(false)
    }

    def encoder(ss: SparkSession): Encoder[Seq[CryptoPartitionKey]] = {
        import ss.implicits._
        implicitly[Encoder[Seq[CryptoPartitionKey]]]
    }
    
    def reduceSeq(seq1: Seq[CryptoPartitionKey], seq2: Seq[CryptoPartitionKey]): Seq[CryptoPartitionKey] = {
        seq2.filter(c => !seq1.exists(c.equals(_))) ++ seq1
    }

    def getPartitionKeys(ss: SparkSession, ds: Dataset[Crypto]): Seq[CryptoPartitionKey] = {
        ds.map(c => Seq(c.partitionKey))(encoder(ss)).reduce((seq1, seq2) => reduceSeq(seq1, seq2))
    }
    
    def toPartitions(ds1: Dataset[Crypto], ds2: Dataset[Crypto]): Map[CryptoPartitionKey, Dataset[Crypto]] = {
          ???
    }
}
