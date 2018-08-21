package com.minhdd.cryptos.scryptosbt.parquet

import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

import scala.util.{Failure, Try}

object PartitionsIntegrator {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        val ds1: Dataset[Crypto] = getPartitionFromPath(ss, "file:///home/mdao/minh/git/cryptos/data/parquets/parquet").get
        toPartitions(ss, "file:///home/mdao/minh/git/cryptos/data/parquets/", ds1)
    }
    
    def run(ss: SparkSession, ds: Dataset[Crypto], parquetsDir: String): Unit = {
        val partitions: Seq[(CryptoPartitionKey, Dataset[Crypto])] = toPartitions(ss, parquetsDir, ds)
        
        partitions.foreach(partition => {
            partition._2.write.mode(SaveMode.Overwrite).parquet(partition._1.getPartitionPath(parquetsDir))
        })
    }

    
    def reduceSeq(seq1: Seq[CryptoPartitionKey], seq2: Seq[CryptoPartitionKey]): Seq[CryptoPartitionKey] = {
        seq2.filter(c => !seq1.exists(c.equals(_))) ++ seq1
    }

    def getPartitionKeys(ss: SparkSession, ds: Dataset[Crypto]): Seq[CryptoPartitionKey] = {
        
        def encoder(ss: SparkSession): Encoder[Seq[CryptoPartitionKey]] = {
            import ss.implicits._
            implicitly[Encoder[Seq[CryptoPartitionKey]]]
        }
        
        ds.map(c => Seq(c.partitionKey))(encoder(ss))
          .reduce((seq1, seq2) => reduceSeq(seq1, seq2))
    }

    implicit class TryOps[T](val t: Try[T]) extends AnyVal {
        def mapException(f: Throwable => Throwable): Try[T] = {
            t.recoverWith({ case e => Failure(f(e)) })
        }
    }
    
    def getPartitionFromPath(ss: SparkSession, path: String): Option[Dataset[Crypto]] = {
        import ss.implicits._
        Try {
            ss.read.parquet(path).as[Crypto]
        }.mapException(e => new Exception("path is not a parquet", e)).toOption
    }
    
    def toPartitions(ss: SparkSession, parquetsDir: String, ds: Dataset[Crypto]): Seq[(CryptoPartitionKey, Dataset[Crypto])] = {

        def sameDatetime(b: Crypto, c: Crypto) = {
            c.cryptoValue.datetime == b.cryptoValue.datetime
        }

        def unionDataset(existingDataset: Option[Dataset[Crypto]], newDataset: Dataset[Crypto]): Dataset[Crypto] = {
            if (existingDataset.isEmpty) newDataset 
            else existingDataset.get
              .filter(b => newDataset.filter(c => sameDatetime(b, c)).count() == 0)
              .union(newDataset)
        }

        def filterDataset(ds1: Dataset[Crypto], key: CryptoPartitionKey): Dataset[Crypto] = {
            ds1.filter(_.partitionKey.equals(key))
        }
        val keys: Seq[CryptoPartitionKey] = getPartitionKeys(ss, ds)
        
        keys.map(key => {
            val existingPartitionDataset: Option[Dataset[Crypto]] = getPartitionFromPath(ss, key.getPartitionPath(parquetsDir))
            val filteredDataset = filterDataset(ds, key)
            val newDataset = unionDataset(existingPartitionDataset, filteredDataset)
            (key, newDataset)
        })
    }

    
}
