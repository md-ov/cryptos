package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.parquet.ParquetFromCSVObj.encoder
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.util.{Failure, Try}

object PartitionsIntegrator {
    def main(args: Array[String]): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
    
        val ds1: Dataset[Crypto] = getPartitionFromPath(ss, "file:///home/mdao/minh/git/cryptos/data/parquets/parquet").get
      
        val result = toPartitions(ss, "file:///home/mdao/minh/git/cryptos/data/parquets/", ds1)
        println("")
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
        }.mapException(e => new Exception("error", e)).toOption
    }
    
    def toPartitions(ss: SparkSession, parquetsDir: String, ds: Dataset[Crypto]): Seq[(CryptoPartitionKey, Dataset[Crypto])] = {

        def sameDatetime(b: Crypto, c: Crypto) = {
            c.cryptoValue.datetime == b.cryptoValue.datetime
        }

        def unionDataset(ds1: Option[Dataset[Crypto]], ds2: Dataset[Crypto]): Dataset[Crypto] = {
            if (ds1.isEmpty) ds2 
            else ds1.get
              .filter(b => ds2.filter(c => sameDatetime(b, c)).count() == 0)
              .union(ds2)
        }

        def filterDataset(ds1: Dataset[Crypto], key: CryptoPartitionKey): Dataset[Crypto] = {
            ds1.filter(_.partitionKey.equals(key))
        }
        
        getPartitionKeys(ss, ds)
          .map(key => {
              val existingDataset: Option[Dataset[Crypto]] = getPartitionFromPath(ss, key.getPartitionPath(parquetsDir))
              (key, unionDataset(existingDataset, filterDataset(ds, key)))
          })
    }

    
}
