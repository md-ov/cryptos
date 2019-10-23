package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object PartitionsIntegrator {

    def reduceSeq(seq1: Seq[CryptoPartitionKey], seq2: Seq[CryptoPartitionKey]): Seq[CryptoPartitionKey] = {
        seq2.filter(c => !seq1.exists(c.equals(_))) ++ seq1
    }

    def getPartitionKeys(ss: SparkSession, ds: Dataset[Crypto]): Seq[CryptoPartitionKey] = {
        
        def encoder(ss: SparkSession): Encoder[Seq[CryptoPartitionKey]] = {
            import ss.implicits._
            implicitly[Encoder[Seq[CryptoPartitionKey]]]
        }
        
        ds.map(c => Seq(c.partitionKey))(encoder(ss)).reduce((seq1, seq2) => reduceSeq(seq1, seq2))
    }
    
    def filterDataset(ds: Dataset[Crypto], key: CryptoPartitionKey): Dataset[Crypto] = {
        ds.filter(_.partitionKey.equals(key))
    }

    def toPartitions(ss: SparkSession, parquetsDir: String, ds: Dataset[Crypto]): Unit = {
        def sameDatetime(b: Crypto, c: Crypto) = {
            c.cryptoValue.datetime == b.cryptoValue.datetime
        }

        def unionDataset(existingDataset: Option[Dataset[Crypto]], newDataset: Dataset[Crypto]): Dataset[Crypto] = {
            if (existingDataset.isEmpty) newDataset
            else existingDataset.get
              .filter(b => newDataset.filter(c => sameDatetime(b, c)).count() == 0)
              .union(newDataset)
        }
        getPartitionKeys(ss, ds).map(key => {
            val existingPartitionDataset: Option[Dataset[Crypto]] = getPartitionFromPath(ss, key.getPartitionPath(parquetsDir))
            val filteredDataset = filterDataset(ds, key)
            val newDataset = unionDataset(existingPartitionDataset, filteredDataset)
            (key, newDataset)
        })
    }
    
    def main(args: Array[String]): Unit = {
        val ss: SparkSession =
            SparkSession.builder()
              .appName("integration parquets")
              .master("local[*]").getOrCreate()
        
        ss.sparkContext.setLogLevel("WARN")
        val ds1: Dataset[Crypto] =
            getPartitionFromPath(ss, "file:///home/mdao/minh/git/cryptos/data/parquets/parquet").get
        toPartitions(ss, "file:///home/mdao/minh/git/cryptos/data/parquets/", ds1)
    }

    
}
