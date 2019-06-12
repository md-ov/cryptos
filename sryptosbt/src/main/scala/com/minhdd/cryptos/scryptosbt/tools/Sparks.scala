package com.minhdd.cryptos.scryptosbt.tools

import com.minhdd.cryptos.scryptosbt.exploration.BeforeSplit
import com.minhdd.cryptos.scryptosbt.parquet.CSVFromParquetObj.merge
import com.minhdd.cryptos.scryptosbt.parquet.Crypto
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Sparks {
    
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
