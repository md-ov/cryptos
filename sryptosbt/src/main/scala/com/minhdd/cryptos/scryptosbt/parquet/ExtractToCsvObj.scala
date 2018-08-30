package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.ExtractToCsv
import com.minhdd.cryptos.scryptosbt.tools.Timestamps
import org.apache.spark.sql.{Dataset, SparkSession}

object ExtractToCsvObj {
    
    def extract(ss: SparkSession, d: String, args: ExtractToCsv) = {
        val year = d.substring(0,4)
        val month = d.substring(4,6)
        val day = d.substring(6,8)
        
        val key = CryptoPartitionKey(
            asset = args.asset,
            currency = args.currency,
            provider = "KRAKEN",
            api = "TRADES",
            year = year,
            month = month,
            day = day)
        
//        val tradesDs = ss.read.parquet(key.getPartitionPath(args.parquetsDir)).as[Crypto]
//        val tradesDs2: Dataset[(Timestamp, Crypto)] = tradesDs.map(c => (c.cryptoValue.datetime, c))
        
        ???        
    }
    
    def run(args: ExtractToCsv, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        import ss.implicits._
        
//        val ds: Dataset[Crypto] = ss.read.parquet(args.parquetPath).as[Crypto]
//        ds.toDF().show(false)
//        
//        val dsString: Dataset[String] = ds.map(_.flatten.toLine)
//        
//        Sparks.csvFromDSString(dsString, args.csvpath)
        val d = "2018-05-05"
        extract(ss, d, args)
        
        
        "status|SUCCESS"
    }
}
