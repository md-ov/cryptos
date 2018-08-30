package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.ExtractToCsv
import com.minhdd.cryptos.scryptosbt.tools.{Datasets, DateTimes, Sparks}
import org.apache.spark.sql.{Dataset, SparkSession}

object ExtractToCsvObj {
    
    def run(args: ExtractToCsv, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
    
        val dates: Seq[String] = DateTimes.getDates(args.startDay, args.endDay)
        
        val cryptos: Seq[Crypto] = dates.flatMap(d => {
            val key1 = CryptoPartitionKey(
                asset = args.asset.toUpperCase,
                currency = args.currency.toUpperCase,
                provider = "KRAKEN",
                api = "TRADES",
                year = DateTimes.getYear(d),
                month = DateTimes.getMonth(d),
                day = DateTimes.getDay(d))
    
            val key2 = CryptoPartitionKey(
                asset = args.asset.toUpperCase,
                currency = args.currency.toUpperCase,
                provider = "KRAKEN",
                api = "OHLC",
                year = DateTimes.getYear(d),
                month = DateTimes.getMonth(d),
                day = DateTimes.getDay(d))
    
            val ds1: Option[Dataset[Crypto]] = Crypto.getPartition(ss, args.parquetsDir, key1)
            val ds2: Option[Dataset[Crypto]] = Crypto.getPartition(ss, args.parquetsDir, key2)
            val ds: Option[Dataset[Crypto]] = Datasets.union(ds1, ds2)
            ds.map(Extractor.oneDayCryptoValue(ss, d, _, Seq(key1, key2)))
        })
        
        Sparks.csvFromSeqCrypto(ss, args.csvpath, cryptos)
    
        "status|SUCCESS"
    }
}
