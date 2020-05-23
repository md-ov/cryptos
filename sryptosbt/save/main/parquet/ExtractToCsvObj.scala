package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.ExtractToCsv
import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.tools.{DatasetHelper, DateTimeHelper, SparkHelper}
import org.apache.spark.sql.{Dataset, SparkSession}

object ExtractToCsvObj {
    
    def run(args: ExtractToCsv, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
    
        val dates: Seq[String] = DateTimeHelper.getDates(args.startDay, args.endDay)
        
        val cryptos: Seq[Crypto] = dates.flatMap(d => {
            val key1 = CryptoPartitionKey(
                asset = args.asset.toUpperCase,
                currency = args.currency.toUpperCase,
                provider = "KRAKEN",
                api = "TRADES",
                year = DateTimeHelper.getYear(d),
                month = DateTimeHelper.getMonth(d),
                day = DateTimeHelper.getDay(d))
    
            val key2 = CryptoPartitionKey(
                asset = args.asset.toUpperCase,
                currency = args.currency.toUpperCase,
                provider = "KRAKEN",
                api = "OHLC",
                year = DateTimeHelper.getYear(d),
                month = DateTimeHelper.getMonth(d),
                day = DateTimeHelper.getDay(d))
    
            val ds1: Option[Dataset[Crypto]] = Crypto.getPartition(ss, args.parquetsDir, key1)
            val ds2: Option[Dataset[Crypto]] = Crypto.getPartition(ss, args.parquetsDir, key2)
            val ds: Option[Dataset[Crypto]] = union(ds1, ds2)
            ds.map(Extractor.oneDayCryptoValue(ss, d, _, Seq(key1, key2)))
        })
        
        SparkHelper.csvFromSeqCrypto(ss, args.csvpath, cryptos)
    
        "status|SUCCESS"
    }

    private def union(ds1: Option[Dataset[Crypto]], ds2: Option[Dataset[Crypto]]): Option[Dataset[Crypto]] = {
        if (ds1.isEmpty && ds2.isEmpty) None
        else if (ds1.isEmpty) ds2
        else if (ds2.isEmpty) ds1
        else Some(ds1.get.union(ds2.get))
    }
}
