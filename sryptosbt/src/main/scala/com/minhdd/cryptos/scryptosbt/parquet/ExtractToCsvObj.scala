package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.ExtractToCsv
import com.minhdd.cryptos.scryptosbt.tools.{DateTimes, Sparks}
import org.apache.spark.sql.SparkSession

object ExtractToCsvObj {
    
    def run(args: ExtractToCsv, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
    
        val dates = DateTimes.getDates(args.startDay, args.endDay)
        
        val cryptos: Seq[Crypto] = dates.map(d => {
            val key = CryptoPartitionKey(
                asset = args.asset,
                currency = args.currency,
                provider = "KRAKEN",
                api = "TRADES",
                year = DateTimes.getYear(d),
                month = DateTimes.getMonth(d),
                day = DateTimes.getDay(d))
            
            val ds = Crypto.getPartition(ss, args.parquetsDir, key).get
            
            Extractor.getOneDayCryptoValue(ss, ds, key)
        })
        
        Sparks.csvFromSeqCrypto(ss, args.csvpath, cryptos)
    
        "status|SUCCESS"
    }
}
