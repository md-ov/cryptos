package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.ParquetFromCsv
import com.minhdd.cryptos.scryptosbt.domain.Crypto
import org.apache.spark.sql.SparkSession

object ParquetFromCSVObj {

    def run(args: ParquetFromCsv, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        if (args.api.toLowerCase == "ohlc") {
            ss.read.textFile(args.csvpath)
              .flatMap(Crypto.parseOHLC)(Crypto.encoder(ss))
              .write.parquet(args.parquetPath)
        } else if (args.api.toLowerCase == "trades") {
            ss.read.textFile(args.csvpath)
              .flatMap(Crypto.parseTrade)(Crypto.encoder(ss))
              .write.parquet(args.parquetPath)
        }
        "status|SUCCESS"
    }
}
