package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.{ParquetFromCsv}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object ParquetFromCSVObj {
    def encoder(ss: SparkSession): Encoder[Crypto] = {
        import ss.implicits._
        implicitly[Encoder[Crypto]]
    }
    
    def run(args: ParquetFromCsv, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        println(args.csvpath)
        if (args.api.toLowerCase == "ohlc") {
            ss.read.textFile("file:///" + args.csvpath)
              .flatMap(Crypto.parseOHLC)(encoder(ss))
              .write.parquet(args.parquetPath)
        } else if (args.api.toLowerCase == "trades") {
            ss.read.textFile(args.csvpath)
              .flatMap(Crypto.parseTrade)(encoder(ss))
              .write.parquet(args.parquetPath)
        }
        "status|SUCCESS"
    }
}
