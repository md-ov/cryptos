package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.{CommandAppArgs, ParquetFromCsv}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object ParquetFromCSVObj {
    def encoder(ss: SparkSession): Encoder[Crypto] = {
        import ss.implicits._
        implicitly[Encoder[Crypto]]
    }
    
    def parseCrypto(line: String): Seq[Crypto] = {
        Crypto.parseLine(line)
    }
    
    def parquet(ss: SparkSession, csvPath: String): Dataset[Crypto] = {
        ss.read.textFile(csvPath).flatMap(parseCrypto)(encoder(ss))
    }
    
    def run(args: ParquetFromCsv, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        val ds: Dataset[Crypto] = parquet(ss, args.csvpath)
        ds.write.parquet(args.parquetPath)
        "status|SUCCESS"
    }
}
