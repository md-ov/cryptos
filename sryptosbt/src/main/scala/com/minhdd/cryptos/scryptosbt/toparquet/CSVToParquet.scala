package com.minhdd.cryptos.scryptosbt.toparquet

import com.minhdd.cryptos.scryptosbt.ParquetFromCsv
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object CSVToParquet {
    def encoder(ss: SparkSession): Encoder[CryptoValue] = {
        import ss.implicits._
        implicitly[Encoder[CryptoValue]]
    }
    
    def toCryptoValue(line: String): Seq[CryptoValue] = {
        CryptoValue.parseLine(line)
    }
    
    def parquetFromCsv(args: ParquetFromCsv, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        val ds: Dataset[CryptoValue] = ss.read.textFile(args.csvpath).flatMap(toCryptoValue)(encoder(ss))
        ds.write.parquet(args.parquetPath)
        "status|SUCCESS"
    }
}
