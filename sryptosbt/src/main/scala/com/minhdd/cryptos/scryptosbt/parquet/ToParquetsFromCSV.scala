package com.minhdd.cryptos.scryptosbt.parquet

import com.minhdd.cryptos.scryptosbt.ToParquetsFromCsv
import com.minhdd.cryptos.scryptosbt.parquet.ParquetFromCSVObj.parquet
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object ToParquetsFromCSV {

    def run(args: ToParquetsFromCsv, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        
        val ds: Dataset[Crypto] = parquet(ss, args.csvpath)
        
        PartitionsIntegrator.run(ss, ds, args.parquetsDir)
        
        "status|SUCCESS"
    }
}
