package com.minhdd.cryptos.scryptosbt.parquet

import java.io.File

import com.minhdd.cryptos.scryptosbt.ToParquetsFromCsv
import com.minhdd.cryptos.scryptosbt.parquet.ParquetFromCSVObj.encoder
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object ToParquetsFromCSV {
    
    def getListOfFiles(dir: String): List[String] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList.map(file => file.getAbsolutePath)
        } else {
            List[String]()
        }
    }

    def run(args: ToParquetsFromCsv, master: String): String = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        
        val fileList = getListOfFiles(args.inputDir)
        
        val dsString: Dataset[String] =
            fileList
              .map(filePath => ss.read.textFile(filePath))
              .reduce((ds1, ds2) => ds1.union(ds2))
        
    
        val ds: Option[Dataset[Crypto]] = if (args.api.toLowerCase == "ohlc") {
            Some(dsString.flatMap(Crypto.parseOHLC)(encoder(ss)))
        } else if (args.api.toLowerCase == "trades") {
            Some(dsString.flatMap(Crypto.parseTrade)(encoder(ss)))
        } else None
        
        
        PartitionsIntegrator.run(ss, ds, args.parquetsDir, args.minimum)
        
        "status|SUCCESS"
    }
}
