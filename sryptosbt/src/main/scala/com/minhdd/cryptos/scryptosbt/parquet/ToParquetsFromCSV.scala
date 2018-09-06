package com.minhdd.cryptos.scryptosbt.parquet

import java.io.File

import com.minhdd.cryptos.scryptosbt.ToParquetsFromCsv
import com.minhdd.cryptos.scryptosbt.parquet.Crypto.getPartitionFromPath
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object ToParquetsFromCSV {
    
    def getListOfFiles(dir: String): List[String] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList.map(file => file.getAbsolutePath)
        } else {
            List[String]()
        }
    }
    
    def getPartitionKey(line: String, api: String): Option[CryptoPartitionKey] = {
        if (api == "ohlc") {
            Option(Crypto.parseOHLC(line).head.partitionKey)
        } else if(api == "trades") {
            Option(Crypto.parseTrade(line).head.partitionKey)
        } else None
    }
    
    def run(args: ToParquetsFromCsv, master: String): String = {
        val apiLowercased = args.api.toLowerCase
        if (apiLowercased == "ohlc" || apiLowercased == "trades") {
            val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
            ss.sparkContext.setLogLevel("WARN")
    
            val fileList = getListOfFiles(args.inputDir)
    
            val datasets: Seq[(String, Dataset[String])] = fileList.map(filePath => (filePath, ss.read.textFile(filePath)))
            val orderedDatasets: Seq[(String, Dataset[String])] = datasets.sortWith((e1, e2) => e1._1 < e2._1)
            val firstStringOfFirstFile: String = orderedDatasets.head._2.first
            val firstStringOfLastFile: String = orderedDatasets.last._2.first
            val firstKey: CryptoPartitionKey = getPartitionKey(firstStringOfFirstFile, apiLowercased).get
            val firstDate: String = firstKey.date()
            val lastDate: String = getPartitionKey(firstStringOfLastFile, apiLowercased).get.date()
            
            import com.minhdd.cryptos.scryptosbt.tools.DateTimes
            val dates: Seq[String] = DateTimes.getDates(firstDate, lastDate)
            val allKeys: Seq[CryptoPartitionKey] =
                dates.map(date => firstKey.copy(
                    year = DateTimes.getYear(date),
                    month = DateTimes.getMonth(date),
                    day = DateTimes.getDay(date)))
    
            val dsStrings: Seq[Dataset[String]] = orderedDatasets.map(_._2)
    
            val dsCryptos: Seq[Dataset[Crypto]] = if (apiLowercased == "ohlc") {
                dsStrings.map(ds => ds.flatMap(Crypto.parseOHLC)(Crypto.encoder(ss)))
            } else if (apiLowercased == "trades") {
                dsStrings.map(ds => ds.flatMap(Crypto.parseTrade)(Crypto.encoder(ss)))
            } else Nil
    
            run(ss, allKeys, dsCryptos, args.parquetsDir, args.minimum)
    
            "status|SUCCESS"
        } else {
            "status|ERROR|api"
        }
    }
    
    def filterDatasets(dss: Seq[Dataset[Crypto]], key: CryptoPartitionKey): Dataset[Crypto] = {
        dss.map(ds => ds.filter(_.partitionKey.equals(key))).reduce(_.union(_))
    }
    
    private def run(ss: SparkSession, keys: Seq[CryptoPartitionKey], dss: Seq[Dataset[Crypto]], parquetsDir: String,
            minimumNumberOfElementForOnePartition: Long): Unit = {
        if (dss.nonEmpty) {
            val newKeys: Seq[CryptoPartitionKey] = keys.filter(key => getPartitionFromPath(ss, key.getPartitionPath(parquetsDir)).isEmpty)
            
            val newPartitions: Seq[(CryptoPartitionKey, Dataset[Crypto])] =
                newKeys.map(key => (key, filterDatasets(dss, key)))
            
            newPartitions
              .filter(_._2.count() >= minimumNumberOfElementForOnePartition)
              .foreach(partition => {
                  println("writing partition : " + partition._1)
                  partition._2.write.mode(SaveMode.Overwrite).parquet(partition._1.getPartitionPath(parquetsDir))
              })
        }
    }
}
