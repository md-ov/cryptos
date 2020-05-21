package com.minhdd.cryptos.scryptosbt.parquet

import java.io.File
import java.util.Date

import com.minhdd.cryptos.scryptosbt.{ToParquetsFromCsv, env}
import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.domain.Crypto.getPartitionFromPath
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, FileSystemService}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import com.minhdd.cryptos.scryptosbt.tools.FileHelper.firstLine

object ToParquetsFromCSV {
    
    def getListOfFiles(dir: String): List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList
        } else {
            Nil
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
        val ss: SparkSession = SparkSession.builder()
          .config("spark.driver.maxResultSize", "3g")
          .config("spark.network.timeout", "600s")
          .config("spark.executor.heartbeatInterval", "60s")
          .appName("toParquet").master(master).getOrCreate()

        ss.sparkContext.setLogLevel("ERROR")
        if (apiLowercased == "ohlc") {
            val fileList: Seq[String] = getListOfFiles(args.inputDir).map(_.getAbsolutePath)
            val dss: Seq[(CryptoPartitionKey, Dataset[String])] = 
                fileList.map(filePath => (getPartitionKey(firstLine(filePath).get, apiLowercased).get, ss.read.textFile
                (filePath)))
            val dsCryptos: Seq[(CryptoPartitionKey, Dataset[Crypto])] = dss.map(e => {
                val ds = e._2.flatMap(Crypto.parseOHLC)(Crypto.encoder(ss))
                (e._1, ds)
            })
            runOHLC(ss, dsCryptos, args.parquetsDir, args.minimum)
            "status|SUCCESS"
        } else if (apiLowercased == "trades") {
            def getNumber(fileName: String) = {
                fileName.split('.').head.toInt
            }
            val fileList = getListOfFiles(args.inputDir)
            val orderedFileList: Seq[String] = 
                fileList
                  .sortWith((file1, file2) => {
                      getNumber(file1.getName) < getNumber(file2.getName)
                  })
                .map(_.getAbsolutePath)
            
            val orderedDatasets: Seq[(CryptoPartitionKey, Dataset[String])] = 
                orderedFileList.filter(firstLine(_).nonEmpty).map(filePath => 
                (getPartitionKey(firstLine(filePath).get, apiLowercased).get, ss.read.textFile(filePath)))
    
            val dsCryptos: Seq[(CryptoPartitionKey, Dataset[Crypto])] =
                orderedDatasets.map(e => {
                    val ds = e._2.flatMap(Crypto.parseTrade)(Crypto.encoder(ss))
                    (e._1, ds)
                })
    
            runTrades(ss, dsCryptos, args.parquetsDir, args.minimum)
//            println("There must be at least : " +  (fileList.size - 1) * 1000  + " and at most : " + fileList.size * 1000)
            "status|SUCCESS"
        } else {
            "status|ERROR|api"
        }
    }
    
    def nextSmallerDate(orderedDates: Seq[String], date: String): String = {
        if (orderedDates.isEmpty) {
            date
        } else {
            val first: String = orderedDates.head
            val toCheckDate: Date = DateTimeHelper.toDate(date)
            if (date == first || toCheckDate.before(DateTimeHelper.toDate(first))) {
                first
            } else {
                orderedDates.takeWhile(DateTimeHelper.toDate(_).before(toCheckDate)).last
            }
        }
    }
    
    def getAllKeys(orderedDatasets: Seq[(CryptoPartitionKey, Dataset[Crypto])]): Seq[CryptoPartitionKey] = {
        val firstKey: CryptoPartitionKey = orderedDatasets.head._1
        val firstDate: String = firstKey.date()
        val lastDate: String = orderedDatasets.last._1.date()
    
        val dates: Seq[String] = DateTimeHelper.getDates(firstDate, lastDate)
        dates.map(date => firstKey.copy(
            year = DateTimeHelper.getYear(date),
            month = DateTimeHelper.getMonth(date),
            day = DateTimeHelper.getDay(date)))
    }
    
    def dateBetween(min: String, toCheck: String, max: String): Boolean = {
        toCheck == max || (
          DateTimeHelper.toDate(toCheck).before(DateTimeHelper.toDate(max)) &&
            DateTimeHelper.toDate(min).before(DateTimeHelper.toDate(toCheck))
          )
    }
    
    def filterDatasets(firstDates: Seq[String], dss: Seq[(String, Dataset[Crypto])], filterKey: CryptoPartitionKey)
    : Option[Dataset[Crypto]] = {
        val filterDate: String = filterKey.date()
        val seq: Seq[Dataset[Crypto]] = 
            dss.indices
              .filter(i => {
                  val currentFirstDate = dss.apply(i)._1
                  (currentFirstDate == filterDate) || 
                    (i < dss.size - 1 && dateBetween(currentFirstDate, filterDate, dss.apply(i + 1)._1))
              })
              .map(dss.apply(_)._2)
        
        if (seq.isEmpty) {
            None
        } else if (seq.size == 1) {
            Some(seq.head.filter(_.partitionKey.equals(filterKey)))
        } else {
            seq
              .map(ds => ds.filter(_.partitionKey.equals(filterKey)))
              .reduceOption((ds1, ds2) => ds1.union(ds2))
        }
    }
    
    private def runOHLC(spark: SparkSession, datasets: Seq[(CryptoPartitionKey, Dataset[Crypto])], parquetsDir: String,
                          minimumNumberOfElementForOnePartition: Long, fs: FileSystemService = FileSystemService("local")): Unit = {
        type AssetCurrency = (String, String)
        if (datasets.nonEmpty) {
            
            val datasetsWithAssetCurrency: Seq[(AssetCurrency, Dataset[Crypto])] = datasets.map(e => {
                val key = e._1
                val asset = key.asset
                val currency = key.currency
                ((asset, currency), e._2)
            })
            
            val groupedDatasets: Map[AssetCurrency, Seq[Dataset[Crypto]]] = 
                datasetsWithAssetCurrency.groupBy(_._1).mapValues(_.map(_._2))
    
            groupedDatasets.foreach(map => {
                val dss: Option[Dataset[Crypto]] = map._2.reduceOption(_.union(_))
                if (dss.isDefined) {
                    val dssGet: Dataset[Crypto] = dss.get
                    val key: CryptoPartitionKey = dssGet.head().partitionKey
                    val lastDs: Dataset[Crypto] = ParquetHelper.ohlcCryptoDs(spark)
                    val newDs: Dataset[Crypto] = lastDs.union(dssGet)
                    val parquetPath: String = key.getOHLCPath(parquetsDir, DateTimeHelper.now)
                    println("writing partition : " + parquetPath)
                    newDs.write.mode(SaveMode.Overwrite).parquet(parquetPath)
                }
            })
        } else {
            println("no data")
        }
    }
    
    private def runTrades(ss: SparkSession, datasets: Seq[(CryptoPartitionKey, Dataset[Crypto])], parquetsDir: String,
                    minimumNumberOfElementForOnePartition: Long): Unit = {
        if (datasets.nonEmpty) {
            
            val orderedDatasetsWithFirstDate: Seq[(String, Dataset[Crypto])] = datasets.map(e => (e._1.date(), e._2))
            
            val firstDates: Seq[String] = orderedDatasetsWithFirstDate.map(_._1)
            val allKeys: Seq[CryptoPartitionKey] = getAllKeys(datasets)
            
    
            val newKeys: Seq[CryptoPartitionKey] =
                allKeys.filter(key => getPartitionFromPath(ss, key.getPartitionPath(parquetsDir)).isEmpty)
            
            if (newKeys.isEmpty) println("There is no add to parquets")
    
            newKeys.foreach(key => {
                val partition: Option[Dataset[Crypto]] = filterDatasets(firstDates, orderedDatasetsWithFirstDate, key)
                if (partition.isDefined) {
                    println("writing partition : " + key)
                    partition.get.write.mode(SaveMode.Overwrite).parquet(key.getPartitionPath(parquetsDir))
                } else {
                    println("---- partition is empty : " + key)
                }
            })
            
        } else {
            println("no data")
        }
    }
}
