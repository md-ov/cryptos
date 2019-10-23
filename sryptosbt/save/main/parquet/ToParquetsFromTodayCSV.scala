package com.minhdd.cryptos.scryptosbt.parquet

import java.io.File
import java.util.Date

import com.minhdd.cryptos.scryptosbt.tools.Files.firstLine
import com.minhdd.cryptos.scryptosbt.ToParquetsFromTodayCsv
import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.domain.Crypto.getPartitionFromPath
import com.minhdd.cryptos.scryptosbt.tools.DateTimes
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.io.Source

object ToParquetsFromTodayCSV {
    
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
    
    def run(args: ToParquetsFromTodayCsv, master: String): String = {
        val apiLowercased = args.api.toLowerCase
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        if (apiLowercased == "ohlc") {

            "status|NOT_PROCESSED"
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
                orderedFileList.filter(firstLine(_).isDefined).map(filePath => 
                (getPartitionKey(firstLine(filePath).get, apiLowercased).get, ss.read.textFile(filePath)))
    
            val dsCryptos: Seq[(CryptoPartitionKey, Dataset[Crypto])] =
                orderedDatasets.map(e => {
                    val ds = e._2.flatMap(Crypto.parseTrade)(Crypto.encoder(ss))
                    (e._1, ds)
                })
    
            runTodayTrades(ss, dsCryptos, args.parquetsDir, args.minimum)
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
            val toCheckDate: Date = DateTimes.toDate(date)
            if (date == first || toCheckDate.before(DateTimes.toDate(first))) {
                first
            } else {
                orderedDates.takeWhile(DateTimes.toDate(_).before(toCheckDate)).last
            }
        }
    }
    
    def getAllKeys(orderedDatasets: Seq[(CryptoPartitionKey, Dataset[Crypto])]): Seq[CryptoPartitionKey] = {
        val firstKey: CryptoPartitionKey = orderedDatasets.head._1
        val firstDate: String = firstKey.date()
        val lastDate: String = orderedDatasets.last._1.date()
    
        val dates: Seq[String] = DateTimes.getDates(firstDate, lastDate)
        dates.map(date => firstKey.copy(
            year = DateTimes.getYear(date),
            month = DateTimes.getMonth(date),
            day = DateTimes.getDay(date)))
    }
    
    def dateBetween(min: String, toCheck: String, max: String): Boolean = {
        toCheck == max || (
          DateTimes.toDate(toCheck).before(DateTimes.toDate(max)) &&
            DateTimes.toDate(min).before(DateTimes.toDate(toCheck))
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
    
    private def runOHLC(ss: SparkSession, datasets: Seq[(CryptoPartitionKey, Dataset[Crypto])], parquetsDir: String,
                          minimumNumberOfElementForOnePartition: Long): Unit = {
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
                    val dssGet = dss.get
                    val key = dssGet.head().partitionKey
                    val parquetPath = key.getOHLCPath(parquetsDir)
                    println("writing partition : " + parquetPath)
                    dssGet.write.mode(SaveMode.Overwrite).parquet(parquetPath)
                }
            })
        } else {
            println("no data")
        }
    }
    
    private def runTodayTrades(ss: SparkSession, datasets: Seq[(CryptoPartitionKey, Dataset[Crypto])], 
                             parquetsDir: String,
                          minimumNumberOfElementForOnePartition: Long): Unit = {
        if (datasets.nonEmpty) {
            
            val orderedDatasetsWithFirstDate: Seq[(String, Dataset[Crypto])] = datasets.map(e => (e._1.date(), e._2))
            
            val firstDates: Seq[String] = orderedDatasetsWithFirstDate.map(_._1)
            val allKeys: Seq[CryptoPartitionKey] = getAllKeys(datasets)
            
            
            val newKeys: Seq[CryptoPartitionKey] =
                allKeys.filter(key => getPartitionFromPath(ss, key.getPartitionPath(parquetsDir)).isEmpty)
            
            newKeys.foreach(key => {
                val partition: Option[Dataset[Crypto]] = filterDatasets(firstDates, orderedDatasetsWithFirstDate, key)
                if (partition.isDefined) {
                    val parquetPath = key.getTodayPartitionPath(parquetsDir)
                    println("writing partition : " + parquetPath)
                    partition.get.write.mode(SaveMode.Overwrite).parquet(parquetPath)
                } else {
                    println("---- partition is empty : " + key)
                }
            })
            
        } else {
            println("no data")
        }
    }
}
