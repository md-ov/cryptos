package com.minhdd.cryptos.scryptosbt.parquet

import java.io.File
import java.util.Date

import com.minhdd.cryptos.scryptosbt.ToParquetsFromCsv
import com.minhdd.cryptos.scryptosbt.parquet.Crypto.getPartitionFromPath
import com.minhdd.cryptos.scryptosbt.tools.DateTimes
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.io.Source

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
    
    def firstLine(filePath: String): String = {
        val file = Source.fromFile(filePath)
        val line = file.bufferedReader().readLine()
        file.close()
        line
    }
    
    def run(args: ToParquetsFromCsv, master: String): String = {
        val apiLowercased = args.api.toLowerCase
        if (apiLowercased == "ohlc" || apiLowercased == "trades") {
            val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
            ss.sparkContext.setLogLevel("WARN")
    
            def getNumber(fileName: String) = {
                fileName.split('.').head.toInt
            }
            
            val orderedFileList: Seq[String] = 
                getListOfFiles(args.inputDir)
                  .sortWith((file1, file2) => {
                      getNumber(file1.getName) < getNumber(file2.getName)
                  })
                .map(_.getAbsolutePath)
            
            val orderedDatasets: Seq[(CryptoPartitionKey, Dataset[String])] = orderedFileList.map(filePath => 
                (getPartitionKey(firstLine(filePath), apiLowercased).get, ss.read.textFile(filePath)))
    
            val dsCryptos: Seq[(CryptoPartitionKey, Dataset[Crypto])] = if (apiLowercased == "ohlc") {
                orderedDatasets.map(e => {
                    val ds = e._2.flatMap(Crypto.parseOHLC)(Crypto.encoder(ss))
                    (e._1, ds)
                })
            } else if (apiLowercased == "trades") {
                orderedDatasets.map(e => {
                    val ds = e._2.flatMap(Crypto.parseTrade)(Crypto.encoder(ss))
                    (e._1, ds)
                })
            } else Nil
    
            run(ss, dsCryptos, args.parquetsDir, args.minimum)
    
            "status|SUCCESS"
        } else {
            "status|ERROR|api"
        }
    }
    
    def nextSmallerDate(orderedDates: Seq[String], date: String): String = {
        val toCheckDate: Date = DateTimes.toDate(date)
        orderedDates.takeWhile(d => d == date || DateTimes.toDate(d).before(toCheckDate)).last
    }
    
    def isDateOk(date: String, max: String, min: String): Boolean = {
        val toCheckDate: Date = DateTimes.toDate(date)
        date == max || date == min || (toCheckDate.after(DateTimes.toDate(min)) && toCheckDate.before(DateTimes.toDate(max)))
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
    
    def filterDatasets(firstDates: Seq[String], dss: Seq[(String, Dataset[Crypto])], key: CryptoPartitionKey): Dataset[Crypto] = {
        dss
          .filter(e => isDateOk(e._1, key.date(), nextSmallerDate(firstDates, key.date())))
          .map(_._2)
          .map(ds => ds.filter(_.partitionKey.equals(key))).reduce(_.union(_))
    }
    
    private def run(ss: SparkSession, orderedDatasets: Seq[(CryptoPartitionKey, Dataset[Crypto])], parquetsDir: String,
                    minimumNumberOfElementForOnePartition: Long): Unit = {
        if (orderedDatasets.nonEmpty) {
            
            val orderedDatasetsWithFirstDate: Seq[(String, Dataset[Crypto])] = orderedDatasets.map(e => (e._1.date(), e._2))
            
            val firstDates: Seq[String] = orderedDatasetsWithFirstDate.map(_._1)
            println(firstDates)
    
            val allKeys: Seq[CryptoPartitionKey] = getAllKeys(orderedDatasets)
    
            val newKeys: Seq[CryptoPartitionKey] =
                allKeys.filter(key => getPartitionFromPath(ss, key.getPartitionPath(parquetsDir)).isEmpty)
    
            val newPartitions: Seq[(CryptoPartitionKey, Dataset[Crypto])] =
                newKeys.map(key => (key, filterDatasets(firstDates, orderedDatasetsWithFirstDate, key)))
            
            newPartitions
//              .filter(_._2.count() >= minimumNumberOfElementForOnePartition)
              .foreach(partition => {
                  println("writing partition : " + partition._1)
                  partition._2.write.mode(SaveMode.Overwrite).parquet(partition._1.getPartitionPath(parquetsDir))
              })
        }
    }
}
