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
    
            val fileList = getListOfFiles(args.inputDir)
            
            val orderedFileList: Seq[String] = 
                fileList
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
            println("There must be at least : " +  (fileList.size - 1) * 1000  + " and at most : " + fileList.size * 1000)
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
    
    private def run(ss: SparkSession, orderedDatasets: Seq[(CryptoPartitionKey, Dataset[Crypto])], parquetsDir: String,
                    minimumNumberOfElementForOnePartition: Long): Unit = {
        if (orderedDatasets.nonEmpty) {
            
            val orderedDatasetsWithFirstDate: Seq[(String, Dataset[Crypto])] = orderedDatasets.map(e => (e._1.date(), e._2))
            
            val firstDates: Seq[String] = orderedDatasetsWithFirstDate.map(_._1)
    
            val allKeys: Seq[CryptoPartitionKey] = getAllKeys(orderedDatasets)
    
            val newKeys: Seq[CryptoPartitionKey] =
                allKeys.filter(key => getPartitionFromPath(ss, key.getPartitionPath(parquetsDir)).isEmpty)
  
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
