package com.minhdd.cryptos.scryptosbt.tools

import java.io.File
import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.domain.CryptoPartitionKey
import com.minhdd.cryptos.scryptosbt.env

import scala.io.{BufferedSource, Source}

object FileHelper {
    def getAllDirFromLastTimestamp(path: String, ts: Timestamp, cryptoPartitionKey: CryptoPartitionKey): Seq[String] = {
        val d = new File(path)
        getRecursiveDirsFromLastTimestamp(d, ts, cryptoPartitionKey, false, false).map(_.getAbsolutePath).filter(_
          .contains(cryptoPartitionKey.provider))
    }
    
    private def getRecursiveDirsFromLastTimestamp(directory: File, ts: Timestamp, 
                                                  cryptoPartitionKey: CryptoPartitionKey, itIsMonth: Boolean, 
                                                  sameParent: Boolean): Seq[File] = {
        if (directory.isFile) {
            Seq()
        } else {
            val directoryName = directory.getName
            val directoryNameInt = NumberHelper.fromStringToInt(directoryName)
            if (directoryName == "parquet") {
                Seq(directory)
            } else {
                if (directoryNameInt.isEmpty) {
                    val list = directory.listFiles()
                    list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, false, false)).reduce(_ ++ _)
                } else {
                    val directoryNameIntGet: Int = directoryNameInt.get
                    if (directoryNameIntGet > 1000) {
                        if (directoryNameIntGet == cryptoPartitionKey.year.toInt) {
                            val list = directory.listFiles()
                            list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, true, true))
                              .reduce(_ ++ _)
                        } else if (directoryNameIntGet > cryptoPartitionKey.year.toInt) {
                            val list = directory.listFiles()
                            list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, true, false))
                              .reduce(_ ++ _)
                        } else {
                            Seq()
                        }
                    } else {
                        if (itIsMonth && sameParent) {
                            if (directoryNameIntGet == cryptoPartitionKey.month.toInt) {
                                val list = directory.listFiles()
                                list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, false, true))
                                  .reduce(_ ++ _)
                            } else if (directoryNameIntGet > cryptoPartitionKey.month.toInt) {
                                val list = directory.listFiles()
                                list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, false, false)).reduce(_ ++ _)
                            } else {
                                Seq()
                            }
                        } else if (itIsMonth) {
                            val list = directory.listFiles()
                            list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, false, false)).reduce(_ ++ _)
                        } else {
                            if ((sameParent && directoryNameIntGet > cryptoPartitionKey.day.toInt) || !sameParent) {
                                val list = directory.listFiles()
                                list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, false, false)).reduce(_ ++ _)
                            } else {
                                Seq()
                            }
                        }
                    }
                }
            }
        }
    }
    
    private def getRecursiveDirs(directory: File): Seq[File] = {
        if (directory.isFile) {
            Seq()
        } else if (directory.getName == "parquet") {
            Seq(directory)
        } else {
            val list = directory.listFiles()
            list.map(getRecursiveDirs).reduce(_ ++ _)
        }
    }
    
    def getAllDir(path: String): Seq[String] = {
        val d = new File(path)
        getRecursiveDirs(d).map(_.getAbsolutePath)
    }
    
    def getPathForSpark(path: String): String = {
        if (env.env == "win") {
            if (path.contains("://")) "file:///" + path else "file:///" + getClass.getResource("/" + path).getPath
        } else {
            path
        }
    }
    
    def firstLine(filePath: String): Option[String] = {
        val file: BufferedSource = Source.fromFile(filePath)
        val line: String = file.bufferedReader().readLine()
        file.close()
        Option(line)
    }
}
