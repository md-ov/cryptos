package com.minhdd.cryptos.scryptosbt.tools

import java.io.File
import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.parquet.CryptoPartitionKey

object Files {
    def getAllDirFromLastTimestamp(path: String, ts: Timestamp, cryptoPartitionKey: CryptoPartitionKey): Seq[String] = {
        val d = new File(path)
        getRecursiveDirsFromLastTimestamp(d, ts, cryptoPartitionKey, false).map(_.getAbsolutePath)
    }
    
    private def getRecursiveDirsFromLastTimestamp(directory: File, ts: Timestamp, 
                                                  cryptoPartitionKey: CryptoPartitionKey, itIsMonth: Boolean): Seq[File] = {
        if (directory.isFile) {
            Seq()
        } else {
            val directoryName = directory.getName
            val directoryNameInt = Numbers.fromStringToInt(directoryName)
            if (directoryName == "parquet") {
                Seq(directory)
            } else {
                if (directoryNameInt.isEmpty) {
                    val list = directory.listFiles()
                    list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, false)).reduce(_ ++ _)
                } else {
                    val directoryNameIntGet: Int = directoryNameInt.get
                    if (directoryNameIntGet > 1000) {
                        if (directoryNameIntGet >= cryptoPartitionKey.year.toInt) {
                            val list = directory.listFiles()
                            list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, true)).reduce(_ ++ _)
                        } else {
                            Seq()
                        }
                    } else {
                        if (itIsMonth) {
                            if (directoryNameIntGet >= cryptoPartitionKey.month.toInt) {
                                val list = directory.listFiles()
                                list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, false)).reduce(_ ++ _)
                            } else {
                                Seq()
                            }
                        } else {
                            if (directoryNameIntGet >= cryptoPartitionKey.day.toInt) {
                                val list = directory.listFiles()
                                list.map(e => getRecursiveDirsFromLastTimestamp(e, ts, cryptoPartitionKey, false)).reduce(_ ++ _)
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
    
    def getPathForSpark(relativePath: String): String = {
        "file://" + getClass.getResource(relativePath).getPath
    }
}
