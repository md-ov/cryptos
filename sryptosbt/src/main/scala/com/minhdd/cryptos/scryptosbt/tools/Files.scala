package com.minhdd.cryptos.scryptosbt.tools

import java.io.File

object Files {
    def getRecursiveDirs(directory: File): Seq[File] = {
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
}
