package com.minhdd.cryptos.scryptosbt.tools

import java.io.File

case class FileSystemService(environment: String) {
  def getChildren(path: String): Seq[String] = {
    LocalFileSystem.getChildren(path)
  }
}

object LocalFileSystem {
  def getChildren(path: String): Seq[String] = {
    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.listFiles.map(_.getName)
    } else {
      Seq()
    }
  }
}
