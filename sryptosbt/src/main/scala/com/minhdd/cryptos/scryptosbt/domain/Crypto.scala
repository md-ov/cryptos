package com.minhdd.cryptos.scryptosbt.domain

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.env
import com.minhdd.cryptos.scryptosbt.tools.TimestampHelper.TimestampImplicit
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, FileSystemService, TimestampHelper}
import org.apache.spark.sql.{Dataset, SparkSession}
import com.minhdd.cryptos.scryptosbt.tools.FileHelper.getSeparator
import com.minhdd.cryptos.scryptosbt.tools.Implicits.TryImplicit
import com.minhdd.cryptos.scryptosbt.tools.FileHelper

import scala.util.Try

case class CryptoPartitionKey(asset: String,
                              currency: String,
                              provider: String,
                              api: String,
                              year: String,
                              month: String,
                              day: String) {

    val PARQUET: String = "parquet"

    def date: String = DateTimeHelper.getDate(year, month, day)

    def getTodayPartitionPath(parquetsDir: String): String = {
        val separator: String = getSeparator(parquetsDir)
        val fullParquetDir: String = if (parquetsDir.endsWith(separator)) parquetsDir else parquetsDir + separator
        val path: String = fullParquetDir +
            asset.toUpperCase + separator +
            currency.toUpperCase + separator +
            api.toUpperCase + separator +
            "today" + separator + PARQUET
        path
    }

    def getPartitionPath(parquetsDir: String): String = {
        val separator: String = getSeparator(parquetsDir)
        val fullParquetDir: String = if (parquetsDir.endsWith(separator)) parquetsDir else parquetsDir + separator
        val path: String = fullParquetDir +
            asset.toUpperCase + separator +
            currency.toUpperCase + separator +
            api.toUpperCase + separator +
            year + separator + month + separator + day + separator +
            provider.toUpperCase + separator +
            api.toUpperCase + separator +
            PARQUET
        path
    }
    
    def getOHLCPath(parquetsDir: String): String = {
        CryptoPartitionKey.getOHLCParquetPath(parquetsDir, asset, currency)
    }

    def getOHLCPath(parquetsDir: String, ts: String): String = {
        CryptoPartitionKey.getOHLCParquetPath(parquetsDir, ts, asset, currency)
    }
}

object CryptoPartitionKey {

    def getOHLCParquetPath(parquetsDir: String, ts: String, asset: String, currency: String): String = {
        val separator: String = getSeparator(parquetsDir)
        val fullParquetDir = if (parquetsDir.endsWith(separator)) parquetsDir else parquetsDir + separator
        val path = fullParquetDir +
          asset.toUpperCase + separator +
          currency.toUpperCase + separator +
          "OHLC" + separator +
          "parquet" + separator +
          ts
        path
    }
    
    def getOHLCParquetPath(parquetsDir: String, asset: String, currency: String): String = {
        val separator: String = getSeparator(parquetsDir)
        val fullParquetDir = if (parquetsDir.endsWith(separator)) parquetsDir else parquetsDir + separator
        val path = fullParquetDir +
            asset.toUpperCase + separator +
            currency.toUpperCase + separator +
            "OHLC" + separator +
            "parquet"
        path
    }
    
    def getTRADESParquetPath(parquetsDir: String, asset: String, currency: String): String = {
        val separator: String = getSeparator(parquetsDir)
        val fullParquetDir = if (parquetsDir.endsWith(separator)) parquetsDir else parquetsDir + separator
        val path = fullParquetDir +
            asset.toUpperCase + separator +
            currency.toUpperCase + separator +
            "TRADES"
        path
    }
    
    def getTRADESParquetPath(parquetsDir: String, asset: String, currency: String, year: String): String = {
        val separator: String = getSeparator(parquetsDir)
        val fullParquetDir = if (parquetsDir.endsWith(separator)) parquetsDir else parquetsDir + separator
        val path: String = fullParquetDir +
            asset.toUpperCase + separator +
            currency.toUpperCase + separator +
            "TRADES" + separator +
            year
        path
    }
}

case class CryptoValue(datetime: Timestamp,
                        value: Double,
                        margin: Option[Margin],
                        volume: Double)

case class Margin(superior: Double,
                  inferior: Double)

case class CryptoPrediction(prediction: Double,
                            accuracy: Option[Double],
                            predictionDt: Timestamp)

object Crypto {
    def parseOHLC(line: String): Seq[Crypto] = {
        val splits: Array[String] = line.split(";")
        val asset: String = splits.apply(0)
        val currency: String = splits.apply(1)
        val provider: String = splits.apply(2)
        val timestampPosition = 5
        val value: String = splits.apply(timestampPosition + 1)
        val volume: String = splits.apply(timestampPosition + 6)
        val count: String = splits.apply(timestampPosition + 7)
        val ts: TimestampHelper = TimestampHelper(splits.apply(timestampPosition).toLong * 1000)
        val partitionKey = CryptoPartitionKey(
            asset = asset.toUpperCase,
            currency = currency.toUpperCase,
            provider = provider.toUpperCase,
            api = "OHLC",
            year = ts.getYear, month = ts.getMonth, day = ts.getDay
        )
        val processingDt: Timestamp = TimestampHelper.now
        val cryptoValue = CryptoValue(
            datetime = ts.timestamp,
            value = value.toDouble,
            volume = volume.toDouble,
            margin = None
        )
        Seq(Crypto(
            partitionKey = partitionKey,
            cryptoValue = cryptoValue,
            tradeMode = None,
            count = Some(count.toInt),
            processingDt = processingDt,
            prediction = None
        ))
    }
    
    def parseTrade(line: String): Seq[Crypto] = {
        val splits: Array[String] = line.split(";")
        val asset: String = splits.apply(0)
        val currency: String = splits.apply(1)
        val provider: String = splits.apply(2)
        val value: String = splits.apply(5)
        val volume: String = splits.apply(6)
        val ts: TimestampHelper = TimestampHelper((splits.apply(7).toDouble * 1000).toLong)
        val tradeMode: String = splits.apply(8)
        val partitionKey = CryptoPartitionKey(
            asset = asset.toUpperCase,
            currency = currency.toUpperCase,
            provider = provider.toUpperCase,
            api = "TRADES",
            year = ts.getYear, month = ts.getMonth, day = ts.getDay
        )
        val processingDt = TimestampHelper.now
        val cryptoValue = CryptoValue(
            datetime = ts.timestamp,
            value = value.toDouble,
            volume = volume.toDouble,
            margin = None
        )
        Seq(Crypto(
            partitionKey = partitionKey,
            cryptoValue = cryptoValue,
            tradeMode = Some(tradeMode),
            count = None,
            processingDt = processingDt,
            prediction = None
        ))
    }

    def getLastParquet(spark: SparkSession, path: String, fs: FileSystemService = FileSystemService("local")): Option[Dataset[Crypto]] = {
        Try {
            import spark.implicits._
            val lastFolder: String = fs.getChildren(path).sortWith { case (x, y) => x.toLong > y.toLong }.head
            spark.read.parquet(path + env.pathDelimiter + lastFolder).as[Crypto]
        }.mapException(e => {
            e.printStackTrace()
            new Exception("there is some problem")
        }).toOption
    }

    def getPartitionFromPath(spark: SparkSession, path: String): Option[Dataset[Crypto]] = {
        Try {
            import spark.implicits._
            spark.read.parquet(path).as[Crypto]
        }.mapException(e => {
            println("getPartitionFromPath")
            println(e.getMessage)
            new Exception("$path is not a parquet", e)
        }).toOption
    }
    
    def getPartitionsUniFromPath(spark: SparkSession, prefix: String, path: String): Option[Dataset[Crypto]] = {
        import com.minhdd.cryptos.scryptosbt.tools.FileHelper
        Try {
            import spark.implicits._
            val allPartitionsPath: Seq[String] = FileHelper.getAllDir(path)
            val allPaths = allPartitionsPath.map(prefix + _)
            allPaths.map(spark.read.parquet(_).as[Crypto]).reduce(_.union(_))
        }.mapException(e => {
            println("getPartitionsUniFromPath")
            println(e.getMessage)
            new Exception("path is not a parquet", e)
        }).toOption
    }

    def getPartitionsUniFromPathBetweenBeginTimestampAndNow(spark: SparkSession, prefix: String, path1: String,
                                                             path2: String,
                                                             beginTs: Timestamp, beginCryptoPartitionKey: CryptoPartitionKey,
                                                             endTs: Timestamp, endCryptoPartitionKey: CryptoPartitionKey): Option[Dataset[Crypto]] = {
        Try {
            import spark.implicits._

            val partitionPathOfBeginTimestampDay: String = beginCryptoPartitionKey.getPartitionPath(path2)
            val allPaths: Seq[String] = FileHelper.getAllDirFromTimestamp(path1, beginTs, beginCryptoPartitionKey)

            val toExcludePaths: Seq[String] = FileHelper.getAllDirFromTimestamp(path1, endTs, endCryptoPartitionKey)

            val filteredPaths: Seq[String] = allPaths.filterNot(toExcludePaths.contains) ++ Seq(partitionPathOfBeginTimestampDay, env.todayPath)

            filteredPaths.map(spark.read.parquet(_).as[Crypto]).reduce(_.union(_))

        }.mapException(e => {
            println("getPartitionsUniFromPathBetweenTwoTimestamps")
            println(e.getMessage)
            new Exception("There is something wrong", e)
        }).toOption
    }

    def getPartitionsUniFromPathBetweenTwoTimestamps(spark: SparkSession, prefix: String, path1: String,
                                                     path2: String,
                                                     beginTs: Timestamp, beginCryptoPartitionKey: CryptoPartitionKey,
                                                     endTs: Timestamp, endCryptoPartitionKey: CryptoPartitionKey): Option[Dataset[Crypto]] = {
        Try {
            import spark.implicits._

            val partitionPathOfBeginTimestampDay: String = beginCryptoPartitionKey.getPartitionPath(path2)
            val allPaths: Seq[String] = FileHelper.getAllDirFromTimestamp(path1, beginTs, beginCryptoPartitionKey)

            val partitionPathOfEndTimestampDay: String = endCryptoPartitionKey.getPartitionPath(path2)
            val toExcludePaths: Seq[String] = FileHelper.getAllDirFromTimestamp(path1, endTs, endCryptoPartitionKey)

            val filteredPaths: Seq[String] = allPaths.filterNot(toExcludePaths.contains) ++ Seq(partitionPathOfBeginTimestampDay, partitionPathOfEndTimestampDay)

//            println(s"filtered paths for : $beginTs -> $endTs" )
//            filteredPaths.foreach(println)
//            println(s"end filtered paths for : $beginTs -> $endTs" )

            filteredPaths.map(spark.read.parquet(_).as[Crypto]).reduce(_.union(_))

        }.mapException(e => {
            println("getPartitionsUniFromPathBetweenTwoTimestamps")
            println(e.getMessage)
            new Exception("There is something wrong", e)
        }).toOption
    }

    def getPartitionsUniFromPathFromLastTimestamp(spark: SparkSession, prefix: String, path1: String,
                                                  path2: String, todayPath: String, ts: Timestamp,
                                                  lastCryptoPartitionKey: CryptoPartitionKey): Option[Dataset[Crypto]] = {

        Try {
            import spark.implicits._

            val todayDs: Dataset[Crypto] = spark.read.parquet(todayPath).as[Crypto]
            if (todayDs.count == 0) {
                throw new RuntimeException("There is no today data")
            } else {
                val partitionPathOfLastTimestampDay: String = lastCryptoPartitionKey.getPartitionPath(path2)
                val allPaths: Seq[String] = FileHelper.getAllDirFromTimestamp(path1, ts, lastCryptoPartitionKey)
                val pathForSpark: String = FileHelper.getPathForSpark(partitionPathOfLastTimestampDay)
                val dsFromLastTimestampDay: Option[Dataset[Crypto]] =
                    Try {
                        spark.read.parquet(pathForSpark).as[Crypto]
                    }.mapException(e => {
                        println(e.getMessage())
                        new Exception("there is some problem")
                    }).toOption

                if (dsFromLastTimestampDay.isEmpty) {
                    todayDs.filter(_.cryptoValue.datetime.afterOrSame(ts))
                } else {
                    val filteredDsFromLastTimestampDay = dsFromLastTimestampDay.get.filter(_.cryptoValue.datetime.afterOrSame(ts))
                    if (allPaths.nonEmpty) {
                        allPaths.map(spark.read.parquet(_).as[Crypto]).reduce(_.union(_))
                          .union(todayDs)
                          .union(filteredDsFromLastTimestampDay)
                    } else {
                        todayDs.union(filteredDsFromLastTimestampDay)
                    }
                }
            }
        }.mapException(e => {
            println("getPartitionsUniFromPathFromLastTimestamp")
            println(e.getMessage)
            new Exception("There is something wrong", e)
        }).toOption
    }
    
    
    def getPartitionFromPathFromLastTimestamp(spark: SparkSession, path: String, ts: Timestamp): Option[Dataset[Crypto]] = {
        Try {
            import spark.implicits._
            spark.read.parquet(path).as[Crypto].filter(_.cryptoValue.datetime.afterOrSame(ts))
        }.mapException(e => {
            println("getPartitionFromPathFromLastTimestamp")
            println(e.getMessage)
            new Exception("path is not a parquet", e)
        }).toOption
    }
    
    def getPartition(ss: SparkSession, parquetsDir: String, key: CryptoPartitionKey): Option[Dataset[Crypto]] = {
        val path = key.getPartitionPath(parquetsDir)
        getPartitionFromPath(ss, path)
    }
}

case class Crypto(partitionKey: CryptoPartitionKey,
                  cryptoValue: CryptoValue,
                  tradeMode: Option[String],
                  count: Option[Int],
                  processingDt: Timestamp,
                  prediction: Option[CryptoPrediction]) {
    def flatten: FlattenCrypto = {
        import cryptoValue._
        import partitionKey._
        FlattenCrypto(
            asset = asset,
            currency = currency,
            provider = provider,
            year = year,
            month = month,
            day = day,
            processingDt = processingDt,
            datetime = datetime,
            value = value,
            volume = volume,
            prediction = prediction.map(_.prediction),
            accuracy = prediction.flatMap(_.accuracy),
            predictionDt = prediction.map(_.predictionDt)
        )
    }
}

case class FlattenCrypto(processingDt: Timestamp,
                         asset: String,
                         currency: String,
                         provider: String,
                         year: String,
                         month: String,
                         day: String,
                         datetime: Timestamp,
                         value: Double,
                         volume: Double,
                         prediction: Option[Double],
                         accuracy: Option[Double],
                         predictionDt: Option[Timestamp]) {

    def toLine(): String =
        ("" /: this.getClass.getDeclaredFields) { (a, f) =>
            f.setAccessible(true)
            a + ";" + f.get(this)
        }.substring(1)
}

