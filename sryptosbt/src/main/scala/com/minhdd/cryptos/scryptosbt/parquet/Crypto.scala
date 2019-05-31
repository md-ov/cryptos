package com.minhdd.cryptos.scryptosbt.parquet

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.tools.{DateTimes, Numbers, Timestamps}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.util.{Failure, Try}

case class CryptoPartitionKey (
    asset: String,
    currency: String,
    provider: String,
    api: String,
    year: String,
    month: String,
    day: String                     
) {
    def getTodayPartitionPath(parquetsDir: String): String = {
        val separator = if (!parquetsDir.contains("\\")) "/" else "\\"
        val fullParquetDir = if (parquetsDir.endsWith(separator)) parquetsDir else parquetsDir + separator
        val path = fullParquetDir +
          asset.toUpperCase + separator +
          currency.toUpperCase + separator +
          api.toUpperCase + separator + 
          "today" + separator + "parquet"
        path
    }
    
    def date(): String = DateTimes.getDate(year, month, day)
    
    def getPartitionPath(parquetsDir: String) = {
        val separator = if (!parquetsDir.contains("\\")) "/" else "\\"
        val fullParquetDir = if (parquetsDir.endsWith(separator)) parquetsDir else parquetsDir + separator
        val path = fullParquetDir + 
          asset.toUpperCase + separator + 
          currency.toUpperCase + separator +
          api.toUpperCase + separator +
          year + separator + month + separator + day + separator + 
          provider.toUpperCase + separator +
          api.toUpperCase + separator +
          "parquet" 
        path
    }
    
    def getOHLCPath(parquetsDir: String): String = {
        CryptoPartitionKey.getOHLCParquetPath(parquetsDir, asset, currency)
    }
    
}

object CryptoPartitionKey {
    def fusion(keys: Seq[CryptoPartitionKey]): CryptoPartitionKey = {
        def getFusionValue(values: Seq[String]) = {
            if (values.size == 1) values.head else values.mkString(":")
        }
        CryptoPartitionKey(
            asset = getFusionValue(keys.map(_.asset).distinct),
            currency = getFusionValue(keys.map(_.currency).distinct),
            provider = getFusionValue(keys.map(_.provider).distinct),
            api = getFusionValue(keys.map(_.api).distinct),
            year = getFusionValue(keys.map(_.year).distinct),
            month = getFusionValue(keys.map(_.month).distinct),
            day = getFusionValue(keys.map(_.day).distinct)
        )
    }
    
    def getOHLCParquetPath(parquetsDir: String, asset: String, currency: String): String = {
        val separator = if (!parquetsDir.contains("\\")) "/" else "\\"
        val fullParquetDir = if (parquetsDir.endsWith(separator)) parquetsDir else parquetsDir + separator
        val path = fullParquetDir +
          asset.toUpperCase + separator +
          currency.toUpperCase + separator +
          "OHLC" + separator + "parquet"
        path
    }
    
    def getTRADESParquetPath(parquetsDir: String, asset: String, currency: String): String = {
        val separator = if (!parquetsDir.contains("\\")) "/" else "\\"
        val fullParquetDir = if (parquetsDir.endsWith(separator)) parquetsDir else parquetsDir + separator
        val path = fullParquetDir +
          asset.toUpperCase + separator +
          currency.toUpperCase + separator +
          "TRADES" 
        path
    }
}

case class CryptoValue (
    datetime : Timestamp,
    value: Double,
    margin: Option[Margin],
    volume: Double                    
)

case class Margin (
    superior: Double,
    inferior: Double
)

case class CryptoPrediction (
    prediction: Double,
    accuracy: Option[Double],
    predictionDt: Timestamp                       
)

object Crypto {
    def parseOHLC(line: String): Seq[Crypto] = {
        val splits: Array[String] = line.split(";")
        val asset: String = splits.apply(0)
        val currency: String = splits.apply(1)
        val provider: String = splits.apply(2)
        val timestampPosition = 5
        val value: String = splits.apply(timestampPosition+1)
        val volume: String = splits.apply(timestampPosition+6)
        val count: String = splits.apply(timestampPosition+7)
        val ts: Timestamps = Timestamps(splits.apply(timestampPosition).toLong*1000)
        val partitionKey = CryptoPartitionKey(
            asset = asset.toUpperCase,
            currency = currency.toUpperCase,
            provider = provider.toUpperCase,
            api = "OHLC",
            year = ts.getYearString, month = ts.getMonthString, day = ts.getDayString
        )
        val processingDt: Timestamp = Timestamps.now
        val cryptoValue = CryptoValue(
            datetime = ts.timestamp,
            value = Numbers.toDouble(value),
            volume = Numbers.toDouble(volume),
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
        val ts: Timestamps = Timestamps((splits.apply(7).toDouble*1000).toLong)
        val tradeMode: String = splits.apply(8)
        val partitionKey = CryptoPartitionKey(
            asset = asset.toUpperCase,
            currency = currency.toUpperCase,
            provider = provider.toUpperCase,
            api = "TRADES",
            year = ts.getYearString, month = ts.getMonthString, day = ts.getDayString
        )
        val processingDt = Timestamps.now
        val cryptoValue = CryptoValue(
            datetime = ts.timestamp,
            value = Numbers.toDouble(value),
            volume = Numbers.toDouble(volume),
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
    
    def encoder(ss: SparkSession): Encoder[Crypto] = {
        import ss.implicits._
        implicitly[Encoder[Crypto]]
    }
    
    implicit class TryOps[T](val t: Try[T]) extends AnyVal {
        def mapException(f: Throwable => Throwable): Try[T] = {
            t.recoverWith({ case e => Failure(f(e)) })
        }
    }
    
    def getPartitionFromPath(ss: SparkSession, path: String): Option[Dataset[Crypto]] = {
        Try {
            ss.read.parquet(path).as[Crypto](encoder(ss))
        }.mapException(e => new Exception("path is not a parquet", e)).toOption
    }
    
    def getPartitionsUniFromPath(ss: SparkSession, prefix: String, path: String): Option[Dataset[Crypto]] = {
        import com.minhdd.cryptos.scryptosbt.tools.Files
        Try {
            val allPartitionsPath: Seq[String] = Files.getAllDir(path)
            val allPaths = allPartitionsPath.map(prefix + _)
//            allPaths.foreach(println)
            allPaths.map(ss.read.parquet(_).as[Crypto](encoder(ss))).reduce(_.union(_))
        }.mapException(e => new Exception("path is not a parquet", e)).toOption
    }
    
    def getPartitionsUniFromPathFromLastTimestamp(ss: SparkSession, prefix: String, path1: String,
                                                  path2: String, ts: Timestamp, 
                                                  lastCryptoPartitionKey: CryptoPartitionKey): Option[Dataset[Crypto]] = {
        import com.minhdd.cryptos.scryptosbt.tools.Files
        Try {
            val partitionPathOfLastTimestampDay: String = lastCryptoPartitionKey.getPartitionPath(path2)
            val allPaths: Seq[String] = Files.getAllDirFromLastTimestamp(path1, ts, lastCryptoPartitionKey)
            val dsFromLastTimestampDay: Dataset[Crypto] = 
                ss.read.parquet(Files.getPathForSpark(partitionPathOfLastTimestampDay)).as[Crypto](encoder(ss))
            val filteredDsFromLastTimestampDay = dsFromLastTimestampDay.filter(c => Timestamps.afterOrSame(ts, c.cryptoValue.datetime))
            println()
            println(filteredDsFromLastTimestampDay.count())
            allPaths
              .map(ss.read.parquet(_).as[Crypto](encoder(ss)))
              .reduce(_.union(_))
              .union(filteredDsFromLastTimestampDay)
        }.mapException(e => new Exception("path is not a parquet", e)).toOption
    }
    
    
    def getPartitionFromPathFromLastTimestamp(ss: SparkSession, path: String, ts: Timestamp)
    : Option[Dataset[Crypto]] = {
        Try {
            ss.read.parquet(path).as[Crypto](encoder(ss)).filter(c => Timestamps.afterOrSame(ts, c.cryptoValue.datetime))
        }.mapException(e => new Exception("path is not a parquet", e)).toOption
    }
    
    def getPartition(ss: SparkSession, parquetsDir: String, key: CryptoPartitionKey): Option[Dataset[Crypto]] = {
        val path = key.getPartitionPath(parquetsDir)
        getPartitionFromPath(ss, path)
    }
}

case class Crypto
(
  partitionKey : CryptoPartitionKey,
  cryptoValue : CryptoValue,
  tradeMode : Option[String],
  count: Option[Int],
  processingDt : Timestamp,
  prediction : Option[CryptoPrediction]
) {
    def flatten: FlattenCrypto = {
        import partitionKey._
        import cryptoValue._
        FlattenCrypto(
            asset =  asset,
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

case class FlattenCrypto (
   processingDt : Timestamp,
   asset: String,
   currency: String,
   provider: String,
   year: String,
   month: String,
   day: String,
   datetime : Timestamp,
   value: Double,
   volume: Double,
   prediction: Option[Double],
   accuracy: Option[Double],
   predictionDt: Option[Timestamp]
) {
    def toLine(): String =
        ("" /: this.getClass.getDeclaredFields) { (a, f) =>
            f.setAccessible(true)
            a + ";" + f.get(this)
        }.substring(1)
}


    