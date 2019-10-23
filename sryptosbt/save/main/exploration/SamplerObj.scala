package com.minhdd.cryptos.scryptosbt.exploration

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.Sampler
import com.minhdd.cryptos.scryptosbt.domain.{Crypto, CryptoPartitionKey, CryptoValue}
import com.minhdd.cryptos.scryptosbt.parquet.{CryptoPartitionKey, CryptoValue}
import com.minhdd.cryptos.scryptosbt.tools.{DateTimeHelper, SparkHelper, TimestampHelper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object SamplerObj {
    
    def oneCrypto(cryptos: Seq[Crypto]): Crypto = {
        val allCount: Int = cryptos.map(_.count.getOrElse(0)).sum
        val totalVolume: Double = cryptos.map(_.cryptoValue.volume).sum
//        val averageVolume: Double = totalVolume / allCount
        
        val first = cryptos.head
        val count = cryptos.size
        if (count == 1) first.copy(processingDt = TimestampHelper.now) else {
            val cryptoValue: CryptoValue = cryptos.map(_.cryptoValue).sortWith(_.value > _.value).apply(count / 2)
            first.copy(
                cryptoValue = cryptoValue.copy(volume = totalVolume), 
                tradeMode = None,
                count = Option(allCount), 
                processingDt = TimestampHelper.now)
        }
    }
    def toSeparate(deltaValue: Double)(first: Crypto, last: Crypto, next: Crypto): Boolean = {
        Math.abs(first.cryptoValue.value - next.cryptoValue.value) > deltaValue ||
        Math.abs(last.cryptoValue.value - next.cryptoValue.value) > deltaValue
    }
    
    def run(args: Sampler, master: String): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("sampler").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath(
            parquetsDir = args.parquetsDir, asset = args.asset, currency = args.currency)
        println(parquetPath)
        val ds: Option[Dataset[Crypto]] = Crypto.getPartitionFromPath(ss, parquetPath)
        
        if (ds.isDefined) {
            val dsget: Dataset[Crypto] = ds.get
            println(dsget.count())
            val seperatedCryptos: Dataset[Crypto] = seperate(toSeparate(args.delta), oneCrypto, dsget, ss)
            println(seperatedCryptos.count())
            SparkHelper.csvFromDSCrypto(ss, args.csvpath, seperatedCryptos)
        }
    }
    
    private def seperate(toSeperate: (Crypto, Crypto, Crypto) => Boolean, oneCrypto: Seq[Crypto] => Crypto,
                         dsget: Dataset[Crypto], ss: SparkSession) = {
        import ss.implicits._
        dsget.mapPartitions(iterator =>
            
            if (!iterator.hasNext) iterator else {
                new Iterator[Crypto]() {
                    var first: Option[Crypto] = Some(iterator.next())
                    
                    override def hasNext: Boolean = first.isDefined
                    
                    override def next(): Crypto = {
                        val nextSeqCrypto = seqCrypto(Seq(first.get), first.get)
                        oneCrypto(nextSeqCrypto)
                    }
                    
                    def seqCrypto(cryptosAccumulated: Seq[Crypto], lastCrypto: Crypto): Seq[Crypto] = {
                        if (iterator.hasNext) {
                            val nextCrypto = iterator.next();
                            if (toSeperate(first.get, lastCrypto, nextCrypto)) {
                                first = Some(nextCrypto)
                                cryptosAccumulated
                            } else {
                                seqCrypto(cryptosAccumulated :+ nextCrypto, nextCrypto)
                            }
                        } else {
                            first = None
                            cryptosAccumulated
                        }
                    }
                }
            }
        )
    }
    
    def getAdjustedDatetime(numberOfMinutesBetweenTwoElement: Int)(dateTime: DateTime): DateTime = {
        val minutes = dateTime.getMinuteOfHour
        val delta: Int = minutes % numberOfMinutesBetweenTwoElement
        dateTime.minusMinutes(delta)
    }
    
    case class CryptoAndNextDatetime(crypto: Crypto, nextdt: Timestamp)
    case class CryptoWrapper(crypto: Crypto)
    def encoderCryptoWrapper(ss: SparkSession): Encoder[CryptoWrapper] = {
        import ss.implicits._
        implicitly[Encoder[CryptoWrapper]]
    }
    def encoderCryptoAndNextDatetime(ss: SparkSession): Encoder[CryptoAndNextDatetime] = {
        import ss.implicits._
        implicitly[Encoder[CryptoAndNextDatetime]]
    }
    
    def fill(crypto: Crypto, datetime: Timestamp, nextdt: Timestamp, numberOfMinutesBetweenTwoElement: Int): Seq[Crypto] = {
        if (nextdt == null) Seq(crypto) else {
            val tss: Seq[Timestamp] = DateTimeHelper.getTimestamps(datetime, nextdt, numberOfMinutesBetweenTwoElement)
            val cryptoValue = crypto.cryptoValue
            tss.map(ts => crypto.copy(cryptoValue = cryptoValue.copy(datetime = ts)))
        }
    }
    
    def adjustSecond(dt: DateTime): DateTime = {
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm'Z'")
        DateTime.parse(formatter.print(dt), formatter)
    }
    
    def sampling(ss: SparkSession, ds: Dataset[Crypto], numberOfMinutesBetweenTwoElement: Int = 15): Dataset[Crypto]= {
        //        val timestampsDelta: Int = Timestamps.oneDayTimestampDelta * numberOfMinutesBetweenTwoElement / (24*60)
        val adjustDatetime: DateTime => DateTime = getAdjustedDatetime(numberOfMinutesBetweenTwoElement)
        //        val adjustedNow: DateTime = adjustDatetime(DateTime.now())
        
        def adjustTimestamp(ts: Timestamp): DateTime = adjustSecond(adjustDatetime(DateTimeHelper.getDateTime(ts)))
        import ss.implicits._
    
        val groupedAndTransformed: RDD[Crypto] =
            ds.rdd.map(c => (adjustTimestamp(c.cryptoValue.datetime), c))
              .groupByKey().mapValues(g => oneCrypto(g.toSeq))
              .map{case (dt, c) => c.copy(cryptoValue = c.cryptoValue.copy(datetime = TimestampHelper.fromDatetime(dt)))}
    
    
        ///// dedup

//        val cryptoWithAdjustedTimestamp: Dataset[(Timestamp, Crypto)] =
//            ds.map(c => (Timestamps.fromDatetime(adjustTimestamp(c.cryptoValue.datetime)), c))
//        val deduped: Dataset[(Timestamp, Crypto)] = cryptoWithAdjustedTimestamp.dropDuplicates("_1")
//        val dedupedCryptoDataset: Dataset[Crypto] =
//            deduped.map{case (t, c) => c.copy(cryptoValue = c.cryptoValue.copy(datetime = t))}

        //////

        val wrapped: Dataset[CryptoWrapper] = groupedAndTransformed.map(c => CryptoWrapper(c)).toDS()
        
        
//        (encoderCryptoWrapper(ss))

        val datetimeColumnName = "crypto.cryptoValue.datetime"
        val volumeColumnName = "crypto.cryptoValue.volume"
        val window = Window.orderBy(datetimeColumnName, volumeColumnName)

        import org.apache.spark.sql.functions.lead

        val dsss: Dataset[CryptoAndNextDatetime] =
            wrapped
              .withColumn("nextdt", lead(datetimeColumnName, 1).over(window))
              .as[CryptoAndNextDatetime](encoderCryptoAndNextDatetime(ss))

        val finalDs: Dataset[Crypto] =
            dsss.flatMap(cryptoAndNextDatetime =>
                fill(crypto = cryptoAndNextDatetime.crypto,
                    datetime = cryptoAndNextDatetime.crypto.cryptoValue.datetime,
                    nextdt = cryptoAndNextDatetime.nextdt,
                    numberOfMinutesBetweenTwoElement = numberOfMinutesBetweenTwoElement))(Crypto.encoder(ss))

        finalDs
    }

}
