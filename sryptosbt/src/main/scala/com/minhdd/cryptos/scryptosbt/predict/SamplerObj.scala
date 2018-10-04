package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.Sampler
import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey, CryptoValue}
import com.minhdd.cryptos.scryptosbt.tools.{Sparks, Timestamps}
import org.apache.spark.sql.{Dataset, SparkSession}

object SamplerObj {
    
    def oneCrypto(cryptos: Seq[Crypto]) = {
        val first = cryptos.head
        val count = cryptos.size
        if (count == 1) first.copy(processingDt = Timestamps.now) else {
            val cryptoValue: CryptoValue = cryptos.map(_.cryptoValue).sortWith(_.value > _.value).apply(count / 2)
            first.copy(cryptoValue = cryptoValue, processingDt = Timestamps.now)
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
            Sparks.csvFromDSCrypto(ss, args.csvpath, seperatedCryptos)
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
}
