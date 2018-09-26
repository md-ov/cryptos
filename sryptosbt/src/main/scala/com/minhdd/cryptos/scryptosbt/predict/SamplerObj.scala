package com.minhdd.cryptos.scryptosbt.predict

import com.minhdd.cryptos.scryptosbt.Sampler
import com.minhdd.cryptos.scryptosbt.parquet.{Crypto, CryptoPartitionKey}
import com.minhdd.cryptos.scryptosbt.predict.SamplerObj.oneCrypto
import org.apache.spark.sql.{Dataset, SparkSession}

object SamplerObj {
    def oneCrypto(cryptos: Seq[Crypto]) = cryptos.head
    def toSeperate(last: Crypto, next: Crypto): Boolean = true
    
    def run(args: Sampler, master: String): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        
        val parquetPath = CryptoPartitionKey.getOHLCParquetPath(
            parquetsDir = args.parquetsDir, asset = args.asset, currency = args.currency)
        println(parquetPath)
        val ds: Option[Dataset[Crypto]] = Crypto.getPartitionFromPath(ss, parquetPath)
        
        if (ds.isDefined) {
            val dsget: Dataset[Crypto] = ds.get
            println(dsget.count())
            val seperatedCryptos: Dataset[Crypto] = seperate(toSeperate, oneCrypto, dsget, ss)
            println(seperatedCryptos.count())
        }
    }
    
    private def seperate(toSeperate: (Crypto, Crypto) => Boolean, oneCrypto: Seq[Crypto] => Crypto,
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
                            if (toSeperate(lastCrypto, nextCrypto)) {
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
