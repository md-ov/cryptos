package com.minhdd.cryptos.scryptosbt.predict

import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.parquet.Crypto
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


object OHLCAndTradesExplorator {
    def explorate(ss:SparkSession, ohlc: Dataset[Crypto], trades: Dataset[Crypto], outputDir: String): Unit = {
        import ss.implicits._
        val sampledOhlcDataSet: Dataset[Crypto] = SamplerObj.sampling(ss, ohlc)
        val sampledTradesDataSet: Dataset[Crypto] = SamplerObj.sampling(ss, trades)
        
        val aa: Dataset[(Timestamp, Crypto)] = sampledOhlcDataSet.map(x => (x.cryptoValue.datetime, x))
        val bb: Dataset[(Timestamp, Crypto)] = sampledTradesDataSet.map(x => (x.cryptoValue.datetime, x))

        aa.show(10, false)
        bb.show(10, false)
//        
//        val joinedd = aa.join(bb, "_1")
//        println(joinedd.count)
//    
//        joinedd.show(10, false)
//        
//        val joined: Dataset[((Timestamp, Crypto), (Timestamp, Crypto))] = aa.joinWith(bb, col("_1"))
//    
//        val sampled = joined.map(el => {
//              val e1: (Timestamp, Crypto) = el._1
//              val e2: (Timestamp, Crypto) = el._2
//              e1._2.copy(cryptoValue = e1._2.cryptoValue.copy(volume = e2._2.cryptoValue.volume))
//          })
//        
//
//        sampled.show(10, false)
    }
}
