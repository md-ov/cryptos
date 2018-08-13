package com.minhdd.cryptos.scryptosbt

import caseapp._
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

sealed trait CommandAppArgs

case class Predict(
    dt: String,
    endDt: Option[String],
    unit: Option[String],
    step: Option[Int]
) extends CommandAppArgs

case class ParquetFromCsv(
    master: String,                     
    csvpath: String,
    parquetPath: String
) extends CommandAppArgs

object CryptosApps extends CommandApp[CommandAppArgs]{
    
    def getMaster(master: String) = {
        if (master == "local") "local[*]"
        else if (master == "ov") "local[*]"
        else "local[*]"
    }
    
    def encoder(ss: SparkSession): Encoder[CryptoValue] = {
        import ss.implicits._
        implicitly[Encoder[CryptoValue]]
    }
    
    def toCryptoValue(line: String): CryptoValue = {
        CryptoValue()
    }

    def parquetFromCsv(args: ParquetFromCsv): String = {
        val master = getMaster(args.master)
        val ss: SparkSession = SparkSession.builder().appName("toParquet").master(master).getOrCreate()
        ss.sparkContext.setLogLevel("WARN")
        val ds: Dataset[CryptoValue] = ss.read.textFile(args.csvpath).map(toCryptoValue)(encoder(ss))
        ds.write.parquet(args.parquetPath)
        "status|SUCCESS"
    }
    
    def predict(args: Predict): String = {
        "status|SUCCESS, result|366"
    }
    
    override def run(options: CommandAppArgs, remainingArgs: RemainingArgs): Unit = {
        println(options match {
            case args: Predict => predict(args)
            case args: ParquetFromCsv => parquetFromCsv(args)
     
        })
    }
}
