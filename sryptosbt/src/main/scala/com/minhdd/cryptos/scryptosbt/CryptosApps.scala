package com.minhdd.cryptos.scryptosbt

import caseapp._
import com.minhdd.cryptos.scryptosbt.predict.Predictor
import com.minhdd.cryptos.scryptosbt.parquet.{CSVFromParquetObj, ParquetFromCSVObj, ToParquetsFromCSV}

sealed trait CommandAppArgs

case class CSVFromParquet(
                           master: String,
                           csvpath: String,
                           parquetPath: String
                         ) extends CommandAppArgs

case class ExtractToCsv(
                         master: String,
                         parquetsDir: String,
                         asset: String,
                         currency: String,
                         csvpath: String,
                         startDay: String,
                         endDay: String,
                         n: Int // number of elements for one day
                       ) extends CommandAppArgs

case class ParquetFromCsv(
                           api: String,
                           master: String,
                           csvpath: String,
                           parquetPath: String
                         ) extends CommandAppArgs

case class ToParquetsFromCsv(
                              api: String,
                              master: String,
                              inputDir: String,
                              parquetsDir: String,
                              minimum: Long  //minimum number for one partition
                         ) extends CommandAppArgs

case class Predict(
                    dt: String,
                    asset: String,
                    currency: String,
                    endDt: Option[String],
                    unit: Option[String],
                    step: Option[Int]
                  ) extends CommandAppArgs

object CryptosApps extends CommandApp[CommandAppArgs]{
    
    private def getMaster(master: String) = {
        if (master == "local") "local[*]"
        else if (master == "ov") "local[*]"
        else "local[*]"
    }
    
    override def run(options: CommandAppArgs, remainingArgs: RemainingArgs): Unit = {
        println(options match {
            case args: Predict => Predictor.predict(args)
            case args: ParquetFromCsv => ParquetFromCSVObj.run(args, getMaster(args.master))
            case args: ToParquetsFromCsv => ToParquetsFromCSV.run(args, getMaster(args.master))
            case args: CSVFromParquet => CSVFromParquetObj.run(args, getMaster(args.master))
            case args: ExtractToCsv => ExtractToCsvObj.run(args, getMaster(args.master))
    
        })
    }
}
