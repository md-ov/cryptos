package com.minhdd.cryptos.scryptosbt

import caseapp._
import com.minhdd.cryptos.scryptosbt.parquet.{ToParquetsFromCSV, ToParquetsFromTodayCSV}

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
                         n: Option[Int] // number of elements for one day
                       ) extends CommandAppArgs

case class Sampler(
                    master: String,
                    parquetsDir: String,
                    csvpath: String,
                    asset: String,
                    currency: String,
                    delta: Double
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
                              minimum: Long //minimum number for one partition
                            ) extends CommandAppArgs

case class ToParquetsFromTodayCsv(
                                   api: String,
                                   master: String,
                                   inputDir: String,
                                   parquetsDir: String,
                                   minimum: Long //minimum number for one partition
                                 ) extends CommandAppArgs

case class Predict(
                    dt: String,
                    asset: String,
                    currency: String,
                    endDt: Option[String],
                    unit: Option[String],
                    step: Option[Int]
                  ) extends CommandAppArgs

object CryptosApps extends CommandApp[CommandAppArgs] {
    
    private def getMaster(master: String): String = {
        if (master == "local") "local[*]"
        else if (master == "ov") "local[*]"
        else "local[*]"
    }
    
    override def run(options: CommandAppArgs, remainingArgs: RemainingArgs): Unit = {
        println(options match {
            case args: ToParquetsFromCsv => ToParquetsFromCSV.run(args, getMaster(args.master))
            case args: ToParquetsFromTodayCsv => ToParquetsFromTodayCSV.run(args, getMaster(args.master))
        })
    }
}
