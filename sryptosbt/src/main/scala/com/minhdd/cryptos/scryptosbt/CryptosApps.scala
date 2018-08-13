package com.minhdd.cryptos.scryptosbt

import caseapp._

sealed trait CommandAppArgs

case class Predict(
    dt: String,
    endDt: Option[String],
    unit: Option[String],
    step: Option[Int]
) extends CommandAppArgs

case class ParquetFromCsv(
    csvpath: String,
    parquetPath: String
) extends CommandAppArgs

object CryptosApps extends CommandApp[CommandAppArgs]{
    
    def parquetFromCsv(args: ParquetFromCsv): String = {
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
