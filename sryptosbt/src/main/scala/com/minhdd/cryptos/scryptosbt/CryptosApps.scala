package com.minhdd.cryptos.scryptosbt

import caseapp._
import com.minhdd.cryptos.scryptosbt.predict.Predictor
import com.minhdd.cryptos.scryptosbt.toparquet.CSVToParquet

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
    
    private def getMaster(master: String) = {
        if (master == "local") "local[*]"
        else if (master == "ov") "local[*]"
        else "local[*]"
    }
    
    override def run(options: CommandAppArgs, remainingArgs: RemainingArgs): Unit = {
        println(options match {
            case args: Predict => Predictor.predict(args)
            case args: ParquetFromCsv => CSVToParquet.parquetFromCsv(args, getMaster(args.master))
        })
    }
}
