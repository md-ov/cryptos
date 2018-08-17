package com.minhdd.cryptos.scryptosbt.tools

import com.minhdd.cryptos.scryptosbt.parquet.CSVFromParquetObj.merge
import org.apache.spark.sql.Dataset

object Sparks {
    def csvFromDSString(dsString: Dataset[String], csvPath: String) = {
        dsString.coalesce(1)
          .write.format("com.databricks.spark.csv")
          .save(csvPath)
        merge(csvPath, csvPath + ".csv")
    }
}
