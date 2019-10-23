package com.minhdd.cryptos.scryptosbt.tools

import org.apache.spark.sql.DataFrame

object DataFrameHelper {
    def derive(df: DataFrame, yColumn: String, xColumn: String, newCol: String): DataFrame = {
        import org.apache.spark.sql.expressions.Window
        val window = Window.orderBy(xColumn)
        import org.apache.spark.sql.functions.{lag, lead}
        
        df.withColumn(newCol,
            (lead(yColumn, 1).over(window) - lag(yColumn, 1).over(window))
              /
              (lead(xColumn, 1).over(window) - lag(xColumn, 1).over(window)))
    }
}
