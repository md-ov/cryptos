package com.minhdd.cryptos.scryptosbt.tools

import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ModelHelper {
    
    def saveModel(ss: SparkSession, model: CrossValidatorModel, modelPath: String): Unit = {
        ss.sparkContext.parallelize(Seq(model), 1).saveAsObjectFile(modelPath)
    }
    def getModel(ss: SparkSession, modelPath: String): CrossValidatorModel = {
        val a: RDD[CrossValidatorModel] = ss.sparkContext.objectFile[CrossValidatorModel](modelPath)
        a.first()
    }
}
