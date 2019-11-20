package com.minhdd.cryptos.scryptosbt.predict.ml2

import com.minhdd.cryptos.scryptosbt.exploration.BeforeSplit
import com.minhdd.cryptos.scryptosbt.constants._
import com.minhdd.cryptos.scryptosbt.tools.{ModelHelper, TimestampHelper}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ml2.{label, predict, prediction}

object Regressor {
    val segmentDirectory = "all-190705-from-brut"
    
    def main(args: Array[String]): Unit = {
//        resultss()
//        t
//        predictOneSegment()
//        trainingModelAndWriteModelAndTestDfWithRawPrediction
        exploreTestDfAndFindThreshold
//        whyThereIsSomeNull
    }
    
    def whyThereIsSomeNull() = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        import ss.implicits._
        val ds = ss.read.parquet(s"$dataDirectory\\segments\\$segmentDirectory\\$beforesplits").as[Seq[BeforeSplit]]
        val f: Dataset[Seq[BeforeSplit]] = ds.filter(s => s.exists(_.secondDerive.isEmpty))
        f.show(false)
    }
    
    
    
    def exploreTestDfAndFindThreshold(): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df: DataFrame = ss.read.parquet(s"$dataDirectory\\segments\\$segmentDirectory\\result")
        
        exploreDfAndFindThreshold(ss, df)
    }
    
    

}
