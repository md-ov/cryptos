package com.minhdd.cryptos.scryptosbt.model.app

import java.io.{BufferedWriter, File, FileWriter}

import com.minhdd.cryptos.scryptosbt.model.service.ml.{upDownPath}
import com.minhdd.cryptos.scryptosbt.env.dataDirectory
import com.minhdd.cryptos.scryptosbt.model.service.ml.{label, predict, prediction}
import com.minhdd.cryptos.scryptosbt.tools.DateTimeHelper
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

//3
//après predictor
//set threshold (for positive or negative)
object Distributions {
    def main(args: Array[String]): Unit = {
//                distribution()
//        percentiles()
        counts()
    }
    
    val spark: SparkSession = SparkSession.builder()
      .config("spark.driver.maxResultSize", "3g")
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "60s")
      .appName("big segments")
      .master("local[*]").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    val df: DataFrame = spark.read.parquet(s"$dataDirectory/ml/results/$upDownPath")
    val thresholdForPositive = 0.5091548599903941
    val thresholdForNegative = 0.06671776956132627

    val binarizerForSegmentDetection = new Binarizer()
      .setInputCol(prediction)
      .setOutputCol(predict)
    binarizerForSegmentDetection.setThreshold(thresholdForPositive)
    val binarizedResultsForPositive: DataFrame = binarizerForSegmentDetection.transform(df)
    val notokPositive =
        binarizedResultsForPositive.filter(!(col(predict) === col(label))).filter(col(predict) === 1.0)
    val okPositive = binarizedResultsForPositive.filter(col(predict) === col(label)).filter(col(predict) === 1.0)

    binarizerForSegmentDetection.setThreshold(thresholdForNegative)
    val binarizedResultsForNegative: DataFrame = binarizerForSegmentDetection.transform(df)

    val notokNegative = binarizedResultsForNegative.filter(!(col(predict) === col(label))).filter(col(predict) === 0.0)
    val okNegative = binarizedResultsForNegative.filter(col(predict) === col(label)).filter(col(predict) === 0.0)

    val ok = okPositive.union(okNegative)
    val notok = notokPositive.union(notokNegative)

    def counts(): Unit = {
        val notOKSmallerThan25Elements = notokNegative.filter(col("numberOfElement") <= 25).count()
        println("number of bad negative prediction Smaller Than 25 Elements: " + notOKSmallerThan25Elements)

        val oKSmallerThan25Elements = okNegative.filter(col("numberOfElement") <= 25).count()
        println("number of good negative prediction Smaller Than 25 Elements: " + oKSmallerThan25Elements)

        println("it is : " + notOKSmallerThan25Elements.toDouble*100/oKSmallerThan25Elements)

        val notOKSmallerThan35Elements = notokNegative.filter(col("numberOfElement") <= 35).count()
        println("number of bad negative prediction Smaller Than 35 Elements: " + notOKSmallerThan35Elements)

        val oKSmallerThan35Elements = okNegative.filter(col("numberOfElement") <= 35).count()
        println("number of good negative prediction Smaller Than 35 Elements: " + oKSmallerThan35Elements)

        println("it is : " + notOKSmallerThan35Elements.toDouble*100/oKSmallerThan35Elements)

        println("total : " + notokNegative.count.toDouble * 100 / okNegative.count)
    }

    def distribution(): Unit = {
        println("notokPositive")
        notokPositive.groupBy("numberOfElement").count().orderBy("numberOfElement").show(1000, false)
        println("notokNegative")
        notokNegative
          .withColumn("numberElement100", (col("numberOfElement") / 100).cast("integer"))
          .groupBy("numberElement100")
          .count().orderBy("numberElement100")
          .show(1000, false)
        
        println("okPositive")
        okPositive.groupBy("numberOfElement").count().orderBy("numberOfElement").show(1000, false)
        println("okNegative")
        okNegative.withColumn("numberElement10", (col("numberOfElement") / 10).cast("integer"))
          .groupBy("numberElement10")
          .count().orderBy("numberElement10")
          .show(1000, false)
        
    }
    
    def percentiles() = {
        val positive = binarizedResultsForPositive.filter(col(predict) === 1.0)
        val negative = binarizedResultsForNegative.filter(col(predict) === 0.0)
        val percentiles: Array[Double] = (0 until 100).map(i => i.toDouble / 100).toArray
        val persNotOk: Array[Double] = notok.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persNotokPositive: Array[Double] = notokPositive.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persNotokNegative: Array[Double] = notokNegative.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persOk: Array[Double] = ok.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persOkPositive: Array[Double] = okPositive.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persOkNegative: Array[Double] = okNegative.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persPositive: Array[Double] = positive.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persNegative: Array[Double] = negative.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        
        val file = new File(s"$dataDirectory/ml/results/percentiles/${DateTimeHelper.now}.csv")
        val bw = new BufferedWriter(new FileWriter(file))
        val data = Seq(percentiles, percentiles, persNotOk, persNotokPositive, persNotokNegative, persOk, persOkPositive, persOkNegative, persPositive, persNegative)
        percentiles.indices.map(i => data.map(_.apply(i))).foreach(line => {
            bw.write(line.mkString(","))
            bw.newLine()
        })
        bw.close()
    }
}
