package com.minhdd.cryptos.scryptosbt.predict.ml2

import java.io.{BufferedWriter, File, FileWriter}

import com.minhdd.cryptos.scryptosbt.constants.dataDirectory
import com.minhdd.cryptos.scryptosbt.predict.ml2.ml2.{label, predict, prediction}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DistributionExplorer {
    def distribution(): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        import org.apache.spark.sql.functions._
        val segmentDirectory = "all-190427"
        val df: DataFrame = ss.read.parquet(s"$dataDirectory\\csv\\segments\\$segmentDirectory\\result")
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
          .setThreshold(1.019)
        val segmentDetectionBinaryResults: DataFrame = binarizerForSegmentDetection.transform(df)
        val notok = segmentDetectionBinaryResults.filter(!(col(predict) === col(label)))
        val notokPositive =
            segmentDetectionBinaryResults.filter(!(col(predict) === col(label))).filter(col(predict) === 1.0)
        val notokNegative = segmentDetectionBinaryResults.filter(!(col(predict) === col(label))).filter(col(predict)
          === 0.0)
        val okPositive = segmentDetectionBinaryResults.filter(col(predict) === col(label)).filter(col(predict) === 1.0)
        val okNegative = segmentDetectionBinaryResults.filter(col(predict) === col(label)).filter(col(predict) === 0.0)
        
        val pers: Array[Double] = notok.stat.approxQuantile("numberOfElement",
            Array(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9), 0.00001)
        
        println("notokPositive")
        notokPositive.groupBy("numberOfElement").count().orderBy("numberOfElement").show(1000, false)
        println("notokNegative")
        notokNegative
          .withColumn("numberElement100", (col("numberOfElement")/100).cast("integer"))
          .groupBy("numberElement100")
          .count().orderBy("numberElement100")
          .show(1000, false)
        
        println("okPositive")
        okPositive.groupBy("numberOfElement").count().orderBy("numberOfElement").show(1000, false)
        println("okNegative")
        okNegative.withColumn("numberElement10", (col("numberOfElement")/10).cast("integer"))
          .groupBy("numberElement10")
          .count().orderBy("numberElement10")
          .show(1000, false)
        
    }
    
    def percentiles() = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        import org.apache.spark.sql.functions._
        val segmentDirectory = "all-190427"
        val df: DataFrame = ss.read.parquet(s"$dataDirectory\\csv\\segments\\$segmentDirectory\\result")
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
          .setThreshold(1.019)
        val segmentDetectionBinaryResults: DataFrame = binarizerForSegmentDetection.transform(df)
        val notok = segmentDetectionBinaryResults.filter(!(col(predict) === col(label)))
        val notokPositive =
            segmentDetectionBinaryResults.filter(!(col(predict) === col(label))).filter(col(predict) === 1.0)
        val notokNegative = segmentDetectionBinaryResults.filter(!(col(predict) === col(label))).filter(col(predict)
          === 0.0)
        val ok = segmentDetectionBinaryResults.filter(col(predict) === col(label))
        val okPositive: Dataset[Row] = segmentDetectionBinaryResults.filter(col(predict) === col(label)).filter(col(predict) === 1.0)
        val okNegative = segmentDetectionBinaryResults.filter(col(predict) === col(label)).filter(col(predict) === 0.0)
        val positive: Dataset[Row] = segmentDetectionBinaryResults.filter(col(predict) === 1.0)
        val negative = segmentDetectionBinaryResults.filter(col(predict) === 0.0)
        
        val percentiles: Array[Double] = (0 until 100).map(i => i.toDouble/100).toArray
        val pers: Array[Double] = segmentDetectionBinaryResults.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persNotOk: Array[Double] = notok.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persNotokPositive: Array[Double] = notokPositive.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persNotokNegative: Array[Double] = notokNegative.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persOk: Array[Double] = ok.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persOkPositive: Array[Double] = okPositive.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persOkNegative: Array[Double] = okNegative.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persPositive: Array[Double] = positive.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        val persNegative: Array[Double] = negative.stat.approxQuantile("numberOfElement", percentiles, 0.00001)
        
        val file = new File(s"D:\\ws\\cryptos\\data\\csv\\segments\\$segmentDirectory\\pers.csv")
        val bw = new BufferedWriter(new FileWriter(file))
        val data = Seq(percentiles, pers, persNotOk, persNotokPositive, persNotokNegative, persOk, persOkPositive, persOkNegative, persPositive, persNegative)
        percentiles.indices.map(i => data.map(_.apply(i))).foreach(line => {
            bw.write(line.mkString(","))
            bw.newLine()
        })
        bw.close()
        
        //        bw.write(percentiles.mkString(","))
        //        bw.newLine()
        //        bw.write(pers.mkString(","))
        //        bw.newLine()
        //        bw.write(persNotOk.mkString(","))
        //        bw.newLine()
        //        bw.write(persNotokPositive.mkString(","))
        //        bw.newLine()
        //        bw.write(persNotokNegative.mkString(","))
        //        bw.newLine()
        //        bw.write(persOk.mkString(","))
        //        bw.newLine()
        //        bw.write(persOkPositive.mkString(","))
        //        bw.newLine()
        //        bw.write(persOkNegative.mkString(","))
        //        bw.newLine()
        bw.close()
    }
    
    def main(args: Array[String]): Unit = {
        distribution()
    }
    
}
