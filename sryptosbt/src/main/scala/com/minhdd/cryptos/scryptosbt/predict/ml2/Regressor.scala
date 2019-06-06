package com.minhdd.cryptos.scryptosbt.predict.ml2

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.Timestamp

import com.minhdd.cryptos.scryptosbt.predict.BeforeSplit
import com.minhdd.cryptos.scryptosbt.tools.Timestamps
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ml2.{label, predict, prediction}


case class Rates(truePositive: Double, falsePositive: Double, trueRate: Double, falseNegative: Double)

object Regressor {
    val segmentDirectory = "all-190601-fusion"
    val dataDirectory = "D:\\ws\\cryptos\\data"
    
    def main(args: Array[String]): Unit = {
//        resultss()
//        t
//        predictOneSegment()
    }
    
    def predictOneSegment(): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df: DataFrame = 
            ss.read.parquet(s"$dataDirectory\\csv\\segments\\$segmentDirectory\\beforesplits")
        val someSegments: DataFrame = df.limit(3)
        Predictor.predictTheSegment(ss, s"$dataDirectory\\models\\$segmentDirectory", someSegments)
    }
    
    def getModelFromPath(ss: SparkSession, modelPath: String): CrossValidatorModel = {
        val a: RDD[CrossValidatorModel] = ss.sparkContext.objectFile[CrossValidatorModel](modelPath)
        val model = a.first()
        a.first()
    }
    

    
    def why() = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        import ss.implicits._
        val ds = ss.read.parquet(s"D:\\ws\\cryptos\\data\\csv\\segments\\all-190531\\beforesplits").as[Seq[BeforeSplit]]
        val f: Dataset[Seq[BeforeSplit]] = ds.filter(s => s.exists(_.secondDerive.isEmpty))
        f.map(s => s.map(b => b.copy(datetime = Timestamps(b.datetime.getTime *1000).timestamp ))).show(false)
    }
    
    def t() = {
        import com.minhdd.cryptos.scryptosbt.predict.ml2.ml2._
        import org.apache.spark.ml.Pipeline
        import org.apache.spark.ml.evaluation.RegressionEvaluator
        import org.apache.spark.ml.feature.Binarizer
        import org.apache.spark.ml.regression.GBTRegressor
        import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
        import org.apache.spark.sql.{DataFrame, SparkSession}
        
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df: DataFrame = ss.read.parquet(s"D:\\ws\\cryptos\\data\\csv\\segments\\$segmentDirectory\\beforesplits")
        val Array(trainDF, testDF) = df.randomSplit(Array(0.7, 0.3), seed=42)
        val gbt = new GBTRegressor()
        gbt.setSeed(273).setMaxIter(5)
        val expanser = new ExpansionSegmentsTransformer(ss, 
            ss.read.parquet("D:\\ws\\cryptos\\data\\csv\\segments\\all-190418\\expansion").schema)
        val pipeline = new Pipeline().setStages(Array(expanser, indexerBegin, vectorAssembler, gbt))
        val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(5, 10, 20, 50, 100)).build()
        val evaluator = new RegressionEvaluator().setLabelCol(label).setPredictionCol(prediction)
        val cv = new CrossValidator()
          .setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid)
          .setNumFolds(3).setSeed(27)
        val model: CrossValidatorModel = cv.fit(trainDF)
        ss.sparkContext.parallelize(Seq(model), 1).saveAsObjectFile(s"D:\\ws\\cryptos\\data\\models\\$segmentDirectory")
        val result = model.transform(testDF)
        result.show(false)
        result.write.parquet(s"D:\\ws\\cryptos\\data\\csv\\segments\\$segmentDirectory\\result")

        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
    
        for (i <- 0 to 10) {
            println("threshold : " + i.toDouble/10)
            binarizerForSegmentDetection.setThreshold(i.toDouble/10)
            val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(result)
            val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
            counts.show()
        } 
    }
    
    def resultss(): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        val df: DataFrame = ss.read.parquet(s"D:\\ws\\cryptos\\data\\csv\\segments\\$segmentDirectory\\result")
        
        for (i <- 0 to 10) {
            val binarizerForSegmentDetection = new Binarizer()
              .setInputCol(prediction)
              .setOutputCol(predict)
            println("threshold : " + i.toDouble/10)
            binarizerForSegmentDetection.setThreshold(i.toDouble/10)
            val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(df)
            val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
            counts.show()
        }
        
        val t: (Double, Rates) = getThreshold(ss, df)
        println(t)
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
        binarizerForSegmentDetection.setThreshold(t._1)
        val segmentDetectionBinaryResults = binarizerForSegmentDetection.transform(df)
        val counts = segmentDetectionBinaryResults.groupBy(label, predict).count()
        counts.show()
    }
    
    def distribution(): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        import org.apache.spark.sql.functions._
        val segmentDirectory = "all-190427"
        val df: DataFrame = ss.read.parquet(s"D:\\ws\\cryptos\\data\\csv\\segments\\$segmentDirectory\\result")
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
        val df: DataFrame = ss.read.parquet(s"D:\\ws\\cryptos\\data\\csv\\segments\\$segmentDirectory\\result")
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
    

    
    def results(): Unit = {
        val ss: SparkSession = SparkSession.builder().appName("ml").master("local[*]").getOrCreate()
        ss.sparkContext.setLogLevel("ERROR")
        import org.apache.spark.sql.functions._
        val df = ss.read.parquet(s"D:\\ws\\cryptos\\data\\csv\\segments\\$segmentDirectory\\result")
//        df.select("label", "prediction").show(false)
//        println("-----min-----max------")
//        df.agg(min("prediction"), max("prediction")).show(false)
//        println("-----stats------")
//        val stats: Array[Double] = df.stat.approxQuantile("prediction", Array(0.1D, 0.2D, 0.3D, 0.4D, 0.5D, 0.6D, 0.7D, 0.8D, 0.9D), 0.00001)
//        stats.foreach(println)
        println("-----binarizers------")
        for (i <- Seq(1.01897137, 1.01897139, 1.0189714, 1.02, 1.021)) {
            val counts: DataFrame = getCountsDf(df, i)
            val truePositif: Long = extractThridValueWithTwoFilter(counts, 1, 1.0)
            val falsePositif: Long = extractThridValueWithTwoFilter(counts, 0, 1.0)
            val falseNegative: Long = extractThridValueWithTwoFilter(counts, 1, 0.0)
            val trueNegative: Long = extractThridValueWithTwoFilter(counts, 0, 0.0)
            counts.show()
            val total = truePositif + falsePositif + falseNegative + trueNegative
            val rate1 = truePositif.toDouble / (truePositif + falsePositif)
            println("rate true positif : " + rate1)
            val rate2 = trueNegative.toDouble / (falseNegative + trueNegative)
            println("rate trueNegative : " + rate2)
            println("rate potisif : " + (truePositif + falsePositif).toDouble/total)
            println("rate true :" + (truePositif + trueNegative).toDouble/total)
            println(i)
            println("----------------------------------------------------------------")
        }
    }
    
    
    private def getCountsDf(df: DataFrame, threshold: Double): DataFrame = {
        val binarizerForSegmentDetection = new Binarizer()
          .setInputCol(prediction)
          .setOutputCol(predict)
          .setThreshold(threshold)
        val segmentDetectionBinaryResults: DataFrame = binarizerForSegmentDetection.transform(df)
        val counts: DataFrame = segmentDetectionBinaryResults.groupBy(label, predict).count()
        counts
    }
    
    private def extractThridValueWithTwoFilter(counts: DataFrame, labelValue: Int, predictValue: Double): Long = {
        import org.apache.spark.sql.functions._
        val row = counts.filter(col("label") === labelValue).filter(col("predict") === predictValue)
        
        if (row.count() == 1) {
            row.first().getAs[Long](2)
        } else {
            0
        }
    }
    
    private def getRates(s: Seq[Double], df: DataFrame) = {
        s.map(t=>{
            val counts: DataFrame = getCountsDf(df, t)
            val truePositive: Long = extractThridValueWithTwoFilter(counts, 1, 1.0)
            val falsePositive: Long = extractThridValueWithTwoFilter(counts, 0, 1.0)
            val falseNegative: Long = extractThridValueWithTwoFilter(counts, 1, 0.0)
            val trueNegative: Long = extractThridValueWithTwoFilter(counts, 0, 0.0)
            val total = truePositive + falsePositive + falseNegative + trueNegative
            val rate1 = truePositive.toDouble / (truePositive + falsePositive)
            val rate2 = trueNegative.toDouble / (falseNegative + trueNegative)
            val rate3 = (truePositive + falsePositive).toDouble/total
            val rate4 = (truePositive + trueNegative).toDouble/total
            (t, Rates(rate1, rate2, rate3, rate4))
        })
    }
    
    def getThreshold(ss: SparkSession, df: DataFrame, minimumTruePositiveRate: Double = 0.82): (Double, Rates) = {
        val centeredThreshold = getCenteredThreshold(ss, df)
        getAdjustedThreshold(ss, df, centeredThreshold._1, minimumTruePositiveRate)
    }
    
    private def getCenteredThreshold(ss: SparkSession, df: DataFrame, minimumTruePositiveRate: Double = 0.82): (Double, 
      Rates) = {
        import ss.implicits._
        import org.apache.spark.sql.functions._
        val minmaxDf: DataFrame = df.agg(min(prediction), max(prediction))
        val minValue: Double = minmaxDf.map(_.getDouble(0)).first()
        val maxValue: Double = minmaxDf.map(_.getDouble(1)).first()
        val diff: Double = maxValue - minValue
        val samplingThresholds: Seq[Int] = 0 until 40
        val thresholds: Seq[Double] = 
            samplingThresholds.map(s => minValue + (s * (diff/(samplingThresholds.length - 1))))
        
        val rates = getRates(thresholds, df)
        
        bestRate(minimumTruePositiveRate, rates)
    }
    
    private def bestRate(minimumTruePositiveRate: Double, rates: Seq[(Double, Rates)]): (Double, Rates) = {
        rates
          .filter(_._2.truePositive > minimumTruePositiveRate)
          .maxBy(_._2.trueRate)
    }
    
    def getAdjustedThreshold(ss: SparkSession, df: DataFrame, centeredThreshold: Double,
                             minimumTruePositiveRate: Double): (Double, Rates) = {
        val epsilon = 0.0005
        val thresholds = (-60 until 60).map(e => centeredThreshold + e*epsilon)
        val rates = getRates(thresholds, df)
        rates.foreach(println)
        println("----------")
        bestRate(minimumTruePositiveRate, rates)
    }
}
