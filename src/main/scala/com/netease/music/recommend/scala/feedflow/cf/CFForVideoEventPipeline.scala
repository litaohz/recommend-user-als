package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{IndexToString, MinMaxScaler, StringIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hzzhangjunfei1 on 2017/8/2.
  */
object CFForVideoEventPipeline {

  case class userItemPref(userId:Long, itemId:Long, pref:Float)

  def parseItemPref(line:String): userItemPref = {
    val fields = line.split("\t")
    assert(fields.size >= 3)
    userItemPref(fields(0).toLong, fields(1).toLong, fields(2).toFloat)
  }

  def pref2Vector = udf((pref:Float) => {
    Vectors.dense(pref.toDouble)
  })

  def vector2RoundedPref = udf((vector:Vector) => {
    assert(vector.size == 1)
    Math.round(vector.toArray(0))
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val options = new Options
    options.addOption("userItemProfile", true, "input directory")
    options.addOption("output", true, "output directory")
    options.addOption("predictionOutput", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userItemProfileInput = cmd.getOptionValue("userItemProfile")
    val output = cmd.getOptionValue("output")
    val predictionOutput = cmd.getOptionValue("predictionOutput")

    import spark.implicits._

    logger.info("start to load pref...")
    val originalPrefs = spark.read.textFile(userItemProfileInput)
      .map(parseItemPref)
      .toDF
      .cache

    val qualifiedUsers = originalPrefs
      .groupBy($"userId")
      .agg(count($"userId").as("userCnt"))
      .filter($"userCnt" > 3)
      .select("userId")

    val prefs = originalPrefs
      .join(qualifiedUsers, Seq("userId"), "inner")
      .withColumn("prefVector", pref2Vector($"pref"))
//        .withColumn("ratingFromPref", getRatingFromPref($"pref", $"maxPref"))

    logger.info("pref df schema:\t" + prefs.schema)
    logger.info("first line:\n" + prefs.first)
    logger.info("num of pref lines:\t" + prefs.count)

    val minMaxScaler = new MinMaxScaler()
      .setInputCol("prefVector")
      .setOutputCol("ratingVector")
      .setMax(10)
      .setMin(0)

    val scalerModel = minMaxScaler.fit(prefs)
    val scaledPrefs = scalerModel.transform(prefs)
      .withColumn("rating", vector2RoundedPref($"ratingVector"))

    val itemIdIndexer = new StringIndexer()
      .setInputCol("itemId")
      .setOutputCol("itemIdIdx")
      .fit(scaledPrefs)
    val userIdIndexer = new StringIndexer()
      .setInputCol("userId")
      .setOutputCol("userIdIdx")
      .fit(scaledPrefs)

    val Array(training, test) = scaledPrefs.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(3)
      .setRegParam(0.01)
        .setRank(5)
      .setUserCol("userIdIdx")
      .setItemCol("itemIdIdx")
      .setRatingCol("rating")


    val pipleline = new Pipeline()
      .setStages(Array(itemIdIndexer, userIdIndexer, als))

    val cfModel = pipleline.fit(training)
    // Evaluate the model by computing the RMSE on test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
//    model.setColdStartStrategy("drop")

    logger.info("start to predict...")
    val predictions = cfModel.transform(test)
    predictions.write.parquet(predictionOutput)
    logger.info("predict complete...")
    logger.info("predictions.schema:\t" + predictions.schema)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    predictions.write.parquet(output)
    /*// Generate top 10 movie recommendations for each user
    val userRecs = cfModel.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    val movieRecs = cfModel.recommendForAllItems(10)
    userRecs.printSchema
    println(userRecs.first)
    movieRecs.printSchema
    println(movieRecs.first)*/

  }
}
