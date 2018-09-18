package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hzzhangjunfei1 on 2017/8/3.
  */
object ALSRecResult {

  case class userItemPref(userId:Long, itemId:Long, pref:Float)

  def parseItemPref(line:String): userItemPref = {
    val fields = line.split("\t")
    assert(fields.size >= 3)
    userItemPref(fields(0).toLong, fields(1).toLong, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("alsModel", true, "input directory")
    options.addOption("videoFeaturesOutput", true, "output directory")
    options.addOption("userFeaturesOutput", true, "output directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val alsModelInput = cmd.getOptionValue("alsModel")
    val videoFeaturesOutput = cmd.getOptionValue("videoFeaturesOutput")
    val userFeaturesOutput = cmd.getOptionValue("userFeaturesOutput")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

//    logger.info("start to load pref...")
//    val originalPrefs = spark.read.textFile(userItemProfileInput)
//      .map(parseItemPref)
//      .toDF
//      .cache
//
//    val qualifiedUsers = originalPrefs
//      .groupBy($"userId")
//      .agg(count($"userId").as("userCnt"))
//      .filter($"userCnt" > 3)
//      .select("userId")
//
//    val prefs = originalPrefs
//      .join(qualifiedUsers, Seq("userId"), "inner")
//
//    logger.info("pref df schema:\t" + prefs.schema)
//    logger.info("first line:\n" + prefs.first)
//    logger.info("Got " + prefs.count + " ratings from " + prefs.select($"userId").distinct + " users on " + prefs.select($"itemId").distinct)
//
//    val itemIdIndexer = new StringIndexer()
//      .setInputCol("itemId")
//      .setOutputCol("itemIdIdx")
//      .fit(prefs)
//    val userIdIndexer = new StringIndexer()
//      .setInputCol("userId")
//      .setOutputCol("userIdIdx")
//      .fit(prefs)
//
//    val Array(training, test) = prefs.randomSplit(Array(0.8, 0.2))
//    logger.info("training set size:\t" + training.count)
//    logger.info("test set size:\t" + test.count)
//
//    val als = new ALS()
//      .setMaxIter(3)
//      .setRegParam(0.01)
////      .setRank(3)
//      .setUserCol("userIdIdx")
//      .setItemCol("itemIdIdx")
//      .setRatingCol("pref")
//
//
//    val pipleline = new Pipeline()
//      .setStages(Array(itemIdIndexer, userIdIndexer, als))
//
//    val model = pipleline.fit(training)
//    // Evaluate the model by computing the RMSE on test data
//    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
////    model.setColdStartStrategy("drop")
//
//    logger.info("start to predict...")
//    val predictions = model.transform(test)
//    logger.info("predict complete...")
//    logger.info("predictions.schema:\t" + predictions.schema)
//    logger.info("predictions set size:\t" + predictions.count)
//
////    val evaluator = new RegressionEvaluator()
////      .setMetricName("rmse")
////      .setLabelCol("pref")
////      .setPredictionCol("prediction")
////    val rmse = evaluator.evaluate(predictions)
////    println(s"Root-mean-square error = $rmse")
//
//    predictions.write.parquet(output)
//
//
//    val alsModel = model.asInstanceOf[MatrixFactorizationModel]
//    val videoFeatures = alsModel.productFeatures
//    videoFeatures.saveAsTextFile(videoFeaturesOutput)
//    val userFeatures = alsModel.userFeatures
//    userFeatures.saveAsTextFile(userFeaturesOutput)

  }
}
