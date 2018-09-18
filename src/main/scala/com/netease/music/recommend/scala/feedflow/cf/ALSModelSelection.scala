package com.netease.music.recommend.scala.feedflow.cf

import com.netease.music.recommend.scala.feedflow.cf.ALSPipelineV2.userItemPref
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hzzhangjunfei1 on 2017/8/8.
  */
object ALSModelSelection {

  case class userItemPref(userId:Long, itemId:Long, pref:Float)

  def parseItemPref(line:String): userItemPref = {
    val fields = line.split("\t")
    assert(fields.size >= 3)
    userItemPref(fields(0).toLong, fields(1).toLong, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
//    options.addOption("userItemProfile", true, "input directory")
    options.addOption("forwardPref", true, "input directory")
    options.addOption("output", true, "output directory")
//    options.addOption("videoFeaturesOutput", true, "output directory")
//    options.addOption("userFeaturesOutput", true, "output directory")
    options.addOption("modelOutput", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

//    val userItemProfileInput = cmd.getOptionValue("userItemProfile")
    val forwardPrefInput = cmd.getOptionValue("forwardPref")
    val output = cmd.getOptionValue("output")
//    val videoFeaturesOutput = cmd.getOptionValue("videoFeaturesOutput")
//    val userFeaturesOutput = cmd.getOptionValue("userFeaturesOutput")
    val modelOutput = cmd.getOptionValue("modelOutput")

    import spark.implicits._

    logger.warn("start to load pref...")
//    val originalPrefs = spark.read.textFile(userItemProfileInput)
//      .map(parseItemPref)
//      .toDF
//    val qualifiedUsers = originalPrefs
//      .groupBy($"userId")
//      .agg(count($"userId").as("userCnt"))
//      .filter($"userCnt" > 3)
//      .select("userId")
//
//    val prefs = originalPrefs
//      .join(qualifiedUsers, Seq("userId"), "inner")
//      .cache

    val forwardPref = spark.read.textFile(forwardPrefInput)
      .map { line =>
        val info = line.split(",")
        userItemPref(info(0).toLong, info(1).toLong, 5.0f)
      }
    val prefs = forwardPref
      .cache
    logger.warn("pref df first line:\n" + prefs.first)
    logger.warn("Got " + prefs.count + " ratings from " + prefs.select($"userId").distinct.count + " users on " + prefs.select($"itemId").distinct.count)

    val itemIdIndexer = new StringIndexer()
      .setInputCol("itemId")
      .setOutputCol("itemIdIdx")
      .fit(prefs)
    val userIdIndexer = new StringIndexer()
      .setInputCol("userId")
      .setOutputCol("userIdIdx")
      .fit(prefs)
    val prefs_stage1 = itemIdIndexer.transform(prefs)
    val prefs_stage2 = userIdIndexer.transform(prefs_stage1)
    val prefs_final = prefs_stage2
      .withColumn("userIdInt", string2int($"userIdIdx"))
      .withColumn("itemIdInt", string2int($"itemIdIdx"))
    logger.warn("prefs_final df first line:\n" + prefs_final.first)
    logger.warn("prefs_final: Got " + prefs_final.count + " ratings from " + prefs_final.select($"userId").distinct.count + " users on " + prefs_final.select($"itemId").distinct.count)
    val Array(training, test) = prefs_final.randomSplit(Array(0.8, 0.2))
    logger.warn("training set size:\t" + training.count)
    logger.warn("test set size:\t" + test.count)

    val ranks = List(10, 14, 18)
    val lambdas = List(0.01, 0.1)
    val numMaxIters = List(15, 20)

    var bestModel:Option[ALSModel] = None
    var bestTestRmse = Double.MaxValue
    var bestRank = 0
    var bestlambda = -1.0
    var bestMaxIter = 1
    for (rank <- ranks; lambda <- lambdas; maxIter <- numMaxIters) {
      val alsModel = new ALS()
        .setMaxIter(maxIter)
        .setRegParam(lambda)
        .setRank(rank)
        .setImplicitPrefs(true)
        .setUserCol("userIdInt")
        .setItemCol("itemIdInt")
        .setRatingCol("pref")
        .fit(training)

      val predictions = alsModel.transform(test)
      val naNCnt = predictions.filter($"prediction".isNaN).count
      val predictionCnt = predictions.count

      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("pref")
        .setPredictionCol("prediction")
      val testRmse = evaluator.evaluate(predictions.filter(!$"prediction".isNaN))
      logger.warn("\npredictions set size:\t" + predictionCnt + "\n" + "NaN value in predictions set:\t" + naNCnt + s"\ntestRmse=$testRmse; rank=$rank; lambda=$lambda; maxIter=$maxIter")

      if (testRmse < bestTestRmse) {
        bestModel = Some(alsModel)
        bestTestRmse = testRmse
        bestRank = rank
        bestlambda = lambda
        bestMaxIter = maxIter
      }
    }

    logger.warn(s"bestTestRmse=$bestTestRmse; bestRank=$bestRank; bestlambda=$bestlambda; bestMaxIter=$bestMaxIter")

    val alsModel = bestModel.get
    val finalVideoRating = alsModel.transform(prefs_final)
    finalVideoRating.write.parquet(output)
    alsModel.save(modelOutput)
//    val itemIdMapping = prefs_final
//      .select("itemIdIdx", "itemId")
//      .distinct
//    val itemFactors = alsModel.itemFactors
//      .join(itemIdMapping, $"id" === $"itemIdIdx", "left_outer")
//    itemFactors.write.parquet(videoFeaturesOutput)
//
//    val userIdMapping = prefs_final
//      .select("userIdIdx", "userId")
//      .distinct
//    val userFactors = alsModel.userFactors
//      .join(userIdMapping, $"id" === $"userIdIdx", "left_outer")
//    userFactors.write.parquet(userFeaturesOutput)

  }
}
