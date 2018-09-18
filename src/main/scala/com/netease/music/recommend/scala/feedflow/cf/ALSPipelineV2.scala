package com.netease.music.recommend.scala.feedflow.cf

import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hzzhangjunfei1 on 2017/8/3.
  */
object ALSPipelineV2 {

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
    options.addOption("forwardPref", true, "input directory")
    options.addOption("commentPref", true, "input directory")
    options.addOption("playendPref", true, "input directory")
    options.addOption("numFactors", true, "numFactors")
    options.addOption("output", true, "output directory")
    options.addOption("videoFeaturesOutput", true, "output directory")
    options.addOption("userFeaturesOutput", true, "output directory")
    options.addOption("modelOutput", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val forwardPrefInput = cmd.getOptionValue("forwardPref")
    val commentPrefInput = cmd.getOptionValue("commentPref")
    val playendPrefInput = cmd.getOptionValue("playendPref")
    val numFactors = cmd.getOptionValue("numFactors").toInt
    val output = cmd.getOptionValue("output")
    val videoFeaturesOutput = cmd.getOptionValue("videoFeaturesOutput")
    val userFeaturesOutput = cmd.getOptionValue("userFeaturesOutput")
    val modelOutput = cmd.getOptionValue("modelOutput")

    import spark.implicits._

    val forwardPref = spark.read.textFile(forwardPrefInput)
      .map { line =>
        val info = line.split(",")
        userItemPref(info(0).toLong, info(1).toLong, 5.0f)
      }
    val commentPref = spark.read.textFile(commentPrefInput)
      .map { line =>
        val info = line.split(",")
        userItemPref(info(0).toLong, info(1).toLong, 2.0f)
      }
    val playendPref = spark.read.textFile(playendPrefInput)
      .map { line =>
        val info = line.split(",")
        userItemPref(info(0).toLong, info(1).toLong, 3.0f)
      }
    val prefs = forwardPref
          .union(commentPref)
          .groupBy($"userId", $"itemId")
          .agg(sum($"pref").as("final_pref"))
          .select("userId", "itemId", "final_pref")

    //      .union(playendPref)
    logger.warn(s"\nThere are ${forwardPref.count} forward prefs, ${commentPref.count} comment prefs, ${playendPref.count} playend prefs in original pref...\n" +
      s"Got ${prefs.count} ratings from ${prefs.select($"userId").distinct.count} users on ${prefs.select($"itemId").distinct.count} items..")

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
//    val Array(training, test) = prefs_final.randomSplit(Array(0.8, 0.2))
    //    logger.warn(s"training set size: ${training.count}\n" +
    //                s"test set size: ${test.count}")

    val alsModel = new ALS()
      .setMaxIter(20)
      .setRegParam(0.1)
      .setRank(numFactors)
      .setImplicitPrefs(true)
      .setUserCol("userIdInt")
      .setItemCol("itemIdInt")
      .setRatingCol("final_pref")
      .fit(prefs_final)

//    alsModel.write.save(modelOutput)
//
//    val predictions = alsModel.transform(test)
//    val naNCnt = predictions.filter($"prediction".isNaN).count
//    val predictionCnt = predictions.count
//
//    val evaluator = new RegressionEvaluator()
//      .setMetricName("rmse")
//      .setLabelCol("pref")
//      .setPredictionCol("prediction")
//    val rmse = evaluator.evaluate(predictions.filter(!$"prediction".isNaN))
//    logger.warn(s"predictions set size: ${predictionCnt}\nNaN value in predictions set: ${naNCnt}\n" +
//      s"Root-mean-square error = ${rmse}")


    val itemIdMapping = prefs_final
      .select("itemIdIdx", "itemId")
      .distinct
    val itemFactors = alsModel.itemFactors
      .join(itemIdMapping, $"id" === $"itemIdIdx", "left_outer")
    itemFactors.write.parquet(videoFeaturesOutput)
    logger.warn(s"There are ${itemFactors.count} items in itemFactors")

    val userIdIndex = prefs_final
      .select("userIdIdx", "userId")
      .distinct
    val userFactors = alsModel.userFactors
      .join(userIdIndex, $"id" === $"userIdIdx", "left_outer")
    userFactors.write.parquet(userFeaturesOutput)

  }
}
