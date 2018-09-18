package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{IndexToString, MinMaxScaler, StringIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._

/**
  * Created by hzzhangjunfei1 on 2017/8/3.
  */
object ALSPipeline {

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
    options.addOption("userItemProfile", true, "input directory")
    options.addOption("videoPool", true, "input directory")
    options.addOption("videoFeaturesOutput", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userItemProfileInput = cmd.getOptionValue("userItemProfile")
    val videoPoolInput = cmd.getOptionValue("videoPool")
    val videoFeaturesOutput = cmd.getOptionValue("videoFeaturesOutput")

    import spark.implicits._

    val videoIds = spark.read.textFile(videoPoolInput)
      .map{ line =>
        line.split("\t")(0)
      }
      .toDF("itemId")
      .groupBy("itemId")
      .agg(count($"itemID"))
      .select("itemId")

    val originalPrefs = spark.read.textFile(userItemProfileInput)
      .map(parseItemPref)
      .toDF
      .join(videoIds, Seq("itemId"), "inner")
      .select("userId", "itemId", "pref")

    val qualifiedUsers = originalPrefs
      .filter($"pref">0)
      .groupBy($"userId")
      .agg(count($"itemId").as("itemCnt"))
      .filter($"itemCnt" > 1)
      .select("userId")

    val qualifiedItems = originalPrefs
      .filter($"pref">0)
      .groupBy($"itemId")
      .agg(count($"userId").as("userCnt"))
      .filter($"userCnt" > 1)
      .select("itemId")
    val prefs = originalPrefs
      .join(qualifiedItems, Seq("itemId"), "inner")
      .join(qualifiedUsers, Seq("userId"), "inner")
      .withColumn("prefFinal", getFinalPref($"pref"))
      .filter($"prefFinal"=!=0)
      .cache


    logger.warn(s"\nGot ${prefs.count} ratings from ${prefs.select($"userId").distinct.count} users on ${prefs.select($"itemId").distinct.count} items..")

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

    val alsModel = new ALS()
      .setMaxIter(20)
      .setRegParam(0.1)
      .setRank(200)
      .setImplicitPrefs(true)
      .setUserCol("userIdInt")
      .setItemCol("itemIdInt")
      .setRatingCol("prefFinal")
      .fit(prefs_final)

    val itemIdMapping = prefs_final
      .select("itemIdIdx", "itemId")
      .distinct
    val itemFactors = alsModel.itemFactors
      .join(itemIdMapping, $"id" === $"itemIdIdx", "left_outer")
    itemFactors.write.parquet(videoFeaturesOutput)

  }
}
