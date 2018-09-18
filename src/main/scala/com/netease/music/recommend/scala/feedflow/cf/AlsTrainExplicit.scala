package com.netease.music.recommend.scala.feedflow.cf

import com.netease.music.recommend.scala.feedflow.cf.AlsFeatures.getQualifiedInput
import com.netease.music.recommend.scala.feedflow.videoProfile.GetVideoHotScore.getExistsPath
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by hzzhangjunfei1 on 2017/12/6.
  */
object AlsTrainExplicit {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoPrefInput", true, "input directory")
    options.addOption("needPredictions", true, "need predictions")
    options.addOption("sourceType", true, "all | video_classify | mv")

    options.addOption("modelOutput", true, "output directory")
    options.addOption("predictionOutput", true, "output directory")
    options.addOption("videoFeaturesOutput", true, "output directory")
    options.addOption("userFeaturesOutput", true, "output directory")
    options.addOption("checkpointDir", true, "checkpoint directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoPrefInput = cmd.getOptionValue("videoPrefInput")
    val needPredictions = cmd.getOptionValue("needPredictions").toBoolean
    val sourceType = cmd.getOptionValue("sourceType")

    val modelOutput = cmd.getOptionValue("modelOutput")
    val predictionOutput = cmd.getOptionValue("predictionOutput")
    val videoFeaturesOutput = cmd.getOptionValue("videoFeaturesOutput")
    val userFeaturesOutput = cmd.getOptionValue("userFeaturesOutput")
    val checkpointDir = cmd.getOptionValue("checkpointDir")

    spark.sparkContext.setCheckpointDir(checkpointDir)

    import spark.implicits._

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val existsVideoPrefInputPaths = getExistsPath(videoPrefInput, fs)
    val validVideoPrefInputPaths = getQualifiedInput(existsVideoPrefInputPaths, spark)
    logger.warn("qualified videoPrefInput path:\t" + validVideoPrefInputPaths.mkString(","))
    var prefTable = spark.read.parquet(validVideoPrefInputPaths : _*)
      .filter($"vType"==="video")
      .groupBy($"actionUserId", $"videoId")
      .agg(
        collect_list($"videoPref").as("prefList")
        ,collect_set($"playSourceSet").as("playSourceSumSet")
        ,collect_set($"impressPageSet").as("impressPageSumSet")
      )
      .withColumn("pref", getExplicitVideoPref($"prefList"))
      .withColumn("userId", stringToLong($"actionUserId"))
      .withColumn("playSourceSet", mergeSeq($"playSourceSumSet"))
      .withColumn("impressPageSet", mergeSeq($"impressPageSumSet"))
      .filter($"pref"=!=0)
//      .select($"userId", $"videoId", $"pref")

    if (sourceType.equals("video_classify")) {
      prefTable = prefTable
        .filter(
          ($"playSourceSet".isNotNull && isInSet("video_classify")($"playSourceSet")) ||
            ($"impressPageSet".isNotNull && isInSet("video_classify")($"impressPageSet"))
        )
    }
    prefTable = prefTable
      .select($"userId", $"videoId", $"pref")

    val prefWindow = Window.partitionBy("pref").orderBy("cnt")
    val videoIndex = prefTable
      .groupBy("videoId", "pref")
      .agg(count($"videoId").as("cnt"))
      .withColumn("videoIdInt", row_number().over(prefWindow))
      .drop("pref", "cnt")
      .cache
    val userIndex = prefTable
      .groupBy("userId", "pref")
      .agg(count($"userId").as("cnt"))
      .withColumn("userIdInt", row_number().over(prefWindow))
      .drop("pref", "cnt")
      .cache
    val prefs_final = prefTable
      .join(videoIndex, Seq("videoId"), "left_outer")
      .join(userIndex, Seq("userId"), "left_outer")
    prefs_final.write.mode(SaveMode.Overwrite).parquet("prefsTable")

    val als = new ALS()
      .setMaxIter(10)
      .setNumUserBlocks(200)
      .setNumItemBlocks(50)
      .setRegParam(0.01)
      .setRank(100)
      .setImplicitPrefs(false)
      .setUserCol("userIdInt")
      .setItemCol("videoIdInt")
      .setRatingCol("pref")

    val alsModel = als.fit(prefs_final)
    alsModel.save(modelOutput)

    /* 计算量太大，改用索引
    val userConverter = new IndexToString()
      .setInputCol("userIdx")
      .setOutputCol("userId")
      .setLabels(userIndexer.labels)
    val videoConverter = new IndexToString()
      .setInputCol("id")
      .setOutputCol("videoId")
      .setLabels(videoIndexer.labels)*/

    if (needPredictions) {
      val predictions = alsModel.transform(prefs_final)
      predictions.write.parquet(predictionOutput)
    }
    /*计算量太大，改用索引
    val itemFactors = videoConverter
      .transform(alsModel.itemFactors)*/
    val dis_thred = 0.05
    val itemFactors = alsModel.itemFactors
      .join(videoIndex, $"id"===$"videoIdInt", "left_outer")
      .select("videoId", "features", "id")
      .withColumnRenamed("id", "videoIdInt")
      .withColumnRenamed("videoId", "id")
//      .withColumn("dis", euDistance($"features"))
//      .filter($"dis">dis_thred)
      .select("id", "features", "videoIdInt")
    itemFactors.write.parquet(videoFeaturesOutput)


    val userFactors = alsModel.userFactors
      .join(userIndex, $"id"===$"userIdInt", "left_outer")
      .select("userId", "features", "id")
      .withColumnRenamed("id", "userIdInt")
      .withColumnRenamed("userId", "id")
//      .withColumn("dis", euDistance($"features"))
//      .filter($"dis">dis_thred)
      .select("id", "features", "userIdInt")
    userFactors.write.parquet(userFeaturesOutput)
  }
}
