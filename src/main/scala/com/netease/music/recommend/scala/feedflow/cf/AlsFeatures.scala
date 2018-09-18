package com.netease.music.recommend.scala.feedflow.cf

import com.netease.music.recommend.scala.feedflow.videoProfile.GetVideoHotScore.getExistsPath
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hzzhangjunfei1 on 2017/12/6.
  */
object AlsFeatures {


  def getQualifiedInput(inputList:List[String], spark:SparkSession):List[String] = {
    var ret = List[String]()
    inputList.foreach{input =>
      val cols = spark.read.parquet(input).columns
      if (cols.contains("playSourceSet"))
        ret = ret :+ input
    }
    ret
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoPrefInput", true, "input directory")
    options.addOption("modelOutput", true, "output directory")
    options.addOption("predictionOutput", true, "output directory")
    options.addOption("needPredictions", true, "need predictions")
    options.addOption("checkpointDir", true, "checkpoint directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoPrefInput = cmd.getOptionValue("videoPrefInput")
    val modelOutput = cmd.getOptionValue("modelOutput")
    val predictionOutput = cmd.getOptionValue("predictionOutput")
    val needPredictions = cmd.getOptionValue("needPredictions").toBoolean
    val checkpointDir = cmd.getOptionValue("checkpointDir")

    spark.sparkContext.setCheckpointDir(checkpointDir)

    import spark.implicits._

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val existsVideoPrefInputPaths = getExistsPath(videoPrefInput, fs)
    val validVideoPrefInputPaths = getQualifiedInput(existsVideoPrefInputPaths, spark)
    logger.warn("qualified videoPrefInput path:\t" + validVideoPrefInputPaths.mkString(","))
    val prefTable = spark.read.parquet(validVideoPrefInputPaths : _*)
      .filter(
        ($"playSourceSet".isNotNull && isInSet("video_classify")($"playSourceSet")) ||
          ($"impressPageSet".isNotNull && isInSet("video_classify")($"impressPageSet"))
      )
      .filter($"vType"==="video")
      .withColumn("pref", getGroupVideoPref($"viewTime", $"playTypes", $"subscribeCnt", $"actionTypeList", $"videoPref"))
      .withColumn("userId", stringToLong($"actionUserId"))
      .select($"userId", $"videoId", $"pref")

    val als = new ALS()
      .setMaxIter(10)
      .setNumUserBlocks(100)
      .setNumItemBlocks(100)
      .setRegParam(0.1)
      .setRank(100)
      .setImplicitPrefs(true)
      .setUserCol("userId")
      .setItemCol("videoId")
      .setRatingCol("pref")

    val alsModel = als.fit(prefTable)
    alsModel.save(modelOutput)

    if (needPredictions) {
      val predictions = alsModel.transform(prefTable)
      predictions.write.parquet(predictionOutput)
    }
//    val itemFactors = alsModel.itemFactors
//    itemFactors.write.parquet(videoFeaturesOutput)
//
//    val userFactors = alsModel.userFactors
//    userFactors.write.parquet(userFeaturesOutput)
  }
}
