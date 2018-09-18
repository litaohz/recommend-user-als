package com.netease.music.recommend.scala.feedflow.videoProfile

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetVideoClickrate {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoActionInfo4ClickrateNdays", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoActionInfo4ClickrateNdaysInput = cmd.getOptionValue("videoActionInfo4ClickrateNdays")
    val outputPath = cmd.getOptionValue("output")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    import spark.implicits._

    val existsInputPaths = getExistsPath(videoActionInfo4ClickrateNdaysInput, fs)
    logger.warn("existing input path:\t" + existsInputPaths.mkString(","))
    val videoActionInfo4ClickrateNdaysTable = spark.read.parquet(existsInputPaths.toSeq : _*)
      .cache

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val nowMiliSeconds = System.currentTimeMillis
    val dateYesterday = sdf.format(new Date(nowMiliSeconds - 3600 * 24 * 1000))
    val videoClickrate1day = videoActionInfo4ClickrateNdaysTable
      .filter($"date"===dateYesterday)
      .withColumn("clickrate1day", $"clickrate1d")

    /*var validDates = for (i <- 1 to 3) yield {
      sdf.format(new Date(nowMiliSeconds - i * 3600 * 24 * 1000))
    }
    val videoClickrate3days = videoActionInfo4ClickrateNdaysTable
      .filter($"date".isin(validDates : _*))
      .groupBy("videoId", "vType", "clickrateType")
      .agg(
        sum("impressCnt").as("impressCnt3days"), sum("actionCnt").as("actionCnt3days")
      )
      .withColumn("clickrate3days", getClickrate($"impressCnt3days", $"actionCnt3days"))*/

    var validDates = for (i <- 1 to 7) yield {
      sdf.format(new Date(nowMiliSeconds - i * 3600 * 24 * 1000))
    }
    val videoClickrate7days = videoActionInfo4ClickrateNdaysTable
      .filter($"date".isin(validDates : _*))
      .groupBy("videoId", "vType", "clickrateType")
      .agg(
        sum("impressCnt").as("impressCnt7days"), sum("actionCnt").as("actionCnt7days")
        ,sum("newuserImpressCnt").as("newuserImpressCnt7days"), sum("newuserActionCnt").as("newuserActionCnt7days")
        ,sum("hotImpressCnt").as("hotImpressCnt7days"), sum("hotActionCnt").as("hotActionCnt7days")
        ,sum("correctImpressCnt").as("correctImpressCnt7days"), sum("correctActionCnt").as("correctActionCnt7days"), sum("correctPlayendCnt").as("correctPlayendCnt7days")
        ,sum("correctPlayTime").as("correctPlayTime7days"), collect_list($"maxDuration").as("durationList")
        ,sum("correctShareClickCnt").as("correctShareClickCnt7days")
        ,sum("newuserShareClickCnt").as("newuserShareClickCnt7days")
        ,sum("hotShareClickCnt").as("hotShareClickCnt7days")
        //        ,collect_list($"clickrate1dPositionList").as("clickrate1dPositionList7days")
        //        ,collect_list($"newuserClickrate1dPositionList").as("newuserClickrate1dPositionList7days")
        ,collect_list($"hotClickrate1dPositionList").as("hotClickrate1dPositionList7days")
        ,sum("zanCnt").as("zanCnt7days")
        ,sum("commentCnt").as("commentCnt7days")
        ,sum("subscribeCnt").as("subscribeCnt7days")
        ,sum("correctZanCnt").as("correctZanCnt7days")
        ,sum("correctCommentCnt").as("correctCommentCnt7days")
        ,sum("correctSubscribeCnt").as("correctSubscribeCnt7days")
      )
      .withColumn("duration", getMajorityIntValue($"durationList"))
      .withColumn("clickrate7days", getClickrate($"impressCnt7days", $"actionCnt7days"))
      .withColumn("newuserClickrate7days", getClickrate($"newuserImpressCnt7days", $"newuserActionCnt7days"))
      .withColumn("newuserSharerate7days", getClickrate($"newuserActionCnt7days", $"newuserShareClickCnt7days"))
      .withColumn("hotClickrate7days", getClickrate($"hotImpressCnt7days", $"hotActionCnt7days"))
      .withColumn("hotSharerate7days", getClickrate($"hotActionCnt7days", $"hotShareClickCnt7days"))
      .withColumn("correctClickrate7days", getClickrate($"correctImpressCnt7days", $"correctActionCnt7days"))
      .withColumn("correctPlayendrate7days", getPlayendrate($"correctImpressCnt7days", $"correctPlayendCnt7days"))
      .withColumn("correctPlayendrate7days_new", getPlayendrate($"correctActionCnt7days", $"correctPlayendCnt7days"))
      .withColumn("correctPlayratio7days", getPlayratio($"actionCnt7days", $"correctPlayTime7days", $"duration"))
      .withColumn("correctSharerate7days", getClickrate($"actionCnt7days", $"correctShareClickCnt7days"))
      .withColumn("hotClickrate7daysPositionList", getPositionClickrateList($"hotClickrate1dPositionList7days"))

    validDates = for (i <- 1 to 30) yield {
      sdf.format(new Date(nowMiliSeconds - i * 3600 * 24 * 1000))
    }
    val videoClickrate30days = videoActionInfo4ClickrateNdaysTable
      .filter($"date".isin(validDates : _*))
      .groupBy("videoId", "vType", "clickrateType")
      .agg(
        sum("impressCnt").as("impressCnt30days"), sum("actionCnt").as("actionCnt30days")
        ,sum("newuserImpressCnt").as("newuserImpressCnt30days"), sum("newuserActionCnt").as("newuserActionCnt30days")
        ,sum("hotImpressCnt").as("hotImpressCnt30days"), sum("hotActionCnt").as("hotActionCnt30days")
        ,sum("correctImpressCnt").as("correctImpressCnt30days"), sum("correctActionCnt").as("correctActionCnt30days"), sum("correctPlayendCnt").as("correctPlayendCnt30days")
        ,sum("correctPlayTime").as("correctPlayTime30days"), collect_list($"maxDuration").as("durationList")
        ,sum("correctShareClickCnt").as("correctShareClickCnt30days")
        ,sum("newuserShareClickCnt").as("newuserShareClickCnt30days")
        ,sum("hotShareClickCnt").as("hotShareClickCnt30days")
//        ,collect_list($"clickrate1dPositionList").as("clickrate1dPositionList30days")
//        ,collect_list($"newuserClickrate1dPositionList").as("newuserClickrate1dPositionList30days")
        ,collect_list($"hotClickrate1dPositionList").as("hotClickrate1dPositionList30days")
        ,sum("zanCnt").as("zanCnt30days")
        ,sum("commentCnt").as("commentCnt30days")
        ,sum("subscribeCnt").as("subscribeCnt30days")
        ,sum("correctZanCnt").as("correctZanCnt30days")
        ,sum("correctCommentCnt").as("correctCommentCnt30days")
        ,sum("correctSubscribeCnt").as("correctSubscribeCnt30days")
      )
      .withColumn("duration", getMajorityIntValue($"durationList"))
      .withColumn("clickrate30days", getClickrate($"impressCnt30days", $"actionCnt30days"))
      .withColumn("newuserClickrate30days", getClickrate($"newuserImpressCnt30days", $"newuserActionCnt30days"))
      .withColumn("newuserSharerate30days", getClickrate($"newuserActionCnt30days", $"newuserShareClickCnt30days"))
      .withColumn("hotClickrate30days", getClickrate($"hotImpressCnt30days", $"hotActionCnt30days"))
      .withColumn("hotSharerate30days", getClickrate($"hotActionCnt30days", $"hotShareClickCnt30days"))
      .withColumn("correctClickrate30days", getClickrate($"correctImpressCnt30days", $"correctActionCnt30days"))
      .withColumn("correctPlayendrate30days", getPlayendrate($"correctActionCnt30days", $"correctPlayendCnt30days"))
      .withColumn("correctPlayratio30days", getPlayratio($"actionCnt30days", $"correctPlayTime30days", $"duration"))
      .withColumn("correctSharerate30days", getClickrate($"actionCnt30days", $"correctShareClickCnt30days"))
      .withColumn("hotClickrate30daysPositionList", getPositionClickrateList($"hotClickrate1dPositionList30days"))

    val videoClickrates = videoClickrate1day
      .join(videoClickrate7days, Seq("videoId", "vType", "clickrateType"), "outer")
      .join(videoClickrate30days, Seq("videoId", "vType", "clickrateType"), "outer")
      .select(
        "videoId", "vType", "clickrateType"
        ,"clickrate7days", "clickrate30days"
        ,"newuserClickrate7days", "newuserClickrate30days"
        ,"newuserSharerate7days", "newuserSharerate30days"
        ,"hotClickrate7days", "hotClickrate30days"
        ,"hotSharerate7days", "hotSharerate30days"
        ,"correctClickrate7days", "correctClickrate30days"
        ,"correctPlayendrate7days", "correctPlayendrate7days_new", "correctPlayendrate30days"
        ,"correctPlayratio7days", "correctPlayratio30days"
        ,"correctSharerate7days", "correctSharerate30days"
        ,"hotClickrate7daysPositionList", "hotClickrate30daysPositionList"
        ,"zanCnt7days", "zanCnt30days"
        ,"commentCnt7days", "commentCnt30days"
        ,"subscribeCnt7days", "subscribeCnt30days"
        ,"actionCnt7days", "actionCnt30days"
        ,"correctZanCnt7days", "correctZanCnt30days"
        ,"correctCommentCnt7days", "correctCommentCnt30days"
        ,"correctSubscribeCnt7days", "correctSubscribeCnt30days"
        , "correctActionCnt7days", "correctActionCnt30days"
      )
      .na.fill("0.0000:0:0", Seq(
      "clickrate7days", "clickrate30days"
      ,"newuserClickrate7days", "newuserClickrate30days"
      ,"newuserSharerate7days", "newuserSharerate30days"
      ,"hotClickrate7days", "hotClickrate30days"
      ,"hotSharerate7days", "hotSharerate30days"
      ,"correctClickrate7days", "correctClickrate30days"
      ,"correctPlayendrate7days", "correctPlayendrate30days", "correctPlayendrate7days_new"
      ,"correctPlayratio7days", "correctPlayratio30days"
      ,"correctSharerate7days", "correctSharerate30days"
      ,"zanCnt7days", "zanCnt30days"
      ,"commentCnt7days", "commentCnt30days"
      ,"subscribeCnt7days", "subscribeCnt30days"
      , "actionCnt7days", "actionCnt30days"
      ,"correctZanCnt7days", "correctZanCnt30days"
      ,"correctCommentCnt7days", "correctCommentCnt30days"
      ,"correctSubscribeCnt7days", "correctSubscribeCnt30days"
      , "correctActionCnt7days", "correctActionCnt30days"
                                ))
      .na.fill("0.0000:0:0:0", Seq(
      "hotClickrate7daysPositionList", "hotClickrate30daysPositionList"
                                  )
      )
      .orderBy($"clickrate30days".desc)

    videoClickrates
      .repartition(1)
      .write.parquet(outputPath + "/parquet")
    videoClickrates
      .repartition(1)
      .write.option("sep", "\t").csv(outputPath + "/csv")
  }
}
