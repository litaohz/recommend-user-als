package com.netease.music.recommend.scala.feedflow.videoProfile

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object GetVideoActionInfo4Clickrate {

  case class NewUserId(newUserId:Long)
  case class Video(videoId:Long, vType:String)
  case class Impress(videoId:Long, vType:String, actionUserId:Long, page:String, os:String, alg:String, position:Int)
  case class PlayAction(videoId:Long, vType:String, actionUserId:Long, time:Long, source:String, isNewversion:String, os:String, endType:String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoResolution", true, "input directory")
    options.addOption("impress", true, "log input directory")
    options.addOption("correct_impress", true, "log input directory")
    options.addOption("playend", true, "log input directory")
    options.addOption("click", true, "log input directory")
    options.addOption("zan", true, "log input directory")
    options.addOption("comment", true, "log input directory")
    options.addOption("commentreply", true, "log input directory")
    options.addOption("subscribemv", true, "log input directory")
    options.addOption("subscribevideo", true, "log input directory")
    options.addOption("newUser", true, "input directory")
    options.addOption("dataDate", true, "dataDate")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoResolutionInput = cmd.getOptionValue("videoResolution")
    val impressInput = cmd.getOptionValue("impress")
    val correct_impressInput = cmd.getOptionValue("correct_impress")
    val playendInput = cmd.getOptionValue("playend")
    val clickInput = cmd.getOptionValue("click")
    val zanInput = cmd.getOptionValue("zan")
    val commentInput = cmd.getOptionValue("comment")
    val commentreplyInput = cmd.getOptionValue("commentreply")
    val subscribemvInput = cmd.getOptionValue("subscribemv")
    val subscribevideoInput = cmd.getOptionValue("subscribevideo")
    val newUserInput = cmd.getOptionValue("newUser")
    val dataDate = cmd.getOptionValue("dataDate")
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    // 视频清晰度表Music_VideoResolution
    val videoResolutionTable = spark.sparkContext.textFile(videoResolutionInput)
      .map{line =>
        val info = line.split("\01")
        val videoId = info(1).toLong
        val width = info(8).toInt
        val height = info(9).toInt
        val duration = info(11).toInt / 1000
        val resolution = info(12).toInt   // 分辨率（1-标清，2-高清，3-超清，4-1080P）
      val resolutionInfo = resolution + ":" + duration + ":" + width + ":" + height
        VideoResolution(videoId, resolution, duration, width, height, resolutionInfo)
      }.toDF
      .groupBy($"videoId")
      .agg(
        max($"duration").as("maxDuration")
      )
      .withColumn("vType", lit("video"))

    val newUserTable = spark.sparkContext.textFile(newUserInput)
      .map{line =>
        val newUserId = line.toLong
        NewUserId(newUserId)
      }
      .toDF
      .groupBy("newUserId")
      .agg(count("newUserId").as("group_cnt"))
      .drop($"group_cnt")

    val uselessAlgSet = Seq("firstpage_force_rcmd")
    val impressTable = spark.sparkContext.textFile(impressInput)
      .map {line =>
        val info = line.split("\t")
        val page = info(1)
        val os = info(2)
        val actionUserId = info(3).toLong
        val videoId = info(5).toLong
        val vType = info(6)
        var position = -1
        try {
          position = info(7).toInt
        } catch {
          case ex:Exception => println("log error:" + line)
        }
        val alg = info(8)
        Impress(videoId, getVType(vType), actionUserId, page, os, alg, position)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"page".isin(usefulPage : _*))
      .filter($"vType".isin(usefulVType : _*))
      .filter(!$"alg".isin(uselessAlgSet : _*))
      .withColumn("clickrateType", getClickratetypeFromPage($"page"))
      .groupBy("videoId", "vType", "clickrateType", "actionUserId", "alg", "position")
      .agg(count($"videoId").as("group_cnt"))
      .drop($"group_cnt")
    // 获取是否新用户
    val impressTableWithUserInfo = impressTable
      .join(newUserTable, $"actionUserId"===$"newUserId", "left_outer")
      .na.fill(0, Seq("newUserId"))
      .withColumn("isNewUser", isNewUser($"newUserId"))
      .drop("newUserId")

    // 修正impress后的曝光日志
    val correct_impressTable = spark.sparkContext.textFile(correct_impressInput)
      .map {line =>
        val info = line.split("\t")
        val page = info(1)
        val os = info(2)
        val actionUserId = info(3).toLong
        val videoId = info(5).toLong
        val vType = info(6)
        var position = -1
        try {
          position = info(7).toInt
        } catch {
          case ex:Exception => println("log error:" + line)
        }
//        val position = info(7).toInt
        val alg = info(8)
        Impress(videoId, getVType(vType), actionUserId, page, os, alg, position)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"page".isin(usefulPage : _*))
      .filter($"vType".isin(usefulVType : _*))
      .filter(!$"alg".isin(uselessAlgSet : _*))
      .filter(!$"alg".contains("hot"))        // 过滤掉热门推荐
      .withColumn("clickrateType", getClickratetypeFromPage($"page"))
      .groupBy("videoId", "vType", "clickrateType", "actionUserId", "alg")
      .agg(count($"videoId").as("group_cnt"))
      .drop($"group_cnt")

    val playTable = spark.sparkContext.textFile(playendInput)
      .map {line =>
        val info = line.split("\t")
        val os = info(2)
        val actionUserId = info(3).toLong
        val videoId = info(5).toLong
        val vType = info(6)
        val source = info(8)
        val isNewversion = info(11)
        var time = 0L
        try {
          time = info(15).toLong
        } catch {
          case ex :NumberFormatException => time = 0
        }
        val endType = info(16)
        PlayAction(videoId, getVType(vType), actionUserId, time, source, isNewversion, os, endType)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"videoId" > 100 && $"time">0)
      .filter($"source".isin(usefulSource : _*))
      .filter($"vType".isin(usefulVType : _*))
      .filter($"isNewversion"===1)
      .withColumn("clickrateType", getClickratetypeFromSource($"source"))
      .withColumn("playend", isPlayend($"endType"))
      .join(videoResolutionTable, Seq("videoId", "vType"), "left_outer")
      .na.fill(0, Seq("maxDuration"))
      .withColumn("correctTime", getCorrectPlaytime($"time", $"maxDuration"))
      .groupBy("videoId", "vType", "clickrateType", "actionUserId")
      .agg(
        sum($"playend").as("playendCnt")
        ,sum($"correctTime").as("correctPlayTimePerUser")
        ,collect_list($"maxDuration").as("duration_list")
      )
      .withColumn("duration", getMajorityIntValue($"duration_list"))
      .withColumn("isPlayend", getIsPlayend($"playendCnt"))
      .withColumn("isPlayed", lit(1))
      .withColumn("correctPlayTimePerUser_adjust", smoothPlaytimePerUser($"correctPlayTimePerUser", $"duration"))
      .drop("playendCnt", "duration_list", "correctPlayTimePerUser", "duration")

    val shareTable = spark.sparkContext.textFile(clickInput)
      .filter{line =>   // 仅保留share click
        val info = line.split("\t")
        if (info.length < 10)
          false
        else {
          val actionUserId = info(3).toLong
          val videoId = info(5).toLong
          val vType = info(6)
          val actionType = info(8)
          if (actionUserId != 0
            && videoId > 100
            && actionType.equals("share")
            && usefulVType.contains(vType)
          )
            true
          else
            false
        }
      }
      .map {line =>
        val info = line.split("\t")
        val os = info(2)
        val actionUserId = info(3).toLong
        val videoId = info(5).toLong
        val vType = info(6)
        var position = -1
        try {
          position = info(7).toInt
        } catch {
          case ex:Exception => println("log error:" + line)
        }
//        val position = info(7).toInt
        val shareType = if (info.length >= 11) info(10) else "default"
        ShareAction(videoId, getVType(vType), actionUserId, position, shareType, os)
      }.toDF
      .groupBy("videoId", "vType", "actionUserId")
      .agg(count($"videoId").as("group_cnt"))
      .drop($"group_cnt")
      .withColumn("isShareClicked", setColumnValue(1)())

    val rawZanTable = spark.sparkContext.textFile(zanInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(1)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        (videoId, getVType(vType), actionUserId)
      }.toDF("videoId", "vType", "actionUserId")
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .cache
    val zanTable = rawZanTable
      .groupBy("videoId", "vType")
      .agg(countDistinct("actionUserId").as("zanCnt"))

    val rawCommentTable = spark.sparkContext.textFile(commentInput+","+commentreplyInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(1)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        (videoId, getVType(vType), actionUserId)
      }.toDF.toDF("videoId", "vType", "actionUserId")
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .cache
    val commentTable = rawCommentTable
      .groupBy("videoId", "vType")
      .agg(countDistinct("actionUserId").as("commentCnt"))

    val rawSubscribeTable = spark.sparkContext.textFile(subscribemvInput+","+subscribevideoInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(1)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        (videoId, getVType(vType), actionUserId)
      }.toDF.toDF("videoId", "vType", "actionUserId")
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .cache
    val subscribeTable = rawSubscribeTable
      .groupBy("videoId", "vType")
      .agg(count("actionUserId").as("subscribeCnt"))



    // 未修正无效曝光的曝光日志
    val rawJoinedTable = impressTableWithUserInfo
      .withColumn("isHot", isHot($"alg"))
      .join(playTable, Seq("videoId", "vType", "clickrateType", "actionUserId"), "left_outer")
      .na.fill(0, Seq("isPlayed", "isPlayend"))
      .join(shareTable, Seq("videoId", "vType", "actionUserId"), "left_outer")
      .na.fill(0, Seq("isShareClicked"))
    // 计算位置点击率
    val positionClickrateTable = rawJoinedTable
      .filter($"position"<=40)
      .groupBy("videoId", "vType", "clickrateType", "position")
      .agg(
        count("actionUserId").as("impressCnt")
        ,sum(($"isPlayed">0).cast(IntegerType)).as("actionCnt")

        ,sum(($"isNewUser"===1).cast(IntegerType)).as("newuserImpressCnt")
        ,sum(($"isNewUser"===1 && $"isPlayed">0).cast(IntegerType)).as("newuserActionCnt")
        //        ,sum(($"isNewUser"===1 && $"isPlayend">0).cast(IntegerType)).as("newuserPlayendCnt")
        //        ,sum(($"isNewUser"===1 && $"isShareClicked">0).cast(IntegerType)).as("newuserShareClickCnt")

        ,sum(($"isHot"===1).cast(IntegerType)).as("hotImpressCnt")
        ,sum(($"isHot"===1 && $"isPlayed">0).cast(IntegerType)).as("hotActionCnt")
        //        ,sum(($"isHot"===1 && $"isPlayend">0).cast(IntegerType)).as("hotPlayendCnt")
        //        ,sum(($"isHot"===1 && $"isShareClicked">0).cast(IntegerType)).as("hotShareClickCnt")
      )
      .withColumn("clickrate1d", getPositionClickrate($"impressCnt", $"actionCnt", $"position"))
      // newuser
      .withColumn("newuserClickrate1d", getPositionClickrate($"newuserImpressCnt", $"newuserActionCnt", $"position"))
      //      .withColumn("newuserPlayendrate1d", getPositionClickrate($"newuserActionCnt", $"newuserPlayendCnt"))
      //      .withColumn("newuserSharerate1d", getPositionClickrate($"newuserActionCnt", $"newuserShareClickCnt"))
      // hot
      .withColumn("hotClickrate1d", getPositionClickrate($"hotImpressCnt", $"hotActionCnt", $"position"))
      //      .withColumn("hotPlayendrate1d", getPositionClickrate($"hotActionCnt", $"hotPlayendCnt"))
      //      .withColumn("hotSharerate1d", getPositionClickrate($"hotActionCnt", $"hotShareClickCnt"))
      .groupBy("videoId", "vType", "clickrateType")
      .agg(
        collect_list("clickrate1d").as("clickrate1dPositionList")
        ,collect_list("newuserClickrate1d").as("newuserClickrate1dPositionList")
        ,collect_list("hotClickrate1d").as("hotClickrate1dPositionList")
      )
    //      positionClickrateTable.write.parquet(outputPath)

    // 计算点击率、新用户点击率、热门点击率
    val joinedTable = rawJoinedTable
      .groupBy("videoId", "vType", "clickrateType")
      .agg(
        count("actionUserId").as("impressCnt")
        ,sum(($"isPlayed">0).cast(IntegerType)).as("actionCnt")

        ,sum(($"isNewUser"===1).cast(IntegerType)).as("newuserImpressCnt")
        ,sum(($"isNewUser"===1 && $"isPlayed">0).cast(IntegerType)).as("newuserActionCnt")
        ,sum(($"isNewUser"===1 && $"isPlayend">0).cast(IntegerType)).as("newuserPlayendCnt")
        ,sum(($"isNewUser"===1 && $"isShareClicked">0).cast(IntegerType)).as("newuserShareClickCnt")

        ,sum(($"isHot"===1).cast(IntegerType)).as("hotImpressCnt")
        ,sum(($"isHot"===1 && $"isPlayed">0).cast(IntegerType)).as("hotActionCnt")
        ,sum(($"isHot"===1 && $"isPlayend">0).cast(IntegerType)).as("hotPlayendCnt")
        ,sum(($"isHot"===1 && $"isShareClicked">0).cast(IntegerType)).as("hotShareClickCnt")
      )
      .withColumn("clickrate1d", getClickrate($"impressCnt", $"actionCnt"))
      // newuser
      .withColumn("newuserClickrate1d", getClickrate($"newuserImpressCnt", $"newuserActionCnt"))
      .withColumn("newuserPlayendrate1d", getPlayendrate($"newuserActionCnt", $"newuserPlayendCnt"))
      .withColumn("newuserSharerate1d", getClickrate($"newuserActionCnt", $"newuserShareClickCnt"))
      // hot
      .withColumn("hotClickrate1d", getClickrate($"hotImpressCnt", $"hotActionCnt"))
      .withColumn("hotPlayendrate1d", getPlayendrate($"hotActionCnt", $"hotPlayendCnt"))
      .withColumn("hotSharerate1d", getClickrate($"hotActionCnt", $"hotShareClickCnt"))

    // 修正曝光后的joinTable
    val joinedCorrectTable = correct_impressTable
      .join(playTable, Seq("videoId", "vType", "clickrateType", "actionUserId"), "left_outer")
      .na.fill(0, Seq("isPlayed", "isPlayend"))
      .join(shareTable, Seq("videoId", "vType", "actionUserId"), "left_outer")
      .na.fill(0, Seq("isShareClicked"))
      .join(rawZanTable.withColumn("isZan", lit(1)), Seq("videoId", "vType", "actionUserId"), "left_outer")
      .na.fill(0, Seq("isZan"))
      .join(rawCommentTable.withColumn("isComment", lit(1)), Seq("videoId", "vType", "actionUserId"), "left_outer")
      .na.fill(0, Seq("isComment"))
      .join(rawSubscribeTable.withColumn("isSubscribe", lit(1)), Seq("videoId", "vType", "actionUserId"), "left_outer")
      .na.fill(0, Seq("isSubscribe"))
      .groupBy("videoId", "vType", "clickrateType")
      .agg(
        count("actionUserId").as("correctImpressCnt")
        ,sum(($"isPlayed">0).cast(IntegerType)).as("correctActionCnt")
        ,sum(($"isPlayed">0).cast(IntegerType) * $"correctPlayTimePerUser_adjust").as("correctPlayTime")
        ,sum(($"isPlayend">0).cast(IntegerType)).as("correctPlayendCnt")
        ,sum(($"isShareClicked">0).cast(IntegerType)).as("correctShareClickCnt")
        ,sum(($"isZan">0).cast(IntegerType)).as("correctZanCnt")
        ,sum(($"isComment">0).cast(IntegerType)).as("correctCommentCnt")
        ,sum(($"isSubscribe">0).cast(IntegerType)).as("correctSubscribeCnt")
      )
      .withColumn("correctClickrate1d", getClickrate($"correctImpressCnt", $"correctActionCnt"))
      .withColumn("correctPlayendrate1d", getPlayendrate($"correctActionCnt", $"correctPlayendCnt"))
      .withColumn("correctSharerate1d", getClickrate($"correctActionCnt", $"correctShareClickCnt"))

    val finalDataset = joinedTable
      .join(positionClickrateTable, Seq("videoId", "vType", "clickrateType"), "left_outer")
      .join(joinedCorrectTable, Seq("videoId", "vType", "clickrateType"), "left_outer")
      .na.fill(0, Seq("correctImpressCnt", "correctActionCnt"))
      .na.fill("0.0000:0:0", Seq("correctClickrate1d", "correctPlayendrate1d", "correctSharerate1d"))
      .withColumn("date", lit(dataDate))
      .join(videoResolutionTable, Seq("videoId", "vType"), "left_outer")
      .na.fill(0, Seq("maxDuration"))
      .join(zanTable, Seq("videoId", "vType"), "left_outer")
      .join(commentTable, Seq("videoId", "vType"), "left_outer")
      .join(subscribeTable, Seq("videoId", "vType"), "left_outer")
      .na.fill(0, Seq("zanCnt", "commentCnt", "subscribeCnt"))

    finalDataset.repartition(1).write.parquet(outputPath)

  }

  def isPlayend = udf((endType:String) => {
    if (endType.trim.equals("playend"))
      1
    else
      0
  })

  def getIsPlayend = udf((playendCnt:Int) => {
    if (playendCnt >= 1)
      1
    else
      0
  })

  def isNewUser = udf((userId:Long) => {
    if (userId <= 0)
      0
    else
      1
  })

  def getDataDate(date:String) = udf(()=> {
    date
  })

  def getVType(vtype:String):String = {
    if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }

  def setColumnValue(value:Int) = udf(() => {
    value
  })

}
