package com.netease.music.recommend.scala.feedflow.userProfile

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._

object GetUselessUser {

  case class Video(videoId:Long, vType:String)
  case class Impress(videoId:Long, vType:String, actionUserId:String, logDate:String, page:String)
  case class PlayAction(videoId:Long, vType:String, actionUserId:String, time:Long, logDate:String, playType:String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("impress", true, "log input directory")
    options.addOption("playend", true, "log input directory")
    options.addOption("upslide", true, "log input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val impressInput = cmd.getOptionValue("impress")
    val playendInput = cmd.getOptionValue("playend")
    val upslideInput = cmd.getOptionValue("upslide")
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val usefulPage = Seq("recommendvideo", "mvplay", "videoplay")
    val usefulVType = Seq("mv", "video")

    val playTable = spark.sparkContext.textFile(playendInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(6)
        val actionUserId = info(3)
        val logDate = getLogDate(info(4).toLong)
        val videoId = info(5).toLong
        var time = 0L
        try {
          time = info(15).toLong
        } catch {
          case ex :NumberFormatException => time = 0
        }
        val playType = info(16)
        PlayAction(videoId, getVType(vType), actionUserId, time, logDate, playType)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .filter($"videoId" > 100 && $"time" > 0)
      .groupBy("videoId", "vType", "actionUserId", "logDate")
      .agg(sum("time").as("viewTime"), collect_set($"playType").as("playTypes"))

    // 获取有下拉行为的用户
    val upslideUserTable = spark.sparkContext.textFile(upslideInput)
      .map {line =>
        val info = line.split("\t")
        val actionUserId = info(3)
        actionUserId
      }
      .toDF("actionUserId")
      .groupBy($"actionUserId")
      .agg(count("actionUserId").as("userUpslideCnt"))
      .withColumn("hasUpslideAction", getDefaultColumn(1)())
      .select("actionUserId", "hasUpslideAction")
    // 获取有playend行为的用户
    val playendUserTable = playTable
      .filter($"viewTime">0)
      .groupBy($"actionUserId")
      .agg(count("actionUserId").as("userPlayendCnt"))
      .withColumn("hasPlayendAction", getDefaultColumn(1)())
      .select("actionUserId", "hasPlayendAction")

    val impressTable = spark.sparkContext.textFile(impressInput)
      .map {line =>
        val info = line.split("\t")
        val page = info(1)
        val actionUserId = info(3)
        val logDate = getLogDate(info(4).toLong)
        val videoId = info(5).toLong
        val vType = info(6)
        Impress(videoId, getVType(vType), actionUserId, logDate, page)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"page".isin(usefulPage : _*))
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("actionUserId")
      .agg(countDistinct($"videoId").as("impressVideoCnt"))


    val uselessUserTable = impressTable
      .join(upslideUserTable, Seq("actionUserId"), "left_outer")
      .join(playendUserTable, Seq("actionUserId"), "left_outer")
      .na.fill(0, Seq("hasUpslideAction", "hasPlayendAction"))
      .withColumn("isUselessUser", isUselessUser($"hasUpslideAction", $"hasPlayendAction"))
      .filter($"isUselessUser"===1)
      .select("actionUserId")

    uselessUserTable.repartition(16).write.option("sep", "\t").csv(outputPath)
  }

  def isUselessUser = udf((hasUpslideAction:Int, hasPlayendAction:Int) => {
    if (hasPlayendAction == 0 && hasUpslideAction == 0)
      1
    else
      0
  })

  def getLogDate(timeInMilis:Long):String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(new Date(timeInMilis))
  }

  def getVType(vtype:String):String = {
    if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }
}
