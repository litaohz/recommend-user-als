package com.netease.music.recommend.scala.feedflow.quota

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetAlgClickrate {

  case class Video(videoId:Long, vType:String)
  case class Impress(videoId:Long, vType:String, actionUserId:String, page:String, alg:String, os:String)
  case class PlayAction(videoId:Long, vType:String, actionUserId:String, time:Long, source:String, os:String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("impress", true, "log input directory")
    options.addOption("playend", true, "log input directory")
    options.addOption("filterIphone", true, "filterIphone")
    options.addOption("output", true, "output directory")
    options.addOption("outputTmp", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val impressInput = cmd.getOptionValue("impress")
    val playendInput = cmd.getOptionValue("playend")
    val filterIphone = cmd.getOptionValue("filterIphone")
    val outputPath = cmd.getOptionValue("output")
    val outputTmpPath = cmd.getOptionValue("outputTmp")

    import spark.implicits._

    val usefulPage = Seq("recommendvideo", "mvplay", "videoplay")
    val usefulVType = Seq("mv", "video")
    val uselessAlgSet = Seq("tag", "mock_data")
    val impressTable = spark.sparkContext.textFile(impressInput)
      .map {line =>
        val info = line.split("\t")
        val page = info(1)
        val os = info(2)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val vType = info(6)
        val alg = info(8)
        Impress(videoId, getVType(vType), actionUserId, page, alg, os)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"page"==="recommendvideo")
      .filter($"vType".isin(usefulVType : _*))
      .filter(length($"alg")>0)
      .filter(!$"alg".isin(uselessAlgSet : _*))
      .filter($"page".isin(usefulPage : _*))
      .filter($"os"=!="iPhone")
      .groupBy("videoId", "vType", "actionUserId", "alg")
      .agg(count("videoId").as("useless"))
      .drop("useless")

    val usefulSource = Seq("mvplay_recommendvideo", "videoplay_recommendvideo", "recommendvideo")
    val playTable = spark.sparkContext.textFile(playendInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(6)
        val os = info(2)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val source = info(8)
        var time = 0L
        try {
          time = info(15).toLong
        } catch {
          case ext: Exception => {
            println("Failed to parse " + info(15) + " in " + line)
          }
        }
        PlayAction(videoId, getVType(vType), actionUserId, time, source, os)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"videoId" > 100 && $"time">0)
      .filter($"source"==="recommendvideo")
      .filter($"vType".isin(usefulVType : _*))
      .filter($"source".isin(usefulSource : _*))
      .filter($"os"=!="iPhone")
      .groupBy("videoId", "vType", "actionUserId")
      .agg(sum($"time").as("playTime"))

    val rawTable = impressTable
      .join(playTable, Seq("videoId", "vType", "actionUserId"), "left_outer")
      .na.fill(0, Seq("playTime"))
      .withColumn("isPlay", isPlay($"playTime"))

    rawTable
      .groupBy($"alg")
      .agg(sum($"isPlay").as("playUV"), count($"videoId").as("impressUV"))
      .withColumn("clickrate", getClickrateOnly($"impressUV", $"playUV"))
      .orderBy($"alg")
      .write.parquet(outputPath)
  }

  def getVType(vtype:String):String = {
    if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }

  def isPlay = udf((time:Long) => {
    if (time > 0)
      1
    else
      0
  })

  def getClickrateOnly = udf((impressCnt:Long, actionCnt:Long) => {
    if (impressCnt == 0)
      "0.000"
    else
      (actionCnt.toDouble/impressCnt.toDouble).formatted("%.4f")
  })
}
