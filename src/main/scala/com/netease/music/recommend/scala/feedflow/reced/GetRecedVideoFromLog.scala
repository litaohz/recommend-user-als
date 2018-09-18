package com.netease.music.recommend.scala.feedflow.reced

import com.netease.music.recommend.scala.feedflow.videoProfile.GetVideoHotScore.getExistsPath
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetRecedVideoFromLog {

  case class Impress(videoId:Long, vType:String, actionUserId:String, page:String)
  case class PlayAction(videoId:Long, vType:String, actionUserId:String, time:Long)
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
    options.addOption("mainpageImpress", true, "log input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val impressInput = cmd.getOptionValue("impress")
    val playendInput = cmd.getOptionValue("playend")
    val mainpageImpressInput = cmd.getOptionValue("mainpageImpress")
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val usefulVType = Seq("mv", "video")
    val impressTable = spark.sparkContext.textFile(impressInput)
      .map {line =>
        val info = line.split("\t")
        val page = info(1)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val vType = info(6)
//        if (vType.isEmpty && !page.isEmpty)
//          vType = getVType(page)
        Impress(videoId, getVType(vType), actionUserId, page)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"videoId" > 100)
      .filter($"vType".isin(usefulVType : _*))
      .select("actionUserId", "videoId", "vType")

    val mainpageImpressTable = spark.sparkContext.textFile(mainpageImpressInput)
      .map {line =>
        val info = line.split("\t")
        val page = info(1)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val vType = info(6)
        Impress(videoId, getVType(vType), actionUserId, page)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"videoId" > 100)
      .filter($"vType".isin(usefulVType : _*))
      .select("actionUserId", "videoId", "vType")

    val playTable = spark.sparkContext.textFile(playendInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(6)
        val actionUserId = info(3)
        var videoId = 0L
        var time = 0L
        try {
          videoId = info(5).toLong
          time = info(15).toLong
        } catch {
          case ex :NumberFormatException => {
            videoId = 0
            time = 0
          }
        }
        PlayAction(videoId, getVType(vType), actionUserId, time)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"videoId" > 100)
      .filter($"vType".isin(usefulVType : _*))
      .select("actionUserId", "videoId", "vType")

    val userRecedVideoFromLog = impressTable
      .union(playTable)
      .union(mainpageImpressTable)
      .rdd
      .map { line =>
        (line.getString(0), (line.getLong(1), line.getString(2)))
      }
      .groupByKey
      .map({case (key, value) => collectRecedVideos(key, value)})

    userRecedVideoFromLog.saveAsTextFile(outputPath)
  }

  def getVType(vtype:String):String = {
    if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }

  def collectRecedVideos(uid: String, videoInfos: Iterable[(Long, String)]): String = {
    uid + "\t" + videoInfos.map(line => line._1 + ":" + line._2).toSet.mkString(",")
  }
}
