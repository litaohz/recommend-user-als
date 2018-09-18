package com.netease.music.recommend.scala.feedflow

import com.netease.music.recommend.scala.feedflow.utils.userFunctions.getNewStringColumn
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, ml}

import scala.collection.mutable

/**
  * 判断视频是否是音乐视频
  */
object GetMusicVideoPoolFromVideoRcmdMeta {

  case class VideoEventIndex(videoId:Long, eventId:Long)
  case class ControversialArtistidForMarking(controversialArtistidForMarking:Long)
  case class ClickrateRecord(clickrate:Double, clickCount:Long, impressCount:Long)
  case class ControversialEventFromKeywordContent(eventId:Long, artistsFromKeywordContent:String)
  case class ControversialEventFromKeywordCreator(creatorIdFromNickname:Long, artistsFromKeywordCreator:String)
  case class VideoRcmdMeta(videoId:Long, vType:String, creatorId:Long, title:String, description:String
                           ,category:String, artistIds:String, videoBgmIds:String, bgmIds:String, userTagIds:String
                           ,auditTagIds:String, expireTime:Long)

  def getIds = udf((ids: String) => {
    if (ids == null || ids.length <= 2 || ids.equalsIgnoreCase("null"))
      "0"
    else
      ids.substring(1, ids.length-1).replace(",", "_tab_")
  })

  def getCategoryId = udf((ids: String) => {
    if (ids == null || ids.length <= 2 || ids.equalsIgnoreCase("null") || ids.startsWith("[0,"))
      "-1_-1"
    else
      ids.substring(1, ids.length-1).replace(",", "_")
  })

  def comb = udf((videoId: Long, vType: String, isMusic: Int) => {
    videoId.toString + "\t" + vType + "\t" + isMusic.toString
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("videoRcmdMeta", true, "input directory")
    options.addOption("musicEventCategories", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoRcmdMetaInput = cmd.getOptionValue("videoRcmdMeta")
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._
    val musicVideoCategoriesSet = collection.mutable.Set[String]()
    spark.sparkContext.textFile(cmd.getOptionValue("musicEventCategories"))
      .collect
      .foreach(categoryInfo => {
        musicVideoCategoriesSet += categoryInfo
      })
    println("musicEventCategoriesSet:\t" + musicVideoCategoriesSet.toString)

    val videoRcmdMetaTable = spark.read.json(videoRcmdMetaInput)
      .withColumn("category", getCategoryId($"category"))

    val nowTimestampInMillisconds = System.currentTimeMillis()
    val videoInfoPoolTable = videoRcmdMetaTable
        .filter($"expireTime">=nowTimestampInMillisconds)
        .withColumn("videoId", $"videoId")
        .withColumn("vType", getNewStringColumn("video")($"videoId"))
        .withColumn("isMusicVideo", isMusicVideo(musicVideoCategoriesSet)($"category"))
        .select("videoId", "vType", "isMusicVideo")

    videoInfoPoolTable.withColumn("text", comb($"videoId", $"vType", $"isMusicVideo"))
      .select("text").repartition(1).write.text(cmd.getOptionValue("output"))
  }

  def isMusicVideo(musicVideoCategoriesSet: mutable.Set[String]) = udf((eventCategory: String) => {
    val category1 = eventCategory.split("_")(0) + "_"
    if (musicVideoCategoriesSet.contains(category1) || musicVideoCategoriesSet.contains(eventCategory))
      1
    else
      0
  })

}
