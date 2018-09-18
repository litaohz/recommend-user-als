package com.netease.music.recommend.scala.feedflow

import com.netease.music.recommend.scala.feedflow.utils.userFunctions.getNewStringColumn
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * 判断视频是否是音乐视频
  */
object GetMusicVideoPoolFromVideoRcmdMeta2 {

  case class VideoEventIndex(videoId:Long, eventId:Long)
  case class ControversialArtistidForMarking(controversialArtistidForMarking:Long)
  case class ClickrateRecord(clickrate:Double, clickCount:Long, impressCount:Long)
  case class ControversialEventFromKeywordContent(eventId:Long, artistsFromKeywordContent:String)
  case class ControversialEventFromKeywordCreator(creatorIdFromNickname:Long, artistsFromKeywordCreator:String)
  case class VideoRcmdMeta(videoId:Long, vType:String, creatorId:Long, title:String, description:String
                           ,category:String, artistIds:String, videoBgmIds:String, bgmIds:String, userTagIds:String
                           ,auditTagIds:String, expireTime:Long)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("videoRcmdMeta", true, "input directory")
    options.addOption("music_tags", true, "input directory")
    options.addOption("Music_VideoTag", true, "input directory")
    options.addOption("output", true, "output directory")
    import spark.implicits._
    val parser = new PosixParser
    val cmd = parser.parse(options, args)
    val musicTags = mutable.HashSet[String]()
    for (tag <- cmd.getOptionValue("music_tags").split(",")) {
      musicTags.add(tag)
    }
    println("musicTags size:" + musicTags.size)

    val videoRcmdMetaInput = cmd.getOptionValue("videoRcmdMeta")
    val outputPath = cmd.getOptionValue("output")

    val musicTagIds = mutable.HashSet[String]()
    // val musicVideoCategoriesSet = collection.mutable.Set[String]()
    spark.sparkContext.textFile(cmd.getOptionValue("Music_VideoTag"))
      .collect
      .foreach(line => {
        // tagid\01tagname
        val ts = line.split("\01")
        if (ts.length >= 2) {
          val tagid = ts(0)
          val tagname = ts(1)
          if (musicTags.contains(tagname)) {
            musicTagIds.add(tagid)
          }
        }
      })
    println("musicTagIds:\t" + musicTagIds)

    val videoRcmdMetaTable = spark.read.json(videoRcmdMetaInput)
      .withColumn("category", getCategoryId($"category"))
      .withColumn("tagIds", getTagIds($"auditTagIds", $"userTagIds"))

    val nowTimestampInMillisconds = System.currentTimeMillis()
    val videoInfoPoolTable = videoRcmdMetaTable
        .filter($"expireTime">=nowTimestampInMillisconds)
        .withColumn("videoId", $"videoId")
        .withColumn("vType", getNewStringColumn("video")($"videoId"))
        //.withColumn("isMusicVideo", isMusicVideo(musicVideoCategoriesSet)($"category"))
        .withColumn("isMusicVideo", isMusicVideoTag(musicTagIds)($"tagIds"))
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

  def isMusicVideoTag(musicTagids: mutable.Set[String]) = udf((tagids: Seq[String]) => {
    var isMusic = 0
    if (tagids != null) {
      for (tagid <- tagids if isMusic==0) {
        if (musicTagids.contains(tagid)) {
          isMusic = 1
        } else {
          0
        }
      }
    }
    isMusic
  })
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

  def getTagIds = udf((auditTagIds: String, userTagIds: String) => {
    val tagIds = mutable.HashSet[String]()
    if (auditTagIds != null && auditTagIds.length > 2 && !auditTagIds.equalsIgnoreCase("null") ) {
      for (tagid <- auditTagIds.substring(1, auditTagIds.length-1).split(",")) {
        tagIds.add(tagid)
      }
    }
    if (userTagIds != null && userTagIds.length > 2 && !userTagIds.equalsIgnoreCase("null") ) {
      for (tagid <- userTagIds.substring(1, userTagIds.length-1).split(",")) {
        tagIds.add(tagid)
      }
    }
    tagIds.toSeq
  })

}
