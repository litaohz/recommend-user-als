package com.netease.music.recommend.scala.feedflow


import breeze.linalg.DenseVector
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.{SparkConf, ml}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import com.netease.music.recommend.scala.feedflow.utils.userFunctions.isControversialEvent
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}

import scala.collection.mutable

/**
  * Created by hzzhangjunfei1 on 2017/8/1.
  */
object GetVideoPoolFromVideoRcmdMeta {

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

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("videoRcmdMeta", true, "input directory")
    options.addOption("eventResourceInfoPool", true, "input directory")
    options.addOption("videoEventRelation", true, "input directory")
    options.addOption("musicEventCategories", true, "input directory")
    options.addOption("controversialFiguresInput", true, "input directory")
    options.addOption("videoClickrateInput", true, "input directory")
    options.addOption("videoHotscoreInput", true, "input directory")
    options.addOption("output", true, "output directory")
    options.addOption("outputNkv", true, "outputNkv directory")
    options.addOption("videoEventIndexOutput", true, "videoEventIndexOutput directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoRcmdMetaInput = cmd.getOptionValue("videoRcmdMeta")
    val eventResourceInfoPoolInput = cmd.getOptionValue("eventResourceInfoPool")
    val videoEventRelationInput = cmd.getOptionValue("videoEventRelation")
    val musicEventCategoriesInput = cmd.getOptionValue("musicEventCategories")
    val controversialFiguresInput = cmd.getOptionValue("controversialFiguresInput")
    val videoClickrateInput = cmd.getOptionValue("videoClickrateInput")
    val videoHotscoreInput = cmd.getOptionValue("videoHotscoreInput")
    val outputPath = cmd.getOptionValue("output")
    val outputNkvPath = cmd.getOptionValue("outputNkv")
    val videoEventIndexOutputPath = cmd.getOptionValue("videoEventIndexOutput")

    import spark.implicits._

    val musicVideoCategoriesSet = collection.mutable.Set[String]()
    spark.sparkContext.textFile(musicEventCategoriesInput)
      .collect
      .foreach(categoryInfo => {
        musicVideoCategoriesSet += categoryInfo
      })
    println("musicEventCategoriesSet:\t" + musicVideoCategoriesSet.toString)

    val controversialArtistIdSet = collection.mutable.Set[String]()
    spark.sparkContext.textFile(controversialFiguresInput)
      .collect()
      .foreach(line => {
          val info = line.split("\t")
          val controversialArtistid = info(0)
          controversialArtistIdSet += controversialArtistid
        })
    println("controversialArtistIdSet:\t" + controversialArtistIdSet.toString)

    val videoRcmdMetaTable = spark.read.json(videoRcmdMetaInput)
      .withColumn("category", getCategoryId($"category"))
      .withColumn("artistIds", getIds($"artistIds"))
      .withColumn("videoBgmIds", getIds($"videoBgmIds"))
      .withColumn("bgmIds", getIds($"bgmIds"))
      .withColumn("userTagIds", getIds($"userTagIds"))
      .withColumn("auditTagIds", getIds($"auditTagIds"))
      .na.fill("null", Seq("title", "description"))

    val videoEventRelationTable = spark.sparkContext.textFile(videoEventRelationInput)
      .map{ line =>
        val info = line.split("\01")
        val eventId = info(1).toLong
        val videoId = info(3).toLong
        VideoEventIndex(videoId, eventId)
      }
      .toDF
    val eventResourceInfoPoolTable = spark.read.parquet(eventResourceInfoPoolInput)
      .filter($"resourceType" === "video")
      .join(videoEventRelationTable, Seq("eventId"), "inner")
      .withColumn("isControversialFromEvent", isControversialEvent($"isControversialEvent", $"isControversialFigure"))
      .select("videoId", "eventId", "score", "isControversialFromEvent")

    // 视频热门分：
    val videoHotscore = spark.read.parquet(videoHotscoreInput)
      .select("videoId", "vType", "hotScore30days")

    // 点击率计算：
    val videoClickrate = spark.read.parquet(videoClickrateInput)
      .filter($"vType"==="video" && $"clickrateType"==="flow")
      .withColumn("weekClickrate", transformToClickrateRecord($"clickrate7days"))
      .withColumn("smoothClickrate", computeSmoothRate(2, 100)($"weekClickrate.clickCount", $"weekClickrate.impressCount"))
      .select("videoId", "vType", "weekClickrate", "smoothClickrate")


    val nowTimestampInMillisconds = System.currentTimeMillis()
    val videoInfoPoolTable =
      videoRcmdMetaTable
        .join(eventResourceInfoPoolTable, Seq("videoId"), "left_outer")
        .filter($"expireTime">=nowTimestampInMillisconds)
        .withColumn("vType", getNewStringColumn("video")($"videoId"))
        .withColumn("isMusicVideo", isMusicVideo(musicVideoCategoriesSet)($"category"))
        .withColumn("isControversial", isControversialVideo(controversialArtistIdSet)($"artistIds", $"isControversialFromEvent"))
        .withColumn("creatorId", $"pubUid")
        .na.fill(0, Seq("isControversial"))
        .na.fill(0, Seq("score"))
        .na.fill(0, Seq("eventId"))
        .filter(!$"title".contains("发布的视频"))
        .join(videoClickrate, Seq("videoId", "vType"), "left_outer")
        .join(videoHotscore, Seq("videoId", "vType"), "left_outer")
        .na.fill(0.0, Seq("hotScore30days", "smoothClickrate"))
        .withColumn("cutTopScore", cutTop(5000)($"hotScore30days"))
        .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
               ,"bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial"
               ,"score", "title", "description", "eventId", "isMusicVideo"
               ,"cutTopScore", "smoothClickrate")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val assembler = new VectorAssembler()
      .setInputCols(Array("cutTopScore", "smoothClickrate"))
      .setOutputCol("features")
    val df = assembler.transform(videoInfoPoolTable)
    val scalerModel = scaler.fit(df)
    val scaledData = scalerModel.transform(df)
    val finalTable = scaledData.withColumn("rawPrediction", dotVectors($"scaledFeatures"))

    finalTable.write.parquet(outputPath + "/parquet")
//    finalTable.write.option("sep", "\t").csv(outputPath + "/csv")

    // 提供给上传redis的输出版本
    finalTable
      .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
        ,"bgmIds", "userTagIds", "auditTagIds", "category", "isControversial"
        ,"rawPrediction")
      .write.option("sep", "\t").csv(outputNkvPath)

    finalTable
      .filter($"eventId">0)
      .select("videoId", "eventId")
      .write.option("sep", "\t").csv(videoEventIndexOutputPath)

  }


  def isMusicVideo(musicVideoCategoriesSet: mutable.Set[String]) = udf((eventCategory: String) => {

    val category1 = eventCategory.split("_")(0) + "_"
    if (musicVideoCategoriesSet.contains(category1) || musicVideoCategoriesSet.contains(eventCategory))
      1
    else
      0
  })

  def isControversialVideo(controversialArtistIdSet:mutable.Set[String]) = udf((artistIds:String, isControversialFromEvent:Int) => {

    var ret = 0
    if (isControversialFromEvent == 1)
      ret = 1
    else {
      if (!artistIds.equals("0")) {
        for (artistId <- artistIds.split("_tab_")) {
          if (controversialArtistIdSet.contains(artistId))
            ret = 1
        }
      }
    }
    ret
  })

  def transformToClickrateRecord = udf((line:String) => {
    val info = line.split(":")
    val clickrate = info(0).toDouble
    val impressCnt = info(1).toLong
    val clickCnt = info(2).toLong
    ClickrateRecord(clickrate, clickCnt, impressCnt)
  })

  def computeSmoothRate(alpha:Int, beta:Int) = udf((clickCnt:Long ,impressCnt:Long) => {
    (clickCnt.toDouble + alpha) / (impressCnt + beta)
  })

  def cutTop(ceiling:Int) = udf((score:Double) => {
    Math.min(ceiling, score);
  })

  def dotVectors = udf((scaledFeatures:ml.linalg.DenseVector) => {
    val bv1 = new DenseVector(scaledFeatures.toArray)
    val w = new DenseVector[Double](Array(0.5, 2))
    bv1 dot w
  })
}
