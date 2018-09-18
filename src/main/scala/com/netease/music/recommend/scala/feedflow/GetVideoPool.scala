package com.netease.music.recommend.scala.feedflow

import java.io.InputStream

import breeze.linalg.DenseVector
import com.netease.music.recommend.scala.feedflow.utils.userFunctions.{isControversialEvent, _}
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.{SparkConf, ml}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

/**
  * Created by hzzhangjunfei1 on 2017/8/1.
  */
object GetVideoPool {

  case class VideoEventIndex(videoId:Long, eventId:Long)
  case class VideoResolution(videoId:Long, resolution:Int, duration:Int, width:Int, height:Int, resolutionInfo:String)
  case class ControversialArtistidForMarking(controversialArtistidForMarking:Long)
  case class ClickrateRecord(clickrate:Double, clickCount:Long, impressCount:Long)
  case class ControversialEventFromKeywordContent(eventId:Long, artistsFromKeywordContent:String)
  case class ControversialEventFromKeywordCreator(creatorIdFromNickname:Long, artistsFromKeywordCreator:String)
  case class VideoGroupInfo(groupId:Long, groupName:String, deleted:Int)
  case class VideoGroupTagMapping(groupId:Long, tagId:Long, deleted:Int)
  case class VideoRcmdMeta(videoId:Long, vType:String, creatorId:Long, title:String, description:String
                           ,category:String, artistIds:String, videoBgmIds:String, bgmIds:String, userTagIds:String
                           ,auditTagIds:String, expireTime:Long)

  def getIds = udf((ids: String) => {
    if (ids == null || ids.length <= 2 || ids.equalsIgnoreCase("null"))
      "0"
    else
      ids.substring(1, ids.length-1).replace(",", "_tab_")
  })

  def getJsonValue(key:String) = udf((json:String) => {

    var smallFlow = false
    if (json != null) {
      implicit val formats = DefaultFormats
      val jsonObj = parse(json)
      smallFlow = (jsonObj \ "smallFlow").extractOrElse[Boolean](false)
    }
    smallFlow
  })

  def getCategoryId = udf((ids: String) => {
    if (ids == null || ids.length <= 2 || ids.equalsIgnoreCase("null") || ids.startsWith("[0,"))
      "-1_-1"
    else
      ids.substring(1, ids.length-1).replace(",", "_")
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

  def computeSmoothRate4newUser(alpha:Int, beta:Int) = udf((newuserClickrate:String ,correctClickrate:String) => {
    var impressUV = newuserClickrate.split(":")(1).toLong
    var clickUV = newuserClickrate.split(":")(2).toLong
    if (impressUV <= 0) { // newuserClickrate为零时，使用correctClickrate作为备用clickrate
      impressUV = correctClickrate.split(":")(1).toLong
      clickUV = correctClickrate.split(":")(2).toLong
    }
    (clickUV.toDouble + alpha) / (impressUV + beta)
  })

  def cutTop(ceiling:Int) = udf((score:Double) => {
    Math.min(ceiling, score);
  })

  def dotVectors = udf((scaledFeatures:ml.linalg.DenseVector) => {
    val bv1 = new DenseVector(scaledFeatures.toArray)
    val w = new DenseVector[Double](Array(0.5, 2, 0))
    bv1 dot w
  })

  def dotVectors4newUser = udf((scaledFeatures:ml.linalg.DenseVector) => {
    val bv1 = new DenseVector(scaledFeatures.toArray)
    val w = new DenseVector[Double](Array(0.5, 0, 2))
    bv1 dot w
  })

  def collect_groups(key:Long, values:Iterable[(String, String)]):VideoTagInfo = {

    val tagIdsStr = values.map(line => line._1).mkString("_tab_")
    // 对标签做lowercase + 去空格
    val tagNamesStr = values.map(line => line._2.toLowerCase.replaceAll(" ", "")).mkString("_tab_")
    val tagIdAndNamesStr = values.map(line => line._1 + "_:_"  + line._2).mkString("_tab_")
    VideoTagInfo(key, tagIdsStr, tagNamesStr, tagIdAndNamesStr)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)
    import spark.implicits._

    val options = new Options
    options.addOption("videoRcmdMeta", true, "input directory")
    options.addOption("eventResourceInfoPool", true, "input directory")
    options.addOption("videoEventRelation", true, "input directory")
    options.addOption("videoResolution", true, "input directory")
    options.addOption("musicEventCategories", true, "input directory")
    options.addOption("controversialFiguresInput", true, "input directory")
    options.addOption("videoGroupTable", true, "input directory")
    options.addOption("videoGroupTagRelation", true, "input directory")
    options.addOption("videoClickrateInput", true, "input directory")
    options.addOption("videoHotscoreInput", true, "input directory")
    options.addOption("videoTag", true, "input directory")
    options.addOption("videoWarehouse", true, "input directory")
    options.addOption("poolStats", true, "input directory")
    options.addOption("downgrade", true, "input directory")
    options.addOption("certifiedUser", true, "input directory")

    options.addOption("output", true, "output directory")
    options.addOption("outputNkv", true, "outputNkv directory")
    options.addOption("videoEventIndexOutput", true, "videoEventIndexOutput directory")
    options.addOption("videoGroupInfoOutput", true, "videoGroupInfoOutput directory")
    options.addOption("videoTagGroupOutput", true, "videoTagGroupOutput directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoRcmdMetaInput = cmd.getOptionValue("videoRcmdMeta")
    val eventResourceInfoPoolInput = cmd.getOptionValue("eventResourceInfoPool")
    val videoEventRelationInput = cmd.getOptionValue("videoEventRelation")
    val videoResolutionInput = cmd.getOptionValue("videoResolution")
    val musicEventCategoriesInput = cmd.getOptionValue("musicEventCategories")
    val controversialFiguresInput = cmd.getOptionValue("controversialFiguresInput")
    val videoGroupTableInput = cmd.getOptionValue("videoGroupTable")
    val videoGroupTagRelationInput = cmd.getOptionValue("videoGroupTagRelation")
    val videoClickrateInput = cmd.getOptionValue("videoClickrateInput")
    val videoHotscoreInput = cmd.getOptionValue("videoHotscoreInput")
    val videoTagInput = cmd.getOptionValue("videoTag")
    val videoWarehouseInput = cmd.getOptionValue("videoWarehouse")
    val poolStats = cmd.getOptionValue("poolStats")
    val downgrade = cmd.getOptionValue("downgrade")
    val certifiedUser = cmd.getOptionValue("certifiedUser")

    val outputPath = cmd.getOptionValue("output")
    val outputNkvPath = cmd.getOptionValue("outputNkv")
    val videoEventIndexOutputPath = cmd.getOptionValue("videoEventIndexOutput")
    val videoGroupInfoOutputPath = cmd.getOptionValue("videoGroupInfoOutput")
    val videoTagGroupOutputPath = cmd.getOptionValue("videoTagGroupOutput")

    val poolStatsTable = spark.read.parquet(poolStats)
      .withColumn("videoId", splitAndGet(":", 0)($"idtype").cast(LongType))
      .withColumn("vType", splitAndGet(":", 1)($"idtype"))
      .select($"videoId", $"vType", $"score".as("poolStatsScore"))

    val certifiedUserS = spark.sparkContext.textFile(certifiedUser)
      .map{line =>
        val info = line.split("\t")
        val userId = info(0)
        userId.toLong
      }.collect
      .toSet

    val downgradeTable = spark.read.parquet(downgrade)
      .withColumn("vType", lit("video"))

    // 音乐视频的categyry集合
    val musicVideoCategoriesSet = collection.mutable.Set[String]()
    spark.sparkContext.textFile(musicEventCategoriesInput)
      .collect
      .foreach(categoryInfo => {
        musicVideoCategoriesSet += categoryInfo
      })
    println("musicEventCategoriesSet:\t" + musicVideoCategoriesSet.toString)
    // 争议艺人集合
    val controversialArtistIdSet = collection.mutable.Set[String]()
    spark.sparkContext.textFile(controversialFiguresInput)
      .collect()
      .foreach(line => {
        val info = line.split("\t")
        val controversialArtistid = info(0)
        controversialArtistIdSet += controversialArtistid
      })
    println("controversialArtistIdSet:\t" + controversialArtistIdSet.toString)
    // 获取group信息
    //    val stream:InputStream = getClass.getResourceAsStream("/groupTagMappings")
    val videoGroupInfoTable = spark.sparkContext.textFile(videoGroupTableInput)
      .map{line =>
        val info = line.split("\01")
        val groupId = info(0).toLong
        val groupName = info(1)
        val deleted = info(7).toInt
        VideoGroupInfo(groupId, groupName, deleted)
      }
      .toDF
      .filter($"deleted"===0)
      .drop($"deleted")
//    videoGroupInfoTable
//      .select($"groupId", $"groupName")
//      .write.option("sep", ",").csv(videoGroupInfoOutputPath)
    val videoGroupTagMappingTable = spark.sparkContext.textFile(videoGroupTagRelationInput)
      .map{line =>
        val info = line.split("\01")
        val groupId = info(1).toLong
        val tagId = info(2).toLong
        val deleted = info(5).toInt
        VideoGroupTagMapping(groupId, tagId, deleted)
      }
      .toDF
      .filter($"deleted"===0)
      .drop($"deleted")
    val tagGroupM = scala.collection.mutable.HashMap[Long, Seq[String]]()
    videoGroupInfoTable
      .join(videoGroupTagMappingTable, Seq("groupId"), "left_outer")
      .groupBy($"tagId")
      .agg(collect_set(concat_ws(":", $"groupId", $"groupName").as("groupIdAndName")))
      .filter($"tagId".isNotNull)
      .rdd
      .collect
      .foreach {line =>
        val tagId = line.getLong(0)
        val groupIdAndNames = line.getSeq[String](1)
        tagGroupM.put(tagId, groupIdAndNames)
      }

    /*scala.io.Source.fromInputStream(stream)
      .getLines
      .foreach{line =>
        val info = line.split("\t")
        val group = info(0)
        val tags = info(1).split(",")
        tags.foreach {tag =>
          val groups = tagGroupM.getOrElse(tag, scala.collection.mutable.Set())
          groups.add(group)
          tagGroupM.put(tag, groups)
        }
      }*/
    logger.info("tagGroupMap:" + tagGroupM.toString)
    val broadcaseTagGroupM = spark.sparkContext.broadcast(tagGroupM)

    // 视频审核表
    val videoRcmdMetaTable = spark.read.json(videoRcmdMetaInput)
      .withColumn("category", getCategoryId($"category"))
      .withColumn("artistIds", getIds($"artistIds"))
      .withColumn("videoBgmIds", getIds($"videoBgmIds"))
      .withColumn("bgmIds", getIds($"bgmIds"))
      .withColumn("userTagIds", getIds($"userTagIds"))
      .withColumn("auditTagIds", getIds($"auditTagIds"))
      .withColumn("smallFlow", getJsonValue("smallFlow")($"extData"))
      .na.fill("null", Seq("title", "description"))
      .filter(!$"smallFlow")
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
        collect_set($"resolutionInfo").as("resolutionInfos")
        ,max($"resolution").as("maxResolution")
        ,max($"width").as("maxWidth")
        ,max($"height").as("maxHeight")
        ,max($"duration").as("maxDuration")
      )

    // 视频动态映射表
    val videoEventRelationTable = spark.sparkContext.textFile(videoEventRelationInput)
      .map{ line =>
        val info = line.split("\01")
        val eventId = info(1).toLong
        val videoId = info(3).toLong
        VideoEventIndex(videoId, eventId)
      }
      .toDF
    // eventPool中提取热门分score和isControversialFromEvent
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
      .withColumn("smoothClickrate4newUser", computeSmoothRate4newUser(2, 100)($"newuserClickrate7days", $"correctClickrate7days"))
      .select("videoId", "vType", "weekClickrate", "smoothClickrate", "smoothClickrate4newUser")


    val nowTimestampInMillisconds = System.currentTimeMillis()
    val videoInfoPoolTable =
      videoRcmdMetaTable
        .join(videoResolutionTable, Seq("videoId"), "left_outer")
        .join(eventResourceInfoPoolTable, Seq("videoId"), "left_outer")
        .filter($"expireTime">=nowTimestampInMillisconds)
        .withColumn("vType", getNewStringColumn("video")($"videoId"))
        //.withColumn("isMusicVideo", isMusicVideo(musicVideoCategoriesSet)($"category"))
        .withColumn("isControversial", isControversialVideo(controversialArtistIdSet)($"artistIds", $"isControversialFromEvent"))
        .withColumn("creatorId", $"pubUid")
        .na.fill(0, Seq("isControversial"))
        .na.fill(0, Seq("score"))
        .na.fill(0, Seq("eventId"))
        .filter(!$"title".contains("发布的视频"))
        .join(videoClickrate, Seq("videoId", "vType"), "left_outer")
        .join(videoHotscore, Seq("videoId", "vType"), "left_outer")
        .join(poolStatsTable, Seq("videoId", "vType"), "left_outer")
        .join(downgradeTable, Seq("videoId", "vType"), "left_outer")
        .na.fill(0.0, Seq("hotScore30days", "smoothClickrate", "smoothClickrate4newUser", "downgradeScore", "poolStatsScore"))
        .withColumn("cutTopScore", cutTop(50000)($"hotScore30days"))
        .select(
          "videoId", "vType", "creatorId", "artistIds", "videoBgmIds",
          "bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial",
          "score", "title", "description", "eventId", /*"isMusicVideo",*/
          "cutTopScore", "smoothClickrate", "smoothClickrate4newUser", "resolutionInfos", "forNew",
          "maxResolution", "maxWidth", "maxHeight", "maxDuration", "addTime",
          "downgradeScore", "poolStatsScore", "coverImgId"
        )

    // 归一化features并计算rawPredictions
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val assembler = new VectorAssembler()
      .setInputCols(Array("cutTopScore", "smoothClickrate", "smoothClickrate4newUser"))
      .setOutputCol("features")
    val df = assembler.transform(videoInfoPoolTable)
    val scalerModel = scaler.fit(df)
    val scaledData = scalerModel.transform(df)
    val videoPool_stage2 = scaledData
      .withColumn("rawPredictionOld", dotVectors($"scaledFeatures"))
      .withColumn("rawPrediction4newUser", dotVectors4newUser($"scaledFeatures"))


    // 获取tagName和tagId的mapping
    val tagNameMappingTable = spark.sparkContext.textFile(videoTagInput)
      .map { line =>
        val info = line.split("\01")
        val tagId = info(0)
        val tagName = info(1)
        TagMap(tagId, tagName)
      }
      .toDF
    val videoTagsGroupsTable = videoPool_stage2
      .filter($"vType"==="video")
      .withColumn("tagIds", mergeTagIds($"userTagIds", $"auditTagIds"))
      .withColumn("tagId", explode(splitByDelim("_tab_")($"tagIds")))
      .join(tagNameMappingTable, Seq("tagId"), "left_outer")
      .na.fill("null", Seq("tagName"))
      .filter($"tagName"=!="null")
      .select($"videoId", $"tagId", $"tagName")
      .rdd
      .map(line => (line.getLong(0), (line.getString(1), line.getString(2))))
      .groupByKey
      .map {case (key, value) => collect_groups(key, value)}
      .toDF
      .withColumn("groupIdAndNames", getGroupIdAndNames(broadcaseTagGroupM)($"tagIds"))
      .withColumn("vType", lit("video"))

    val videoWarehouseTable = spark.read.parquet(videoWarehouseInput)
      .select($"videoId", $"vType", $"rawPrediction")

//    val broadcastMusicVideoTags = spark.sparkContext.broadcast(musicVideoTags)
    val finalTable = videoPool_stage2
      .join(videoTagsGroupsTable, Seq("videoId", "vType"), "left_outer")
      .join(videoWarehouseTable, Seq("videoId", "vType"), "left_outer")
      .na.fill("null", Seq("tagIds", "tagNames", "tagIdAndNames", "groupIdAndNames"))
      .na.fill(0.0, Seq("rawPrediction"))
//      .withColumn("isMusicVideoByTag", isMusicVideoByTag(musicVideoTags)($"groupIdAndNames"))
      .withColumn("isMusicVideo", isMusicVideoByGroupId($"groupIdAndNames"))
        .withColumn("isCertifiedCreator", isCertifiedCreator(certifiedUserS)($"creatorId"))
      .coalesce(16)
      .cache

    // 输出videoPool的parquet格式
    finalTable.write.parquet(outputPath + "/parquet")
    // 提供给上传redis的输出版本
    finalTable
      .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
        ,"bgmIds", "userTagIds", "auditTagIds", "category", "isControversial"
        ,"rawPrediction", "rawPrediction4newUser", "isMusicVideo", "downgradeScore", "poolStatsScore")
      .write.option("sep", "\t").csv(outputNkvPath)
    // 输出videoId、eventId索引
    finalTable
      .filter($"eventId">0)
      .select("videoId", "eventId")
      .write.option("sep", "\t").csv(videoEventIndexOutputPath)

    finalTable
      .filter($"groupIdAndNames"=!="null")
      .withColumn("groupIdAndName", explode(splitByDelim("_tab_")($"groupIdAndNames")))
      .groupBy($"groupIdAndName")
      .agg(count($"groupIdAndName").as("videoCntInGroup"))
      .withColumn("groupId", extractByPos(0, ":")($"groupIdAndName"))
      .withColumn("groupName", extractByPos(1, ":")($"groupIdAndName"))
      .select("groupId", "groupName", "videoCntInGroup")
      .orderBy($"videoCntInGroup".desc)
      .write.option("sep", ",").csv(videoGroupInfoOutputPath)

    finalTable
      .filter($"tagIdAndNames"=!="null" && $"groupIdAndNames"=!="null")
      .select("videoId", "vType", "tagIdAndNames", "groupIdAndNames")
      .write.option("sep", "\t").csv(videoTagGroupOutputPath)
  }
}
