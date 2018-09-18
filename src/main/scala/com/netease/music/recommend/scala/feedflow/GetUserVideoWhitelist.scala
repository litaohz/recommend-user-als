package com.netease.music.recommend.scala.feedflow

import com.netease.music.recommend.scala.feedflow.cf.ALSPipeline.parseItemPref
import com.netease.music.recommend.scala.feedflow.utils.SchemaObject._
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * 统一召回逻辑
  * Created by hzzhangjunfei1 on 2017/8/9.
  */
object GetUserVideoWhitelist {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoInfoPoolInput", true, "input directory")
    options.addOption("eventActiveUserWarehouse", true, "input directory")
    options.addOption("userItemProfile", true, "input directory")
    options.addOption("same_aritist_video_num", true, "same_aritist_video_num")
    options.addOption("same_keyword_video_num", true, "same_keyword_video_num")
    options.addOption("same_cateogry2level_video_num", true, "same_cateogry2level_video_num")
    options.addOption("max_video_num", true, "max_video_num")
    options.addOption("outputPath", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val eventActiveUserWarehouseInput = cmd.getOptionValue("eventActiveUserWarehouse")
    val userItemProfileInput = cmd.getOptionValue("userItemProfile")
    val sameArtistVideoNum = cmd.getOptionValue("same_aritist_video_num")
    val sameKeywordVideoNum = cmd.getOptionValue("same_keyword_video_num")
    val sameCateogry2levelVideoNum = cmd.getOptionValue("same_cateogry2level_video_num")
    val maxVideoNum = cmd.getOptionValue("max_video_num")
    val outputPath = cmd.getOptionValue("outputPath")

    val videoInfoPoolTable = spark.read.parquet(videoInfoPoolInput)

    val videoInfoM = mutable.HashMap[Long, Seq[Any]]()
    val controversialVideoSet = mutable.Set[String]()
//    val videoTobeExaminedSet = mutable.Set[String]()
    videoInfoPoolTable.collect().foreach(f = video => {

      val videoId = video.getLong(0)
      val isControversial = video.getInt(9)

      videoInfoM.put(videoId, video.toSeq)
//      videoTobeExaminedSet += videoId.toString
    
      // 建立controversialVideo索引
      if (isControversial == 1)
        controversialVideoSet += videoId.toString
    })
    logger.warn(s"videoInfoM size= + ${videoInfoM.size}\ncontroversialVideoSet size:${controversialVideoSet.size}")

    import spark.implicits._

    // 获取待推荐用户id
    val qualifiedUsers = spark.read.textFile(userItemProfileInput)
      .map(parseItemPref)
      .toDF
      .groupBy($"userId")
      .agg(count($"userId").as("userCnt"))
      .filter($"userCnt" > 3)
      .select("userId")

    val broadcastVideoInfoM = spark.sparkContext.broadcast(videoInfoM)
    val broadcastControversialVideoSet = spark.sparkContext.broadcast(controversialVideoSet)
//    val broadcastEventTobeExaminedSet = spark.sparkContext.broadcast(videoTobeExaminedSet)
    val broadcastSameArtistVideoNum = spark.sparkContext.broadcast(sameArtistVideoNum.toInt)
    val broadcastSameKeywordVideoNum = spark.sparkContext.broadcast(sameKeywordVideoNum.toInt)
    val broadcastSameCategory2elvelVideoNum = spark.sparkContext.broadcast(sameCateogry2levelVideoNum.toInt)
    val broadcastMaxVideoNum = spark.sparkContext.broadcast(maxVideoNum.toInt)
    // 获取待推荐用户的相关信息
    val eventActiveUserWarehouseTable = spark.read.parquet(eventActiveUserWarehouseInput)
      .join(qualifiedUsers, Seq("userId"), "inner")
    logger.warn(s"Qualified Users Today:${eventActiveUserWarehouseTable.count}")

    getWhiteEventForUser(eventActiveUserWarehouseTable, outputPath
                        ,broadcastVideoInfoM
                        ,broadcastControversialVideoSet
//                        ,broadcastEventTobeExaminedSet
                        ,broadcastSameArtistVideoNum
                        ,broadcastSameKeywordVideoNum
                        ,broadcastSameCategory2elvelVideoNum
                        ,broadcastMaxVideoNum
                        )
  }

  def getWhiteEventForUser(userInfoDF:DataFrame, outputPath:String
                           ,broadcastVideoInfoM:Broadcast[mutable.HashMap[Long, Seq[Any]]]
                           ,broadcastControversialVideoSet:Broadcast[mutable.Set[String]]
//                           ,broadcastVideoTobeExaminedSet:Broadcast[mutable.Set[String]]
                           ,broadcastSameArtistVideoNum:Broadcast[Int]
                           ,broadcastSameKeywordVideoNum:Broadcast[Int]
                           ,broadcastSameCategory2elvelVideoNum:Broadcast[Int]
                           ,broadcastMaxVideoNum:Broadcast[Int]
                           ) = {

    val recedVideos = userInfoDF("recedEvents")
    val subArtists = userInfoDF("subArtists")
    val followingCreators = userInfoDF("followingCreators")
    val userId = userInfoDF("userId")

    val recResultTable = {
        userInfoDF
          .withColumn("whiteVideosNew", getWhiteVideosForActiveUsers(broadcastVideoInfoM
                                                                    ,broadcastControversialVideoSet
//                                                                    ,broadcastVideoTobeExaminedSet
                                                                    ,broadcastSameArtistVideoNum
                                                                    ,broadcastSameKeywordVideoNum
                                                                    ,broadcastSameCategory2elvelVideoNum
                                                                    ,broadcastMaxVideoNum
                                                                    )(recedVideos,subArtists, followingCreators, userId)
                     )
          .select("userId", "whiteVideosNew")
    }
    val resultCol = recResultTable("whiteVideosNew")
    val finalResult = recResultTable.filter(resultCol =!= "null")

    finalResult.write.option("sep", "\t").csv(outputPath)
  }
}
