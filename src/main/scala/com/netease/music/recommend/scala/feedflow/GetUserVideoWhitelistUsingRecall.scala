package com.netease.music.recommend.scala.feedflow

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

import utils.userFunctions._
import utils.SchemaObject._

/**
  * 召回后，统一过滤逻辑，快速+扩展信息版	
  * Created by lvqiang on 2017/6/13.
  */
object GetUserVideoWhitelistUsingRecall {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("videoInfoPoolInput", true, "input directory")
    options.addOption("eventActiveUserWarehouse", true, "input directory")
    options.addOption("recallInput", true, "recall input")
    options.addOption("same_aritist_video_num", true, "same_aritist_video_num")
    options.addOption("same_keyword_video_num", true, "same_keyword_video_num")
    options.addOption("same_cateogry2level_video_num", true, "same_cateogry2level_video_num")
    options.addOption("max_video_num", true, "max_video_num")
    options.addOption("outputPath", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val eventActiveUserWarehouseInput = cmd.getOptionValue("eventActiveUserWarehouse")
    val sameArtistVideoNum = cmd.getOptionValue("same_aritist_video_num")
    val sameKeywordVideoNum = cmd.getOptionValue("same_keyword_video_num")
    val sameCateogry2levelVideoNum = cmd.getOptionValue("same_cateogry2level_video_num")
    val maxVideoNum = cmd.getOptionValue("max_video_num")
    val recallInput = cmd.getOptionValue("recallInput")
    val outputPath = cmd.getOptionValue("outputPath")

    import spark.implicits._

    val videoInfoPoolTable = spark.read.parquet(videoInfoPoolInput)

    val videoInfoM = mutable.HashMap[Long, Seq[Any]]()
    val controversialVideoSet = mutable.Set[String]()
    val videoTobeExaminedSet = mutable.Set[String]()
    videoInfoPoolTable.collect().foreach(f = video => {

      val videoId = video.getLong(0)
      val isControversial = video.getInt(9)

      videoInfoM.put(videoId, video.toSeq)
      videoTobeExaminedSet += videoId.toString
    
      // 建立controversialVideo索引
      if (isControversial == 1)
        controversialVideoSet += videoId.toString
    })

    println("videoInfoM size:\t" + videoInfoM.size)
    println("controversialVideoSet size:\t" + controversialVideoSet.size)
    println("videoTobeExaminedSet size:\t" + videoTobeExaminedSet.size)

    val broadcastVideoInfoM = spark.sparkContext.broadcast(videoInfoM)
    val broadcastControversialVideoSet = spark.sparkContext.broadcast(controversialVideoSet)
    val broadcastEventTobeExaminedSet = spark.sparkContext.broadcast(videoTobeExaminedSet)
    val broadcastSameArtistVideoNum = spark.sparkContext.broadcast(sameArtistVideoNum.toInt)
    val broadcastSameKeywordVideoNum = spark.sparkContext.broadcast(sameKeywordVideoNum.toInt)
    val broadcastSameCategory2elvelVideoNum = spark.sparkContext.broadcast(sameCateogry2levelVideoNum.toInt)
    val broadcastMaxVideoNum = spark.sparkContext.broadcast(maxVideoNum.toInt)
    // 获取待推荐用户的相关信息
    val eventActiveUserWarehouseTable = spark.read.parquet(eventActiveUserWarehouseInput)
      .na.fill(3.0, Seq("rank"))

    val userRecallVideoData = spark.read.option("sep", "\t")
      .schema(userRecallVideoSchema).csv(recallInput)
    
    val eventActiveUserWarehouseTableWithRecall = eventActiveUserWarehouseTable.join(userRecallVideoData, Seq("userId"))

    getWhiteEventForUser(eventActiveUserWarehouseTableWithRecall, outputPath
                        ,broadcastVideoInfoM
                        ,broadcastControversialVideoSet
                        ,broadcastEventTobeExaminedSet
                        ,broadcastSameArtistVideoNum
                        ,broadcastSameKeywordVideoNum
                        ,broadcastSameCategory2elvelVideoNum
                        ,broadcastMaxVideoNum
                        )
  }

  def getWhiteEventForUser(userInfoDF:DataFrame, outputPath:String
                           ,broadcastVideoInfoM:Broadcast[mutable.HashMap[Long, Seq[Any]]]
                           ,broadcastControversialVideoSet:Broadcast[mutable.Set[String]]
                           ,broadcastVideoTobeExaminedSet:Broadcast[mutable.Set[String]]
                           ,broadcastSameArtistVideoNum:Broadcast[Int]
                           ,broadcastSameKeywordVideoNum:Broadcast[Int]
                           ,broadcastSameCategory2elvelVideoNum:Broadcast[Int]
                           ,broadcastMaxVideoNum:Broadcast[Int]
                           ) = {
    
    val recallVideos = userInfoDF("videos")
    val recedVideos = userInfoDF("recedVideos")
    val subArtists = userInfoDF("subArtists")
    val followingCreators = userInfoDF("followingCreators")
    val userId = userInfoDF("userId")

    val recResultTable = {
        userInfoDF
          .withColumn("whiteVideosNew", getWhiteVideosForActiveUsersUsingRecall(broadcastVideoInfoM
                                                                               ,broadcastControversialVideoSet
                                                                               ,broadcastVideoTobeExaminedSet
                                                                               ,broadcastSameArtistVideoNum
                                                                               ,broadcastSameKeywordVideoNum
                                                                               ,broadcastSameCategory2elvelVideoNum
                                                                               ,broadcastMaxVideoNum
                                                                               )(recallVideos, recedVideos,subArtists, followingCreators, userId)
                     )
          .select("userId", "whiteVideosNew")
    }
    val resultCol = recResultTable("whiteVideosNew")
    val finalResult = recResultTable.filter(resultCol =!= "null")

    finalResult.write.option("sep", "\t").csv(outputPath)
  }
}
