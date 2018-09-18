package com.netease.music.recommend.scala.feedflow.tag

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random
import org.apache.spark.sql.functions._
/**
  * 合并偏好
  */
object MixPrefJoin {

  def getEventVideoM(lines: Array[String]) = {
    val eventVideoM = mutable.HashMap[String, String]()
    for (line <- lines) {
      val ts = line.split("\01", -1)
      val event = ts(1)
      val video = ts(3)
      eventVideoM.put(event, video)
    }
    eventVideoM
  }

  def appendAll(itemMap: mutable.HashMap[String, Double], prefItems: mutable.WrappedArray[String], pref: Double) = {
    if (prefItems != null && prefItems != None) {
      for (item <- prefItems) {
        if (item != null) {
          itemMap.put(item, itemMap.getOrElse(item, 0.0) + pref)
        }
      }
    }
  }

  def joinIdType = udf((vid: String, vtype: String) => {
    vid + "-" + vtype
  })

  def toLong = udf((idstr: String) => {
    idstr.toLong
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("user_pref_event", true, "user_pref_event")
    options.addOption("user_pref_video", true, "user_pref_video")

    options.addOption("user_song_from_playlist", true, "user_song_from_playlist")
    options.addOption("searchProfile_days", true, "searchProfile_days")
    options.addOption("user_local_song", true, "user_local_song")

    options.addOption("vid_uid_pos", true, "vid_uid_pos")

    options.addOption("Music_EventVideoRelation", true, "Music_EventVideoRelation")

    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)
    // 动态偏好 >= 2
    val eventVideoM = getEventVideoM(spark.read.textFile(cmd.getOptionValue("Music_EventVideoRelation")).collect())
    println("eventVideoM size:" + eventVideoM.size)
    val eventVideoMBroad = sc.broadcast(eventVideoM)
    val user_pref_event = cmd.getOptionValue("user_pref_event")
    println("user_pref_event:" + user_pref_event)
    val userVidPrefFromEvent = spark.read.textFile(user_pref_event).map(line => {
      val ueprefs = line.split("\t", 4)
      var uid = -1L
      var prefVideo = ""
      if (ueprefs.size >= 3) {
        uid = ueprefs(0).toLong
        if (uid < 0) {
          uid = -uid
        }
        val eid = ueprefs(1)
        val vid = eventVideoMBroad.value.getOrElse(eid, null)
        val pref = ueprefs(2).toDouble
        if (vid != null && pref >= 2) {
          // (uid, vid + "-video", pref)
          prefVideo = vid + "-video"
        } else {
          uid = -1
        }
      }
      (uid, prefVideo)
    }).toDF("uid", "vidtypeFromEvent")
      .filter($"uid" =!= -1)
      .groupBy($"uid").agg(
      collect_list("vidtypeFromEvent").as("vidtypeFromEventList")
    ).select("uid", "vidtypeFromEventList")

    //println("userVidPrefFromEvent cnt:" + userVidPrefFromEvent.count())
    //userVidPrefFromEvent.show(10, false)

    // 视频偏好 >= 4
    val userVidPref = spark.read.parquet(cmd.getOptionValue("user_pref_video"))
      .filter($"videoPref" >= 5 || $"subscribeCnt"> 0 || $"shareClickCnt" > 0 || $"zanCnt" > 0)
        .withColumn("vidtype", joinIdType($"videoId", $"vType"))
        .withColumn("uid", toLong($"actionUserId"))
      .groupBy($"uid").agg(
        collect_list("vidtype").as("vidtypeList")
      ).select("uid", "vidtypeList")
    //println("userVidPref cnt:" + userVidPref.count())
    //userVidPref.show(10, false)
    // 歌曲
    /* TODO
      val userSongFromPlaylistData = spark.read.textFile(cmd.getOptionValue("Music_TrackOfPlaylist"))
      .map(line => {
        // Id,TrackId(songid),PlaylistId,UserId,Position,AddTime
        val ts = line.split("\t")
        var uid = ts(3).toLong
        if (uid < 0) {
          uid = -uid
        }
        if (uid == 359792224) {
          println("playlist for 359792224:" + line)
        }
        (ts(3), ts(1))
      }).toDF("uid", "songid").groupBy("uid")
      .agg(
      collect_list("songid").as("songidsFromPlaylist")
    )
     */
    // 目前用历史数据
    val userSongFromPlaylistData = spark.read.textFile(cmd.getOptionValue("user_song_from_playlist"))
      .map(line => {
        // uid soingid.songid,...
        val ts = line.split("\t")
        val samples = mutable.ArrayBuffer[String]()
        val random = new Random()
        var uid = ts(0).toLong
        for (sid <- ts(1).split(",")) {
          if (samples.length < 1000) {
            samples.append(sid)
          } else {
            //println("songids > 500:" + uid.toString)
            samples.remove(random.nextInt(900), 100) // 随机删除100
          }
        }
        if (uid < 0) {
          uid = -uid
        }
        (uid, samples)
      }).toDF("uid", "songidsPlaylist")
    //println("userSongFromPlaylistData cnt:" + userSongFromPlaylistData.count())
    //userSongFromPlaylistData.show(10, false)

    println("Load user search song data...")
    val userSearchSongData = spark.read.textFile(cmd.getOptionValue("searchProfile_days"))
      .map(line => {
        val uidsongids = mutable.ArrayBuffer[Tuple2[Long, Long]]()
        var uid = 0L
        // 100001799	371362:4.0,400689263:4.0,512359367:1.0
        val ts = line.split("\t")
        if (ts.length >= 2) {
          uid = ts(0).toLong
          if (uid < 0) {
            uid = -uid
          }
        }
        val random = new Random()
        val samples = mutable.ArrayBuffer[String]()
        for (idwt <- ts(1).split(",")) {
          if (samples.length < 1000) {
            samples.append(idwt.split(":")(0))
          } else {
            //println("songids > 500:" + uid.toString)
            samples.remove(random.nextInt(900), 100) // 随机删除100
          }
        }
        (uid, samples)
      }).toDF("uid", "songidsSearchList")
    //println("userSearchSongData cnt:" + userSearchSongData.count())
    //userSearchSongData.show(10, false)

    val userLocalSongData = spark.read.textFile(cmd.getOptionValue("user_local_song"))
      .map(line => {
        // uid soingid type
        val ts = line.split("\t")
        var uid = 0L
        var songid = 0L
        if (ts.length >= 3) {
          uid = ts(0).toLong
          songid = ts(1).toLong
          if (uid < 0) {
            uid = -uid
          }
          if (songid < 0) {
            songid = -songid
          }
        }
        (uid, songid.toString)
      }).toDF("uid", "songidsLocal")
      .groupBy($"uid").agg(
      collect_list("songidsLocal").as("songidsLocalList")
    ).select("uid", "songidsLocalList")
    //println("userLocalSongData cnt:" + userLocalSongData.count())
    //userLocalSongData.show(10, false)

    // 视频用户正向数据
    val userPosVideosPrefData = spark.read.textFile(cmd.getOptionValue("vid_uid_pos")).map(line => {
      val ts = line.split("\t")
      val idtype = ts(0) + "-video"
      var uid = ts(1).toLong
      if (uid < 0) {
        uid = -uid
      }
      (uid, idtype)
    }).toDF("uid", "vidtypePos")
      .groupBy($"uid").agg(
      collect_list("vidtypePos").as("vidtypePosList"))
      .select("uid", "vidtypePosList")
    //println("userPosVideosPrefData cnt:" + userPosVideosPrefData.count())
    //userPosVideosPrefData.show(10, false)

    val joinData = userPosVideosPrefData
      .join(userVidPrefFromEvent, Seq("uid"), "outer")
      .join(userVidPref, Seq("uid"), "outer")
      .join(userSongFromPlaylistData, Seq("uid"), "outer")
      .join(userSearchSongData, Seq("uid"), "outer")
      .join(userLocalSongData, Seq("uid"), "outer")
      .flatMap(row => {
        val uid = row.getAs[Long]("uid")
        val vidtypePosList = row.getAs[mutable.WrappedArray[String]]("vidtypePosList")
        val songidsLocalList = row.getAs[mutable.WrappedArray[String]]("songidsLocalList")
        val songidsSearchList = row.getAs[mutable.WrappedArray[String]]("songidsSearchList")
        val songidsPlaylist = row.getAs[mutable.WrappedArray[String]]("songidsPlaylist")
        val vidtypeList = row.getAs[mutable.WrappedArray[String]]("vidtypeList")
        val vidtypeFromEventList = row.getAs[mutable.WrappedArray[String]]("vidtypeFromEventList")

        val videoWtM = mutable.HashMap[String, Double]()
        val songWtM = mutable.HashMap[String, Double]()

        appendAll(videoWtM, vidtypeList, 1.0)
        appendAll(videoWtM, vidtypeFromEventList, 1.0)
        appendAll(videoWtM, vidtypePosList, 2.0)

        appendAll(songWtM, songidsLocalList, 2.0)
        appendAll(songWtM, songidsSearchList, 2.0)
        appendAll(songWtM, songidsPlaylist, 1.0)

        val sortedVideoWtList = videoWtM.toArray[(String, Double)].sortWith(_._2 > _._2)
        val sortedSongWtList = songWtM.toArray[(String, Double)].sortWith(_._2 > _._2)

        val result = mutable.ArrayBuffer[(String, String, Double)]()

        val uidStr = uid.toString
        val targetNumForVideo = 300
        val sortedVideoIter = sortedVideoWtList.iterator
        while (sortedVideoIter.hasNext && result.size < targetNumForVideo) {
          val (item, wt) = sortedVideoIter.next()
          result.append((uidStr, item, wt))
        }
        val targetNumForSong = result.size + 300
        val sortedSongIter = sortedSongWtList.iterator
        while (sortedSongIter.hasNext && result.size < targetNumForSong) {
          val (item, wt) = sortedSongIter.next()
          result.append((uidStr, item, wt))
        }
        if (uid == 359792224L) {
          println("uid=359792224L")
          for ((uidStr, item, wt) <- result) {
            println(uidStr + ", " + item + ", " + wt)
          }
        }
        result

      }).toDF("userIdStr", "idtype", "rating")

    val userIdMapping = joinData.select($"userIdStr").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("userIdStr"), line._2.toInt)
    }).toDF("userIdStr", "userId").cache()
    userIdMapping.write.parquet(cmd.getOptionValue("output") + "/userIdMapping")

    val itemMapping = joinData.select($"idtype").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("idtype"), line._2.toInt)
    }).toDF("idtype", "itemId").cache()
    itemMapping.write.parquet(cmd.getOptionValue("output") + "/itemMapping")

    val userItemRating = joinData
      .join(userIdMapping, Seq("userIdStr"), "left")
      .join(itemMapping, Seq("idtype"), "left")
        .select("userId", "itemId", "rating")
      .filter($"itemId".isNotNull && $"userId".isNotNull && $"rating".isNotNull)

    userItemRating.write.parquet(cmd.getOptionValue("output") + "/data")

  }

}