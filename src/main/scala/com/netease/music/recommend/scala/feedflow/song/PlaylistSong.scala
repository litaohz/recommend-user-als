package com.netease.music.recommend.scala.feedflow.song

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable
/**
  * Created by hzlvqiang on 2018/4/15.
  */
object PlaylistSong {

  def getSongSeq = udf((songidPositions: Seq[String], userId: Long) => {
    val result = mutable.ArrayBuffer[String]()
    val songidPositionTuples = mutable.ArrayBuffer[(String, Int)]()
    for (spStr <- songidPositions) {
      val sps = spStr.split("\t")
      songidPositionTuples.append((sps(0), sps(1).toInt))
    }
    for( (sortedSid, sortedPos) <- songidPositionTuples.sortWith(_._2 < _._2)) {
      result.append(sortedSid.toString)
    }
    val res = result.mkString(" ")
    if (userId == 359792224) {
      println("359792224 playlist")
      println(res)
    }
    res
  })

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("Music_Playlist_BookedCount", true, "input directory")
    options.addOption("Music_TrackOfPlaylist_All", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)
    import spark.implicits._

    val playlistData = spark.read.textFile(cmd.getOptionValue("Music_Playlist_BookedCount")).repartition(1000)
      .map(line => {
        // Id,UserId,BookedCount,tags,TrackCount,coverStatus,playCount,CreateTime
        var res = (0L, 0L, 0, 0, 0, 0L)
        val ts = line.toString.split("\t")

        val playlistid = ts(0).toLong
        val userId = ts(1).toLong
        val bookCnt = ts(2).toInt
        val trackCnt = ts(4).toInt
        val playCnt = ts(6).toInt
        val createtime = ts(7).toLong
        res = (playlistid, userId, bookCnt, trackCnt, playCnt, createtime)
        res
      }).toDF("playlistId", "userId", "bookCnt", "trackCnt", "playCnt", "createtime")
      .filter($"playlistId" =!= 0)
    println("playlistData")
    playlistData.show(10, false)

    val playlistSongData = spark.read.textFile(cmd.getOptionValue("Music_TrackOfPlaylist_All")).repartition(1000)
      .map(line => {
        //// Id,TrackId(songid),PlaylistId,UserId,Position,AddTime
        val ts = line.toString.split("\t")
        var res =  (0L, "")
        if (ts.length >= 6) {
          res = (ts(2).toLong, ts(1) + "\t" + ts(4))
        }
        res
    }).toDF("playlistId", "songId_position")
      .filter($"playlistId" =!= 0L)
      .groupBy($"playlistId").agg(collect_list($"songId_position").as("songId_position_list"))

    println("playlistSongData")
    playlistSongData.show(5, false)
    /* data 1
    val outData = playlistSongData
        .join(playlistData, Seq("playlistId"), "left")
        .na.fill(0, Seq("bookCnt", "trackCnt", "playCnt"))
      .filter($"bookCnt" >= 5)
      .filter($"trackCnt" >= 3)
      .filter($"trackCnt" < 1000)
      .filter($"playCnt" >= 50)
      .withColumn("songSeq", getSongSeq($"songId_position_list", $"userId"))
      */
    val outData = playlistSongData
      .join(playlistData, Seq("playlistId"), "left")
      .na.fill(0, Seq("bookCnt", "trackCnt", "playCnt"))
      .filter($"bookCnt" >= 0)
      .filter($"trackCnt" >= 3)
      .filter($"trackCnt" < 500)
      .filter($"playCnt" >= 2)
      .withColumn("songSeq", getSongSeq($"songId_position_list", $"userId"))

    outData.select("songSeq").write.text(cmd.getOptionValue("output"))


  }
}
