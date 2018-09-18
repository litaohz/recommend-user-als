package com.netease.music.recommend.scala.feedflow.mainpage

import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

/**
  * 从Music_TrackOfPlaylist_All提取用户偏好歌曲
  * Created by hzlvqiang on 2017/12/20.
  */
object GetSongFromTrackPlaylist {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf);
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("Music_TrackOfPlaylist", true, "Music_TrackOfPlaylist")

    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    val userPrefSidsData = spark.read.textFile(cmd.getOptionValue("Music_TrackOfPlaylist"))
      .map(line => {
        // Id,TrackId(songid),PlaylistId,UserId,Position,AddTime
        val ts = line.split("\t")
        if (ts(3).equals("359792224")) {
          println("playlist for 359792224:" + line)
        }
        (ts(3), ts(1))
      }).rdd.groupByKey.map({ case (key, values) =>
      val sids = values.mkString(",")
      if (key.equals("359792224")) {
        println("sids:" + sids)
      }
      println("")
      key + "\t" + sids
    })

    userPrefSidsData.repartition(100).saveAsTextFile(cmd.getOptionValue("output"))
  }
}
