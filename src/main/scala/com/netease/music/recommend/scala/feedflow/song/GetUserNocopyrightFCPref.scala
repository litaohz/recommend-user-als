package com.netease.music.recommend.scala.feedflow.song

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by hzzhangjunfei1 on 2017/8/17.
  */
object GetUserNocopyrightFCPref {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("noCopyrightSong", true, "noCopyrightSong input directory")
    options.addOption("user_profile", true, "user_profile input directory")
    options.addOption("fanchangInput", true, "fanchangInput input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val noCopyrightSong = cmd.getOptionValue("noCopyrightSong")
    val user_profile = cmd.getOptionValue("user_profile")
    val fanchangInput = cmd.getOptionValue("fanchangInput")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val noCopyrightSongS = spark.sparkContext.textFile(noCopyrightSong)
      .map {line =>
        val info = line.split("\t")
        val songId = info(0).toLong
        val albumId = info(1).toLong
        songId
      }
      .collect()
      .toSet

    val broadcaseNocpoyrightSongS = spark.sparkContext.broadcast(noCopyrightSongS)

    val fanchangTable = spark.sparkContext.textFile(fanchangInput)
      .filter {line =>
        val info = line.split("\t")
        val songId = info(0).toLong
        if (broadcaseNocpoyrightSongS.value.contains(songId))
          true
        else
          false
      }
      .map {line =>
        val info = line.split("\t")
        val songId = info(0).toLong
        var fcSongId = 0l
        val sameArtistFCSongIds = info(1)
        if (!sameArtistFCSongIds.equals("null")) {
          for (sid <- sameArtistFCSongIds.split(",")) {
            if (fcSongId == 0l && !broadcaseNocpoyrightSongS.value.contains(sid.toLong))
              fcSongId = sid.toLong
          }
        }
        val otherFCSongIds = info(2)
        if (fcSongId == 0 && !otherFCSongIds.equals("null")) {
          for (sid <- otherFCSongIds.split(",")) {
            if (fcSongId == 0l && !broadcaseNocpoyrightSongS.value.contains(sid.toLong))
              fcSongId = sid.toLong
          }
        }
        (songId, fcSongId)
      }
//      .toDF("songId", "fcSongId")
//      .filter($"fcSongId">0)
    val broadcastFanchangM = spark.sparkContext.broadcast(
      fanchangTable.collect.toMap
    )

    val user_profileTable = spark.sparkContext.textFile(user_profile)
      .map {line =>
        val info = line.split("\t")
        val userId = info(0).toLong
        val prefs = info(1).split(",")
        val fcPrefBuffer = collection.mutable.ArrayBuffer[String]()
        for(prefInfoStr <- prefs) {
          val prefInfo = prefInfoStr.split(":")
          val songId = prefInfo(0).toLong
          val fcSongId = broadcastFanchangM.value.getOrElse(songId, 0l)
          if (fcSongId > 0) {
            val pref = prefInfo(1)
            fcPrefBuffer.append(fcSongId + ":" + pref)
          }
        }
        if (fcPrefBuffer.size > 0)
          userId + "\t" + fcPrefBuffer.mkString(",")
        else
          "null"
      }
      .filter(!_.equals("null"))

    user_profileTable.saveAsTextFile(output)
  }


}
