package com.netease.music.recommend.scala.feedflow.userProfile

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by hzzhangjunfei1 on 2017/8/17.
  */
object GetUserSongProfile {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("activeUserSongProfile", true, "input directory")
    options.addOption("simSong", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val activeUserSongProfileInput = cmd.getOptionValue("activeUserSongProfile")
    val simSongInput = cmd.getOptionValue("simSong")
    val output = cmd.getOptionValue("output")

    val activeUserSongProfileTable = spark.sparkContext.textFile(activeUserSongProfileInput)
      .map {line =>
        val info = line.split("\t")
        val userId = info(0).toLong
        val prefSongFromMusicInfo = info(1).split(",")
        val subSongInfo = info(2).split(",")
        val prefSongFromSearchInfo = info(3).split(",")
      }



  }


}
