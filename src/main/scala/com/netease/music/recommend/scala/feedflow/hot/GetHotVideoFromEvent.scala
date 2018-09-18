package com.netease.music.recommend.scala.feedflow.hot

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetHotVideoFromEvent {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("videoRcmdMeta", true, "input directory")
    options.addOption("eventResourceInfoPool", true, "input directory")
    options.addOption("videoEventRelation", true, "input directory")
    options.addOption("musicEventCategories", true, "input directory")
    options.addOption("output", true, "output directory")
    options.addOption("outputNkv", true, "outputNkv directory")
    options.addOption("videoEventIndexOutput", true, "videoEventIndexOutput directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoRcmdMetaInput = cmd.getOptionValue("videoRcmdMeta")
    val eventResourceInfoPoolInput = cmd.getOptionValue("eventResourceInfoPool")
    val videoEventRelationInput = cmd.getOptionValue("videoEventRelation")
    val musicEventCategoriesInput = cmd.getOptionValue("musicEventCategories")
    val outputPath = cmd.getOptionValue("output")


  }
}
