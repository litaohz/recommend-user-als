package com.netease.music.recommend.scala.feedflow.itembased

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetUserItemPairsFromSongOrArt {

  case class UserItemPrefPair(userId:Long, itemId:Long, pref:Int)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("userVideoRelativeEntityPref_days", true, "input directory")
//    options.addOption("minSupport", true, "minSupport")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userVideoRelativeEntityPref_daysInput = cmd.getOptionValue("userVideoRelativeEntityPref_days")
//    val minSupport = cmd.getOptionValue("minSupport").toInt
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val userVideoRelativeEntityPref_daysTable = spark.sparkContext.textFile(userVideoRelativeEntityPref_daysInput)
      .flatMap {line =>
        val info = line.split("\t")
        val userId = info(0).toLong
        for (prefsStr <- info(1).split(",")) yield {
          val prefInfo = prefsStr.split(":")
          val itemId = prefInfo(0).toLong
          UserItemPrefPair(userId, itemId, 5)
        }
      }
      .toDF

    userVideoRelativeEntityPref_daysTable.write.option("sep", ",").csv(outputPath)

  }
}
