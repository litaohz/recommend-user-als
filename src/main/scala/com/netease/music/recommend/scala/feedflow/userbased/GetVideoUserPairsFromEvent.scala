package com.netease.music.recommend.scala.feedflow.userbased

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetVideoUserPairsFromEvent {

  case class EventUserPrefPair(eventId:Long, userId:Long, pref:Int)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("userItemPairs", true, "input directory")
    options.addOption("minSupport", true, "minSupport")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userItemPairsInput = cmd.getOptionValue("userItemPairs")
    val minSupport = cmd.getOptionValue("minSupport").toInt
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val userVideoPrefTable = spark.sparkContext.textFile(userItemPairsInput)
      .map {line =>
        val info = line.split(",")
        val userId = info(0).toLong
        val eventId = info(1).toLong
        EventUserPrefPair(eventId, userId, 5)

      }
      .toDF

    val usefulUsersTable = userVideoPrefTable
      .groupBy($"userId")
      .agg(count($"eventId").as("cnt"))
      .filter($"cnt">minSupport)
      .select("userId")

    val finalData = userVideoPrefTable
      .join(usefulUsersTable, Seq("userId"), "inner")
      .select("eventId", "userId", "pref")

    finalData.write.option("sep", ",").csv(outputPath)

  }
}
