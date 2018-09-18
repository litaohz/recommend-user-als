package com.netease.music.recommend.scala.feedflow.userbased

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetVideoUserPairs {

  case class VideoUserPrefPair(videoId:Long, userId:Long, pref:Int)
  case class UserPrefInfo(userId:Long, prefNum:Int, prefTotal:Float, rawLine:String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("userVideoPref_days", true, "input directory")
    options.addOption("minSupport", true, "minSupport")
    options.addOption("userNum", true, "userNum")
    options.addOption("maxPrefPerUser", true, "maxPrefPerUser")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userVideoPref_daysInput = cmd.getOptionValue("userVideoPref_days")
    val minSupport = cmd.getOptionValue("minSupport").toInt
    val userNum = cmd.getOptionValue("userNum").toInt
    val maxPrefPerUser = cmd.getOptionValue("maxPrefPerUser").toInt
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val userVideoPrefTable = spark.sparkContext.textFile(userVideoPref_daysInput)
      .map {line =>
        val info = line.split("\t")
        val userId = info(0).toLong
        val videoPrefCnt = info(1).split(",").length
        var prefTotal = 0.0f
        for (videoInfoStr <- info(1).split(",")) yield {
          val pref = videoInfoStr.split(":")(0).toFloat
          prefTotal += pref
        }
        UserPrefInfo(userId, videoPrefCnt, prefTotal, line)
      }.toDF
      .filter($"prefNum" >= minSupport)
//      .orderBy($"prefNum".desc, $"prefTotal".desc)
//      .limit(userNum)
      .select("rawLine")
      .rdd
      .map{line => line.getString(0)}
//    val userVideoPrefTable = spark.sparkContext.textFile(userVideoPref_daysInput)
      .flatMap {line =>
        val info = line.split("\t")
        val userId = info(0).toLong
        var prefCnt = 0
//        val videoPrefCnt = info(1).split(",").length
//        if (videoPrefCnt >= minSupport) {
          for (videoInfoStr <- info(1).split(",")) yield {
            if (prefCnt < maxPrefPerUser) {
              val videoId = videoInfoStr.split(":")(0).toLong
              prefCnt += 1
              VideoUserPrefPair(videoId, userId, 5)
            } else {
              VideoUserPrefPair(-1, userId, -1)
            }
          }
//        } else {
//          Seq(VideoUserPrefPair(-1, userId, -1))
//        }
      }
      .toDF
      .filter($"videoId">0)

    userVideoPrefTable.write.option("sep", ",").csv(outputPath)

  }
}
