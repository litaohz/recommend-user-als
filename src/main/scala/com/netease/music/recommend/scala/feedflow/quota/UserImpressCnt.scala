package com.netease.music.recommend.scala.feedflow.quota

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * 获取用户历史曝光videoId数量
  */
object UserImpressCnt {

  case class Impress(videoId:Long, vType:String, actionUserId:String, page:String, alg:String, os:String, impress:String)
  case class UserVideocnt(actionUserId:String, impressVideoCnt:Long)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("impress_lstday", true, "log input directory")
    options.addOption("user_videocnt_history", true, "log input directory")
    options.addOption("output", true, "output")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val impressInput = cmd.getOptionValue("impress_lstday")
    val userVideocntInput = cmd.getOptionValue("user_videocnt_history")

    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val userVideocntTableLstday = spark.sparkContext.textFile(impressInput)
      .map { line =>
        val info = line.split("\t")
        val impress = info(0)
        val page = info(1)
        val os = info(2)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val vType = info(6)
        val alg = info(8)
        Impress(videoId, getVType(vType), actionUserId, page, alg, os, impress)
      }.toDF
      .filter($"vType" === "video")
      .filter($"page" =!= "mvplay")
      .filter($"page" =!= "videoplay")
      .filter($"impress" === "impress")
      .groupBy("actionUserId")
      .agg(count("videoId").as("videoCnt"))
      .toDF("actionUserId", "videoCnt")

    var flushTable = userVideocntTableLstday

    if (userVideocntInput != null) {
      val userVideocntTableHistory = spark.sparkContext.textFile(userVideocntInput)
        .map { line =>
          val info = line.split(",")
          val uid = info(0)
          val vcnt = info(1).toLong
          UserVideocnt(uid, vcnt)
        }.toDF("actionUserId", "videoCntHistory")

      flushTable = userVideocntTableLstday.join(userVideocntTableHistory, Seq("actionUserId"), "outer")
        .na.fill(0, Seq("videoCnt", "videoCntHistory"))
        .withColumn("sumVideoCnt", add($"videoCnt", $"videoCntHistory"))
        .select("actionUserId", "sumVideoCnt")
        .toDF("actionUserId", "sumVideoCnt")

    }

    flushTable.write.csv(outputPath)

  }

  def getVType(vtype:String):String = {
    if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }

  def add = udf((impressCnt1:Long, impressCnt2:Long) => {
    impressCnt1 + impressCnt2
  })
}


