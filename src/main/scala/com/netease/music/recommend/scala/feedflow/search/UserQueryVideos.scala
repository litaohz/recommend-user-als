package com.netease.music.recommend.scala.feedflow.search

import jdk.nashorn.internal.parser.JSONParser
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql.functions._
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.util.parsing.json.{JSON, JSONObject}

/**
  * 用户搜索点击日志
  */
object UserQueryVideos {

  def getPlays = udf((query:String, props: String) => {
    // "props":{"31750711":{"impress":1,"pos":28,"impress_uv":1},
    // "59177680":{"log_time":1529558229000,"play_uv":1,"play":1,"play_inter":1,"click":1,"impress":1,"pos":12,"impress_uv":1,"click_uv":1,"play_inter_uv":1}
    val videoList = parse(props)
    val res = mutable.ArrayBuffer[String]()
    res.append(query)
    res.append(" ")
    res.append(query.replaceAll(" ", ""))
    for (vidAction <- videoList.values.asInstanceOf[Map[String, JValue]]) {
      val vid = vidAction._1.substring(0, vidAction._1.length-1);
      val action = vidAction._2.asInstanceOf[Map[String, JValue]]
      val playNum = action.getOrElse("play", 0L).toString.toLong
      val playEndNum = action.getOrElse("play_end", 0L).toString.toLong
      if (playNum > 0L) {
        res.append(vid)
      }
    }
    res.mkString(" ")
  })

  def toContext = udf((queryPlayVideos: Seq[String]) => {
    queryPlayVideos.mkString(" ")
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("input", true, "input directory")

    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val hotSearchWordTable = spark.read.json(cmd.getOptionValue("input"))
      .withColumn("userid", $"userid")
      .withColumn("queryPlayVideos", getPlays($"query", $"props"))
      .groupBy($"userid")
      .agg(collect_list($"queryPlayVideos").as("queryPlayVideosAGG"))
      .withColumn("context", toContext($"queryPlayVideosAGG"))
        .repartition(200).write.text(cmd.getOptionValue("output"))

  }



}
