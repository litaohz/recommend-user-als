package com.netease.music.recommend.scala.feedflow.videoProfile

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetVideoHotScore {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoActionInfoNdays", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoActionInfoNdaysInput = cmd.getOptionValue("videoActionInfoNdays")
    val outputPath = cmd.getOptionValue("output")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    import spark.implicits._

    val existsInputPaths = getExistsPath(videoActionInfoNdaysInput, fs)
    logger.warn("existing input path:\t" + existsInputPaths.mkString(","))
    val videoActionInfoNdaysTable = spark.read.parquet(existsInputPaths.toSeq : _*)
      .cache

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val nowMiliSeconds = System.currentTimeMillis
    var validDates = for (i <- 1 to 3) yield {
      sdf.format(new Date(nowMiliSeconds - i * 3600 * 24 * 1000))
    }
    val videoHotScore3days = videoActionInfoNdaysTable
      .filter($"date".isin(validDates : _*))
      .groupBy("videoId", "vType")
      .agg(sum("hotScore1day").as("hotScore3days"))

    validDates = for (i <- 1 to 7) yield {
      sdf.format(new Date(nowMiliSeconds - i * 3600 * 24 * 1000))
    }
    val videoHotScore7days = videoActionInfoNdaysTable
      .filter($"date".isin(validDates : _*))
      .groupBy("videoId", "vType")
      .agg(sum("hotScore1day").as("hotScore7days"))

    validDates = for (i <- 1 to 30) yield {
      sdf.format(new Date(nowMiliSeconds - i * 3600 * 24 * 1000))
    }
    val videoHotScore30days = videoActionInfoNdaysTable
      .filter($"date".isin(validDates : _*))
      .groupBy("videoId", "vType")
      .agg(sum("hotScore1day").as("hotScore30days"))

    validDates = for (i <- 1 to 90) yield {
      sdf.format(new Date(nowMiliSeconds - i * 3600 * 24 * 1000))
    }
    val videoHotScore90days = videoActionInfoNdaysTable
      .filter($"date".isin(validDates : _*))
      .groupBy("videoId", "vType")
      .agg(sum("hotScore1day").as("hotScore90days"))

    val videoHotScores = videoHotScore3days
      .join(videoHotScore7days, Seq("videoId", "vType"), "outer")
      .join(videoHotScore30days, Seq("videoId", "vType"), "outer")
      .join(videoHotScore90days, Seq("videoId", "vType"), "outer")
      .na.fill(0, Seq("hotScore3days", "hotScore7days", "hotScore30days", "hotScore90days"))
      .orderBy($"hotScore90days".desc)

    videoHotScores.repartition(1).write.parquet(outputPath + "/parquet")
    videoHotScores.repartition(1).write.option("sep", "\t").csv(outputPath + "/csv")
  }

  def getExistsPath(inputPaths:String, fs:FileSystem):List[String] = {
    var list = List[String]()
    for (input <- inputPaths.split(",")) {
      if (fs.exists(new Path(input))) {
        list = list :+ input
      }
    }
    list
    //    if (set.size > 0)
    //      set.mkString(",")
    //    else
    //      "null"
  }
}
