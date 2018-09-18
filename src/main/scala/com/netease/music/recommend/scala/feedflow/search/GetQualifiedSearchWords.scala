package com.netease.music.recommend.scala.feedflow.search

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created by hzzhangjunfei1 on 2017/11/15.
  */
object GetQualifiedSearchWords {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("hotSearchWord", true, "input directory")
    options.addOption("crawlerWord", true, "input directory")

    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val hotSearchWordInput = cmd.getOptionValue("hotSearchWord")
    val crawlerWordInput = cmd.getOptionValue("crawlerWord")

    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val hotSearchWordTable = spark.sparkContext.textFile(hotSearchWordInput)
      .map {line =>
        val info = line.split("#")
        val keyword = info(0)
        keyword
      }.toDF("keyword")

    val crawlerWordTable = spark.sparkContext.textFile(crawlerWordInput)
      .toDF("keyword")

    val hotwordTable = hotSearchWordTable
      .union(crawlerWordTable)
      .groupBy("keyword")
      .agg(count("keyword"))
      .select("keyword")

    hotwordTable.write.csv(outputPath)

  }



}
