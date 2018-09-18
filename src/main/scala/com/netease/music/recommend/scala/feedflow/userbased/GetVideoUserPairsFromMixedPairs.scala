package com.netease.music.recommend.scala.feedflow.userbased

import com.netease.music.recommend.scala.feedflow.cf.mixedId2OriginalId
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetVideoUserPairsFromMixedPairs {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("mixedUserVideoPref_days", true, "input directory")
    options.addOption("index", true, "input directory")
    options.addOption("minSupport", true, "minSupport")
    options.addOption("maxPrefPerUser", true, "maxPrefPerUser")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val mixedUserVideoPref_days = cmd.getOptionValue("mixedUserVideoPref_days")
    val index = cmd.getOptionValue("index")
    val minSupport = cmd.getOptionValue("minSupport").toInt
    val maxPrefPerUser = cmd.getOptionValue("maxPrefPerUser").toInt
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._
    // key  : originalId
    // value: mixedId
    val indexM = spark.sparkContext.textFile(index)
      .map{line =>
        val info = line.split("\t")
        (info(1), info(0))
      }
      .collect
      .toMap[String, String]
    // key   : mixedId
    // value : originalId
    val broatcast_index = spark.sparkContext.broadcast[Map[String, String]](indexM)

    val userVideoPrefTable = spark.read.option("sep", "\t")
      .csv(mixedUserVideoPref_days)
      .toDF("userId", "mixedId", "pref")
      .withColumn("originalId", mixedId2OriginalId(broatcast_index)($"mixedId"))
      .filter($"originalId".endsWith("-video"))   // 只保留video相关偏好
      .select("mixedId", "userId", "pref")

    userVideoPrefTable.write.option("sep", ",").csv(outputPath)

  }
}
