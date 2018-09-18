package com.netease.music.recommend.scala.feedflow.log

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object FurtherLogProcess {

  case class UselessUser(actionUserId:Long, isUselessUser:Int)
  case class Impress(actionUserId:Long, vType:String, logStr:String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("impress", true, "log input directory")
    options.addOption("uselessUser", true, "uselessUser")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val impressInput = cmd.getOptionValue("impress")
    val uselessUserInput = cmd.getOptionValue("uselessUser")
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val usefulVType = Seq("mv", "video")
    val impressTable = spark.sparkContext.textFile(impressInput)
      .map {line =>
        val info = line.split("\t")
        val actionUserId = info(3).toLong
        val vType = info(6)
        Impress(actionUserId, vType, line)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .select("actionUserId", "logStr")

    val uselessUserTable = spark.sparkContext.textFile(uselessUserInput)
      .map {line =>
        val actionUserId = line.toLong
        UselessUser(actionUserId, 1)
      }
      .toDF

    val correctedImpressTable = impressTable
      .join(uselessUserTable, Seq("actionUserId"), "left_outer")
      .na.fill(0, Seq("isUselessUser"))
      .filter($"isUselessUser"===0)
      .select("logStr")

    correctedImpressTable.write.option("compression", "gzip").csv(outputPath + "/correct_impress")
  }
}
