package com.netease.music.recommend.scala.feedflow.userbased

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.netease.music.recommend.scala.feedflow.utils.SchemaObject._

object UserRowGenerator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("forwardInput", true, "forward itembased input");
    options.addOption("eventFeatureInput", true, "event feature input");
    options.addOption("forwardOutput", true, "forward row output");

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val forwardInputDir = cmd.getOptionValue("forwardInput");
    val eventFeatureInputDir = cmd.getOptionValue("eventFeatureInput");
    val forwardOutputDir = cmd.getOptionValue("forwardOutput");

    import spark.implicits._

    val forwardThread = 0.02d
    val forwardOutput = spark.read.option("sep", "\t")
      .schema(similarOutputSchema)
      .csv(forwardInputDir)
    val betterForwardOutput = forwardOutput
      .filter($"score">forwardThread)
  }
}
