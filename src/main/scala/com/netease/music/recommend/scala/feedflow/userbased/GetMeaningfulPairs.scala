package com.netease.music.recommend.scala.feedflow.userbased

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.netease.music.recommend.scala.feedflow.utils.SchemaObject._
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._


object GetMeaningfulPairs {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("inputPairs", true, "input directory")
    options.addOption("simThread", true, "simThread")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val inputPairs = cmd.getOptionValue("inputPairs")
    val simThread = cmd.getOptionValue("simThread").toDouble
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val inputPairsTable = spark.read.option("sep", "\t")
      .schema(similarOutputSchema)
      .csv(inputPairs)
      .filter($"score">simThread)
      .write.option("sep", "\t").csv(output)



  }

}
