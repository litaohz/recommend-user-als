package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object MixedIdConverter {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("inputPairs", true, "input directory")
    options.addOption("index", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val inputPairs = cmd.getOptionValue("inputPairs")
    val index = cmd.getOptionValue("index")
    val output = cmd.getOptionValue("output")

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
    val broatcast_index = spark.sparkContext.broadcast(indexM)


    val finalTable = spark.read.option("sep", "\t")
      .csv(inputPairs)
      .toDF("id0", "id1", "simScore")
      .withColumn("mixedId0", mixedId2OriginalId(broatcast_index)($"id0"))
      .withColumn("mixedId1", mixedId2OriginalId(broatcast_index)($"id1"))
      .select("mixedId0", "mixedId1", "simScore")
    finalTable
      .write.option("sep", "\t").csv(output)
  }
}
