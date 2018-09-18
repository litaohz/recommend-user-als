package com.netease.music.recommend.scala.feedflow.utils

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MergeSmallParquetFiles {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("input", true, "input directory")
    options.addOption("numPartitions", true, "numPartitions")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val input = cmd.getOptionValue("input")
    val numPartitions = cmd.getOptionValue("numPartitions").toInt
    val output = cmd.getOptionValue("output")

    val dataset = spark.read.parquet(input)
    dataset
      .repartition(numPartitions)
      .write.parquet(output)
  }
}
