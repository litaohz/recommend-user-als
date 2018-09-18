package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object GetUserMixedPref4CF_days_kwd {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("mixedOriginalPref", true, "input directory")
    options.addOption("videoTag", true, "input directory")
    options.addOption("output", true, "output directory")
    options.addOption("outputIndex", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val mixedOriginalPref = cmd.getOptionValue("mixedOriginalPref")
    val videoTag = cmd.getOptionValue("videoTag")

    val output = cmd.getOptionValue("output")
    val outputIndex = cmd.getOptionValue("outputIndex")

    import spark.implicits._
    val videoKwdM = getVideoKwdM(videoTag)
    val broad_videoKwdM = spark.sparkContext.broadcast(videoKwdM)

    val finalDataset = spark.read.textFile(mixedOriginalPref)
        .flatMap{line =>
          val info = line.split("\t")
          val userId = info(0)
          val idtype = info(1)
          val itemS = scala.collection.mutable.Set[String]()
          itemS += idtype
          if (broad_videoKwdM.value.contains(idtype)) {
            broad_videoKwdM.value(idtype).foreach(kwd => itemS += (kwd + "-kwd"))
          }
          itemS.map(item => (userId, item, 5))
        }
      .toDF("userId", "itemId", "pref")
      .cache

    finalDataset
      .repartition(16)
      .write.option("sep", "\t").csv(output + "/originalId")

    val windowSpec = Window.partitionBy("groupColumn").orderBy("groupColumn")
    val index = finalDataset
      .groupBy("itemId")
      .agg(count("itemId").as("cnt"))
      .withColumn("groupColumn", lit(1))
      .withColumn("mixedId", row_number() over(windowSpec))
      .select("itemId", "mixedId")
      .cache
    index.write.option("sep", "\t").csv(outputIndex)

    finalDataset
      .join(index, Seq("itemId"), "left_outer")
      .select("userId", "mixedId", "pref")
      .repartition(16)
      .write.option("sep", "\t").csv(output + "/mixedId")
  }



}
