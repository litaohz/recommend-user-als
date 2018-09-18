package com.netease.music.recommend.scala.feedflow.hot

import java.io.InputStream

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetHotVideoForFeatureExamine {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("videoPoolInput", true, "input directory")
    options.addOption("numToExamine", true, "numToExamine")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoPoolInput = cmd.getOptionValue("videoPoolInput")
    val numToExamine = cmd.getOptionValue("numToExamine").toInt
    val outputPath = cmd.getOptionValue("output")

    val stream:InputStream = getClass.getResourceAsStream("/excludeVideoIds")
    val excludeVideoSet = scala.collection.mutable.HashSet[Long]()
    scala.io.Source.fromInputStream(stream).getLines.foreach {line =>
      excludeVideoSet.add(line.toLong)
    }
    val broadcastExcludedVideoSet = spark.sparkContext.broadcast(excludeVideoSet)

    import spark.implicits._

    val videoExaminePool = spark.read.parquet(videoPoolInput)
      .filter($"vType"==="video")
      .filter($"score">=500.0)
      .filter(!$"videoId".isin(broadcastExcludedVideoSet.value.toSeq : _*))
      .orderBy($"rawPrediction4newUser".desc)
      .limit(numToExamine)
      .select("videoId", "vType", "score", "features", "scaledFeatures", "rawPrediction4newUser", "smoothClickrate4newUser")

    videoExaminePool.write.parquet(outputPath + "/parquet")
    videoExaminePool
      .orderBy($"score".desc)
      .select("videoId", "vType", "score", "smoothClickrate4newUser")
      .write.option("sep", "\t").csv(outputPath + "/csv")
  }
}
