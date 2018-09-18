package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SongItemBasedSimWithPlayTimes {

  def main(args: Array[String]): Unit = {

    val options = new Options
    options.addOption("input", true, "input directory, feed flow music pref mixed video pref")
    options.addOption("sampleFactor", true, "sampleFactor")
    //options.addOption("partitionNum", true, "partitionNum")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val input = cmd.getOptionValue("input")
    val sampleFactor = cmd.getOptionValue("sampleFactor").toFloat
    //val partitionNum = cmd.getOptionValue("partitionNum").toInt
    val output = cmd.getOptionValue("output")
    val maxSizeSimItem = 1200

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    import spark.implicits._

    val itemNum = spark.sparkContext.textFile(input)
      .map{_.split("\t").last}
      .flatMap{_.split(",").map(_.split(":").head)}
      .map{x => (x, 1)}
      .reduceByKey(_ + _)
      .toDF("songId", "sumSongNum")

    val itemNum1 = itemNum
      .withColumnRenamed("songId", "songId1")
      .withColumnRenamed("sumSongNum", "sumSongNum1")

    val userListen = spark.sparkContext.textFile(input)
      .map{_.split("\t")}
      .flatMap{x =>
        val userId = x(0)
        val songs = x.last.split(",").map{a => a.split(":")}.map{a => (userId, a(0), a(1).toInt)}.take(300)
        songs
      }.toDF("userId", "songId", "songListenNum")

    val leftUserListen = userListen.join(itemNum, "songId")

    val rightUserListen = userListen.sample(false, sampleFactor)
      .withColumnRenamed("songId", "songId1")
      .withColumnRenamed("songListenNum", "songListenNum1")
      .join(itemNum1, "songId1")

    val sortedScoreWindow = Window.partitionBy("songId").orderBy($"simScore".desc)
    val result = leftUserListen.join(rightUserListen, "userId")
      .filter($"songId" =!= $"songId1")
      .select("songId", "songId1", "songListenNum", "songListenNum1", "sumSongNum", "sumSongNum1")
      .withColumn("simScore", getItembasedSimScore($"songListenNum", $"songListenNum1", $"sumSongNum", $"sumSongNum1"))
      .groupBy("songId", "songId1")
      .agg(sum($"simScore").as("simScore"))
      .withColumn("rank", row_number().over(sortedScoreWindow))
      .where($"rank" <= maxSizeSimItem)
      .select("songId", "songId1", "simScore")

    result.write.option("sep", "\t").csv(output)
  }

  def getScore(times1:Int, times2:Int):Float = {
    val socre = times2.toFloat / times1.toFloat
    if (times2 == times1 && times1 == 1)
      return 0.3f

    if (socre < 1)
      socre
    else
      1.0f / socre
  }

  def getItembasedSimScore = udf((songListenNum:Int, songListenNum1:Int, sumSongNum:Int, sumSongNum1:Int) => {
    getScore(songListenNum, songListenNum1) / math.sqrt(sumSongNum.toFloat * sumSongNum1.toFloat).toFloat
  })
}
