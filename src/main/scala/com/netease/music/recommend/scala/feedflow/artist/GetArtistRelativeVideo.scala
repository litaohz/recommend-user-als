package com.netease.music.recommend.scala.feedflow.artist

import com.netease.music.recommend.scala.feedflow.RealtimeAlgConstant
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hzzhangjunfei1 on 2017/8/17.
  */
object GetArtistRelativeVideo {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("videoInfoPoolInput", true, "input directory")
    options.addOption("output", true, "output directory")
    options.addOption("outputOnline", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val output = cmd.getOptionValue("output")
    val outputOnline = cmd.getOptionValue("outputOnline")

    import spark.implicits._

    val videoInfoPoolTable = spark.read.parquet(videoInfoPoolInput)

    // 获取artist相关联的动态
    val artistRelativeEventRDD = videoInfoPoolTable
      .filter($"artistIds"=!="0")
      .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
          ,"bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial"
          ,"rawPrediction", "isMusicVideo")
      .withColumn("artistIdForRec", explode(splitByDelim("_tab_")($"artistIds")))
      .rdd

    val offlineDataRDD = artistRelativeEventRDD
      .map(line => {
        (line.getString(12), line.mkString(":"))
      })
      .groupByKey
      .map({case (key, values) => collectVideoByKey(key, values)})
    offlineDataRDD.saveAsTextFile(output)

    // 获取艺人相关视频 4 实时推荐
    val onlinelineDataRDD = artistRelativeEventRDD
      .map{line =>
        (line.getString(12), OnlineVideoInfo(line.getLong(0), line.getString(1), RealtimeAlgConstant.ARTIST, line.getDouble(10), 0))
      }
      .groupByKey
      .map({case (key, values) => collectOnlineVideoByKey(key, values)})
    onlinelineDataRDD.saveAsTextFile(outputOnline)

  }


}
