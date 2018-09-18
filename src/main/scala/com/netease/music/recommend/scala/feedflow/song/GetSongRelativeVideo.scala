package com.netease.music.recommend.scala.feedflow.song

import com.netease.music.recommend.scala.feedflow.RealtimeAlgConstant
import com.netease.music.recommend.scala.feedflow.artist.OnlineVideoInfo
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hzzhangjunfei1 on 2017/8/17.
  */
object GetSongRelativeVideo {

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

    // 获取song相关联的动态
    val songRelativeEventRDD = videoInfoPoolTable
      .filter($"bgmIds"=!="0")
      .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
        ,"bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial"
        ,"rawPrediction", "isMusicVideo")
      .withColumn("songIdForRec", explode(splitByDelim("_tab_")($"bgmIds")))
      .rdd

    val offlineDataRDD = songRelativeEventRDD
      .map(line => {
        (line.getString(12), line.mkString(":"))
      })
      .groupByKey
      .map({case (key, values) => collectVideoByKey(key, values)})
    offlineDataRDD.saveAsTextFile(output)

    val onlineDataRDD = songRelativeEventRDD
      .map(line => {
        (line.getString(12), OnlineVideoInfo(line.getLong(0), line.getString(1), RealtimeAlgConstant.SONG, line.getDouble(10), 0))
      })
      .groupByKey
      .map({case (key, values) => collectOnlineVideoByKey(key, values)})
    onlineDataRDD.saveAsTextFile(outputOnline)
  }


}
