package com.netease.music.recommend.scala.feedflow.song

import com.netease.music.recommend.scala.feedflow.cf.usefulCFRecalltypeS
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by hzzhangjunfei1 on 2018/3/16.
  */

// recalltype:
// 1: songCF
// 2: song
// 3: als
object GetSongRVideo4Feature {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("videoInfoPoolInput", true, "input directory")
    options.addOption("songCF", true, "input directory")
    options.addOption("alsCF", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val songCFInput = cmd.getOptionValue("songCF")
    val alsCFInput = cmd.getOptionValue("alsCF")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val videoPoolDf = spark.read.parquet(videoInfoPoolInput)

    val videoPoolS = videoPoolDf
      .rdd
      .map(row => row.getAs[Long]("videoId"))
      .collect
      .toSet
    val bc_videoPoolS = spark.sparkContext.broadcast(videoPoolS)

    // 打标
    val songTable = videoPoolDf
      .filter($"bgmIds"=!="0")
      .withColumn("songIdForRec", explode(splitByDelim("_tab_")($"bgmIds")))
      .select("videoId", "songIdForRec")
      .rdd
      .map(line => {
        (line.getString(1), line.getLong(0))
      }).toDF("songId", "videoId")
      .groupBy("songId")
      .agg(
        collect_set($"videoId").as("songVideoS")
      )
    // merged_cf_sim_song_video
    val songCFTable = spark.sparkContext.textFile(songCFInput)
      .filter{line =>
        val info = line.split("\t")
        info.length >= 2 && !info(1).equals("null")
      }
      .map{line =>
        val info = line.split("\t")
        val songid = info(0).split("-")(0)
        val videos = info(1).split(",")
          .map { item =>
            val vInfo = item.split(":|-")
            val videoId = vInfo(0).toLong
            val vType = vInfo(1)
            val recalltypes = vInfo(3)
            var remain = false
            if (vType.equals("video")  // 只保留有效的 vtype
              && bc_videoPoolS.value.contains(videoId) // 保留videoPool中idtype
            )
              remain = true
            (videoId+":"+recalltypes, remain)
          }
          .filter(tup => tup._2)
          .map(tup => tup._1)
        (songid, videos)
      }.toDF("songId", "songCFVInfoS")
    // als
    val alsCFTable = spark.sparkContext.textFile(alsCFInput)
      .filter{line =>
        val info = line.split("\t")
        info.length >= 2 && info(0).endsWith("-song")
      }
      .map{line =>
        val info = line.split("\t")
        val songid = info(0).split("-")(0)
        val videos = info(1).split(",")
          .map { item =>
            val vInfo = item.split(":|-")
            val videoId = vInfo(0).toLong
            val vType = vInfo(1)
            var remain = false
            if (vType.equals("video") &&  // 保留有效的 vtype
              bc_videoPoolS.value.contains(videoId)) // 保留videoPool中idtype
              remain = true
            (videoId, remain)
          }
          .filter(tup => tup._2)
          .map(tup => tup._1)
        (songid, videos)
      }.toDF("songId", "alsVideoS")

    val mergedTable = songCFTable
      .join(songTable, Seq("songId"), "outer")
      .join(alsCFTable, Seq("songId"), "outer")
      .map{line =>
        val songId = line.getString(0)

        val songCFVInfoS = line.getAs[Seq[String]]("songCFVInfoS")
        val songVideoS = line.getAs[Seq[Long]]("songVideoS")
        val alsVideoS = line.getAs[Seq[Long]]("alsVideoS")

        val addedS = scala.collection.mutable.Set[Long]()
        val videosRet = new scala.collection.mutable.ArrayBuffer[String]()

        if (songCFVInfoS != null) {
          val cfRet = new scala.collection.mutable.ArrayBuffer[Long]()
          songCFVInfoS.foreach { vidRecalltypes =>
            val vInfo = vidRecalltypes.split(":")
            val vid = vInfo(0).toLong
            if ((songVideoS==null || !songVideoS.contains(vid))
              && addedS.add(vid)) {
              cfRet.append(vid)
            }
          }
          if (cfRet.size > 0) {
            // 默认相似度排序
            videosRet.appendAll(cfRet.map(vid => vid + ":video:mcf"))
          }
        }
        if (alsVideoS != null) {
          val alsRet = new scala.collection.mutable.ArrayBuffer[Long]()
          alsVideoS.foreach { vid =>
            if ((songVideoS==null || !songVideoS.contains(vid))
              && addedS.add(vid)) {
              alsRet.append(vid)
            }
          }
          if (alsRet.size > 0) {
            // 默认相似度排序
            videosRet.appendAll(alsRet.map(vid => vid + ":video:als"))
          }
        }
        var videoStr = "null"
        if (videosRet.length > 0)
          videoStr = videosRet.mkString(",")
        (songId, videoStr)
      }.toDF("songId", "videos")
      .filter($"videos"=!="null")
    //.cache
    mergedTable.write.option("sep", "\t").csv(output)

  }
}
