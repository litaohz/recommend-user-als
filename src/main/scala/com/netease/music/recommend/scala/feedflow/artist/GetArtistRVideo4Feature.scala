package com.netease.music.recommend.scala.feedflow.artist

import com.netease.music.recommend.scala.feedflow.song.splitByDelim
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by hzzhangjunfei1 on 2018/3/16.
  */

// recalltype:
// 1: mCF
// 2: artist
// 3: als
object GetArtistRVideo4Feature {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("videoInfoPoolInput", true, "input directory")
    options.addOption("mCF", true, "input directory")
    options.addOption("alsCF", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val mCFInput = cmd.getOptionValue("mCF")
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
    val artistTable = videoPoolDf
      .filter($"artistIds"=!="0")
      .withColumn("artistIdForRec", explode(splitByDelim("_tab_")($"artistIds")))
      .select("videoId", "artistIdForRec")
      .rdd
      .map(line => {
        (line.getString(1), line.getLong(0))
      }).toDF("artistId", "videoId")
      .groupBy("artistId")
      .agg(
        collect_set($"videoId").as("artistVideoS")
      )
    // merged_cf_sim_artist_video
    val mCFTable = spark.sparkContext.textFile(mCFInput)
      .filter{line =>
        val info = line.split("\t")
        info.length >= 2 && !info(1).equals("null")
      }
      .map{line =>
        val info = line.split("\t")
        val artistid = info(0).split("-")(0)
        val videos = info(1).split(",")
          .map { item =>
            val vInfo = item.split(":|-")
            val videoId = vInfo(0).toLong
            val vType = vInfo(1)
            val recalltypes = vInfo(3)
            var remain = false
            if (vType.equals("video") // 只保留有效的vtype
              && bc_videoPoolS.value.contains(videoId) // 保留videoPool中idtype
            )
              remain = true
            (videoId+":"+recalltypes, remain)
          }
          .filter(tup => tup._2)
          .map(tup => tup._1)
        (artistid, videos)
      }.toDF("artistId", "mCFVInfoS")
    // als
    val alsCFTable = spark.sparkContext.textFile(alsCFInput)
      .filter{line =>
        val info = line.split("\t")
        info.length >= 2 && info(0).endsWith("-artist")
      }
      .map{line =>
        val info = line.split("\t")
        val artistid = info(0).split("-")(0)
        val videos = info(1).split(",")
          .map { item =>
            val vInfo = item.split(":|-")
            val videoId = vInfo(0).toLong
            val vType = vInfo(1)
            var remain = false
            if (vType.equals("video") // 只保留有效的vtype
              && bc_videoPoolS.value.contains(videoId) // 保留videoPool中idtype
            )
              remain = true
            (videoId, remain)
          }
          .filter(tup => tup._2)
          .map(tup => tup._1)
        (artistid, videos)
      }.toDF("artistId", "alsVideoS")

    val mergedTable = mCFTable
      .join(artistTable, Seq("artistId"), "outer")
      .join(alsCFTable, Seq("artistId"), "outer")
      .map{line =>
        val artistId = line.getString(0)

        val mCFVInfoS = line.getAs[Seq[String]]("mCFVInfoS")
        val artistVideoS = line.getAs[Seq[Long]]("artistVideoS")
        val alsVideoS = line.getAs[Seq[Long]]("alsVideoS")

        val addedS = scala.collection.mutable.Set[Long]()
        val videosRet = new scala.collection.mutable.ArrayBuffer[String]()

        if (mCFVInfoS != null) {
          val cfRet = new scala.collection.mutable.ArrayBuffer[Long]()
          mCFVInfoS.foreach { vidRecalltypes =>
            val vInfo = vidRecalltypes.split(":")
            val vid = vInfo(0).toLong
            if ((artistVideoS==null || !artistVideoS.contains(vid))
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
            if ((artistVideoS==null || !artistVideoS.contains(vid))
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
        (artistId, videoStr)
      }.toDF("artistId", "videos")
      .filter($"videos"=!="null")
    //.cache
    mergedTable.write.option("sep", "\t").csv(output)

  }
}
