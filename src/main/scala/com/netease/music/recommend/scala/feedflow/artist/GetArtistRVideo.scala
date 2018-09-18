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
object GetArtistRVideo {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("downgrade", true, "input directory")
    options.addOption("videoInfoPoolInput", true, "input directory")
    options.addOption("mCF", true, "input directory")
    options.addOption("alsCF", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val downgrade = cmd.getOptionValue("downgrade")
    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val mCFInput = cmd.getOptionValue("mCF")
    val alsCFInput = cmd.getOptionValue("alsCF")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val vidDowngradeM = spark.read.parquet(downgrade)
      .rdd
      .map(row => (row.getAs[Long](0), row.getAs[Double](1)))
      .collect()
      .toMap[Long, Double]
    val vidDowngradeM_broad = spark.sparkContext.broadcast(vidDowngradeM)

    // 获取artist相关联的动态
    val artistRelativeEventRDD = spark.read.parquet(videoInfoPoolInput)
      .filter($"artistIds"=!="0")
      .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
        ,"bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial"
        ,"rawPrediction", "isMusicVideo")
      .withColumn("artistIdForRec", explode(splitByDelim("_tab_")($"artistIds")))
      .rdd
    // 打标
    val artistTable = artistRelativeEventRDD
      .map(line => {
        (line.getString(12), line.getLong(0))
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
            if (vType.equals("video") /*&&
              (recalltypes.contains("&") || usefulCFRecalltypeS.contains(recalltypes))  // 只保留有效的cf recalltypes*/
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
            if (vType.equals("video")) // 只保留有效的cf recalltypes
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

        val vidDowngradeM = vidDowngradeM_broad.value
        val addedS = scala.collection.mutable.Set[Long]()
        val videosRet = new scala.collection.mutable.ArrayBuffer[String]()

        if (mCFVInfoS != null) {
          val cfRet = new scala.collection.mutable.ArrayBuffer[(Long, Double)]()
          mCFVInfoS.foreach { vidRecalltypes =>
            val vInfo = vidRecalltypes.split(":")
            val vid = vInfo(0).toLong
            val recallCnt = vInfo(1).split("&").length
            if (vidDowngradeM.contains(vid) && addedS.add(vid)) {
              val score = vidDowngradeM(vid)
              cfRet.append((vid , recallCnt.toDouble))
            }
          }
          if (cfRet.size > 0) {
            //val cfSize = cfRet.length
            //val endIndex = if (cfSize > 20) 20 else cfSize
            //videosRet.appendAll(cfRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:mcf").slice(0, endIndex))
            // 默认相似度排序
            videosRet.appendAll(cfRet.map(tup => tup._1 + ":video:mcf"))/*.slice(0, endIndex))*/
          }
        }
        if (artistVideoS != null) {
          val artistRet = new scala.collection.mutable.ArrayBuffer[(Long, Double)]()
          artistVideoS.foreach { vid =>
            if (vidDowngradeM.contains(vid) && addedS.add(vid)) {
              val score = vidDowngradeM(vid)
              artistRet.append((vid , score))
            }
          }
          if (artistRet.size > 0) {
            /*val labelSize = artistRet.length
            val endIndex = if (labelSize > 20) 20 else labelSize*/
            videosRet.appendAll(artistRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:label"))/*.slice(0, endIndex))*/
          }
        }
        if (alsVideoS != null) {
          val alsRet = new scala.collection.mutable.ArrayBuffer[(Long, Double)]()
          alsVideoS.foreach { vid =>
            if (vidDowngradeM.contains(vid) && addedS.add(vid)) {
              val score = vidDowngradeM(vid)
              alsRet.append((vid, score))
            }
          }
          if (alsRet.size > 0) {
            val alsSize = alsRet.length
            val endIndex = if (alsSize > 20) 20 else alsSize
            //println("zhp:" + alsSize + "," + alsRet.length + "," + alsRet.size + "," + endIndex + "," + artistId)
            //videosRet.appendAll(alsRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:als").slice(0, endIndex))
            // 默认相似度排序
            videosRet.appendAll(alsRet.map(tup => tup._1 + ":video:als").slice(0, endIndex))
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
