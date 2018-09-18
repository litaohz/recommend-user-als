package com.netease.music.recommend.scala.feedflow.song

import com.netease.music.recommend.scala.feedflow.cf.usefulCFRecalltypeS
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
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
object GetSongRItems {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("downgrade", true, "input directory")
    options.addOption("videoInfoPoolInput", true, "input directory")
    options.addOption("songCF", true, "input directory")
    options.addOption("alsCF", true, "input directory")
    options.addOption("mergeInput", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val downgrade = cmd.getOptionValue("downgrade")
    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val songCFInput = cmd.getOptionValue("songCF")
    val alsCFInput = cmd.getOptionValue("alsCF")
    val mergeInput = cmd.getOptionValue("mergeInput")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val vidDowngradeM = spark.read.parquet(downgrade)
      .rdd
      .map(row => (row.getAs[Long](0), row.getAs[Double](1)))
      .collect()
      .toMap[Long, Double]
    val vidDowngradeM_broad = spark.sparkContext.broadcast(vidDowngradeM)

    // 获取song相关联的动态
    val songRelativeEventRDD = spark.read.parquet(videoInfoPoolInput)
      .filter($"bgmIds"=!="0")
      .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
        ,"bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial"
        ,"rawPrediction", "isMusicVideo")
      .withColumn("songIdForRec", explode(splitByDelim("_tab_")($"bgmIds")))
      .rdd
    // 打标
    val songTable = songRelativeEventRDD
      .map(line => {
        (line.getString(12), line.getLong(0))
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
            if (vType.equals("video") &&
              (recalltypes.contains("&") || usefulCFRecalltypeS.contains(recalltypes))  // 只保留有效的cf recalltypes
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
            if (vType.equals("video")) // 只保留有效的cf recalltypes
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

        val vidDowngradeM = vidDowngradeM_broad.value
        val addedS = scala.collection.mutable.Set[Long]()
        val videosRet = new scala.collection.mutable.ArrayBuffer[String]()

        if (songCFVInfoS != null) {
          val cfRet = new scala.collection.mutable.ArrayBuffer[(Long, Double)]()
          songCFVInfoS.foreach { vidRecalltypes =>
            val vInfo = vidRecalltypes.split(":")
            val vid = vInfo(0).toLong
            val recallCnt = vInfo(1).split("&").length
            if (vidDowngradeM.contains(vid) && addedS.add(vid)) {
              val score = vidDowngradeM(vid)
              cfRet.append((vid , recallCnt.toDouble))  // 使用recalltypes cnt排序
            }
          }
          if (cfRet.size > 0) {
            val cfSize = cfRet.length
            val endIndex = if (cfSize > 20) 20 else cfSize
            videosRet.appendAll(cfRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:mcf").slice(0, endIndex))
          }
        }
        if (songVideoS != null) {
          val songRet = new scala.collection.mutable.ArrayBuffer[(Long, Double)]()
          songVideoS.foreach { vid =>
            if (vidDowngradeM.contains(vid) && addedS.add(vid)) {
              val score = vidDowngradeM(vid)
              songRet.append((vid , score))
            }
          }
          if (songRet.size > 0) {
            val labelSize = songRet.length
            val endIndex = if (labelSize > 20) 20 else labelSize
            videosRet.appendAll(songRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:label").slice(0, endIndex))
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
            //println("zhp:" + alsSize + "," + alsRet.length + "," + alsRet.size + "," + endIndex + "," + songId)
            videosRet.appendAll(alsRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:als").slice(0, endIndex))
          }
        }
        var videoStr = "null"
        if (videosRet.length > 0)
          videoStr = videosRet.mkString(",")
        (songId, videoStr)
      }.toDF("songId", "videos")
      .filter($"videos"=!="null")



    // mergeInput处理：
    val fs = FileSystem.get(new Configuration)
    val mergeInputDir = new Path(mergeInput)
    val mergeInputPaths = scala.collection.mutable.ArrayBuffer[String]()
    for (status <- fs.listStatus(mergeInputDir)) {
      val path = status.getPath.toString
      mergeInputPaths.append(path)
    }
    println("mergeInputPaths:\t" + mergeInputPaths.mkString(","))
    val mergeInputPathsTable = spark.read.option("sep", "\t").csv(mergeInputPaths.mkString(",")).toDF("songId", "videos")
    val finalTable = mergedTable
      .union(mergeInputPathsTable)
      .groupBy($"songId")
      .agg(
        collect_list($"videos").as("videosS")
      )
      .rdd
      .map{line =>
        val songId = line.getAs[String](0)
        val videoS = line.getAs[Seq[String]](1)
        songId + "\t" + videoS.mkString(",")
      }


    finalTable.saveAsTextFile(output)

  }
}
