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
object GetSongRVideoTest {

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
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val downgrade = cmd.getOptionValue("downgrade")
    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val songCFInput = cmd.getOptionValue("songCF")
    val alsCFInput = cmd.getOptionValue("alsCF")
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
            (videoId, remain)
          }
          .filter(tup => tup._2)
          .map(tup => tup._1)
        (songid, videos)
      }.toDF("songId", "songCFVideoS")
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

        val songCFVideoS = line.getAs[Seq[Long]]("songCFVideoS")
        val songVideoS = line.getAs[Seq[Long]]("songVideoS")
        val alsVideoS = line.getAs[Seq[Long]]("alsVideoS")

        val vidDowngradeM = vidDowngradeM_broad.value
        val addedS = scala.collection.mutable.Set[Long]()
        val videosRet = new scala.collection.mutable.ArrayBuffer[String]()

        if (songCFVideoS != null) {
          val cfRet = new scala.collection.mutable.ArrayBuffer[(Long, Double)]()
          songCFVideoS.foreach { vid =>
            if (vidDowngradeM.contains(vid) && addedS.add(vid)) {
              val score = vidDowngradeM(vid)
              cfRet.append((vid , score))
            }
          }
          if (cfRet.size > 0) {
            // videosRet.appendAll(cfRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:mcf"))
            // 默认相似度排序
            videosRet.appendAll(cfRet.map(tup => tup._1 + ":video:mcf"))
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
            videosRet.appendAll(songRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:label"))
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
            println("zhp:" + alsSize + "," + alsRet.length + "," + alsRet.size + "," + endIndex + "," + songId)
            // videosRet.appendAll(alsRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:als").slice(0, endIndex))
            // 默认相似度排序
            videosRet.appendAll(alsRet.map(tup => tup._1 + ":video:als").slice(0, endIndex))
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



    /*val videoPoolM = videoInfoPoolTable
      .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
        ,"bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial"
        ,"rawPrediction", "isMusicVideo")
      .map(line => {
        (line.getLong(0).toString, line.mkString(":"))
      })
      .collect()
      .toMap[String, String]
    val videoPoolM_bc = spark.sparkContext.broadcast(videoPoolM)


    val alsRecall_thred = 20
    val resultTable = finalCF
      .map{line =>
        val info = line.split("\t")
        val songId = info(0)
        val ret = scala.collection.mutable.ArrayBuffer[String]()
        val retOnline = scala.collection.mutable.ArrayBuffer[String]()
        var alsRecall_cnt = 0
        for (videoInfoStr <- info(1).split(",")) {
          val videoId = videoInfoStr.split(":")(0)
          val algtype = videoInfoStr.split(":")(1)
          if (videoPoolM_bc.value.contains(videoId)) {

            if ((algtype.equals("als") && alsRecall_cnt < alsRecall_thred) ||
              algtype.equals("als")) {

              val videoInfo = videoPoolM_bc.value(videoId).split(":")
              val vType = videoInfo(1)
              val algConstant = "song"
              val score = videoInfo(10)
              val recalltype = "0"
              ret.append(videoPoolM_bc.value(videoId))
              retOnline.append(videoId + ":" + vType + ":" + algConstant + ":" + score + ":" + recalltype)
              if (algtype.equals("als"))
                alsRecall_cnt += 1
            }
          }
        }
        val videosStr = if (ret.size > 0) ret.mkString(",") else "null"
        val videosOnlineStr = if (retOnline.size > 0) retOnline.mkString(",") else "null"
        (songId, videosStr, videosOnlineStr)
      }.toDF("songId", "videosStr", "videosOnlineStr")
      .cache

    resultTable
      .filter($"videosStr"=!= "null")
      .select("songId", "videosStr")
      .write.option("sep", "\t").csv(output)

    resultTable
      .filter($"videosOnlineStr"=!= "null")
      .select("songId", "videosOnlineStr")
      .write.option("sep", "\t").csv(outputOnline)*/
  }
}
