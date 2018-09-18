package com.netease.music.recommend.scala.feedflow.song

import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by hzzhangjunfei1 on 2018/3/16.
  */
object GetSongRelativeVideoAll {

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
    options.addOption("outputOnline", true, "output directory")
    options.addOption("outputOnlineAll", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val songCFInput = cmd.getOptionValue("songCF")
    val alsCFInput = cmd.getOptionValue("alsCF")

    val output = cmd.getOptionValue("output")
    val outputOnline = cmd.getOptionValue("outputOnline")
    val outputOnlineAll = cmd.getOptionValue("outputOnlineAll")

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

    /*val offlineDataRDD = songRelativeEventRDD
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
    onlineDataRDD.saveAsTextFile(outputOnline)*/

    val songTable = songRelativeEventRDD
      .map(line => {
        (line.getString(12), line.getLong(0))
      }).toDF("songId", "videoId")
      .groupBy("songId")
      .agg(
        collect_set($"videoId").as("songVideoS")
      )
    val songCFTable = spark.sparkContext.textFile(songCFInput)
      .filter{line =>
        val info = line.split("\t")
        info.length >= 2 && !info(1).equals("null")
      }
      .map{line =>
        val info = line.split("\t")
        val songid = info(0).split("-")(0)
        val videos = info(1).split(",").map{item =>
          val videoId = item.split(":")(0)
          videoId
        }
        (songid, videos)
      }.toDF("songId", "songCFVideoS")
    val alsCFTable = spark.sparkContext.textFile(alsCFInput)
      .filter{line =>
        val info = line.split("\t")
        info.length >= 2 && info(0).endsWith("-song")
      }
      .map{line =>
        val info = line.split("\t")
        val songid = info(0).split("-")(0)
        val videos = info(1).split(",").map{item =>
          val videoId = item.split(":|-")(0)
          videoId
        }
        (songid, videos)
      }.toDF("songId", "alsVideoS")

    val finalCF = songCFTable
      .join(songTable, Seq("songId"), "outer")
      .join(alsCFTable, Seq("songId"), "left_outer")
      .map{line =>
        val songId = line.getString(0)

        val songCFVideoS = line.getAs[Seq[String]]("songCFVideoS")
        val songVideoS = line.getAs[Seq[Long]]("songVideoS")
        val alsVideoS = line.getAs[Seq[String]]("alsVideoS")

        val addedS = scala.collection.mutable.Set[String]()
        val ret = new scala.collection.mutable.ArrayBuffer[String]()

        if (songCFVideoS != null)
          songCFVideoS.foreach{vid =>
            if (addedS.add(vid))
              ret.append(vid + ":songCF")
          }
        if (songVideoS != null)
          songVideoS.foreach{vid =>
            if (addedS.add(vid.toString))
              ret.append(vid + ":song")
          }
        if (alsVideoS != null)
          alsVideoS.foreach{vid =>
            if (addedS.add(vid))
              ret.append(vid + ":als")
          }
        val videoStr = if(ret.size > 0) ret.mkString(",") else "null"
        /*val songCFStr = if(songCFVideoS != null && songCFVideoS.size > 0) songCFVideoS.mkString(",") else "null"
        val songStr = if(songVideoS != null && songVideoS.size > 0) songVideoS.mkString(",") else "null"
        val alsStr = if(alsVideoS != null && alsVideoS.size > 0) alsVideoS.mkString(",") else "null"
        songId + "\t" + songCFStr + "\t" + songStr + "\t" + alsStr + "\t" + videoStr*/
        songId + "\t" + videoStr
      }
      .filter(!_.endsWith("null"))
     /* .filter{line =>
        val info = line.split("\t")
        !info(1).equals("null") && !info(2).equals("null") && !info(3).equals("null") && !info(4).equals("null")
      }*/
      .cache

    finalCF.rdd.saveAsTextFile(outputOnlineAll)

    val videoPoolM = videoInfoPoolTable
      .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
        ,"bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial"
        ,"rawPrediction", "isMusicVideo")
      .map(line => {
        (line.getLong(0).toString, line.mkString(":"))
      })
      //.write.mode(SaveMode.Overwrite).option("sep", "\t").csv("test")
      .collect()
      .toMap[String, String]
    val videoPoolM_bc = spark.sparkContext.broadcast(videoPoolM)

    /*finalCF
    .map{line =>
      val info = line.split("\t")
      val songId = info(0)
      val ret = scala.collection.mutable.ArrayBuffer[String]()
      for (videoInfoStr <- info(1).split(",")) {
        val videoId = videoInfoStr.split(":")(0)
        if (videoPoolM_bc.value.contains(videoId))
          ret.append(videoInfoStr)
      }
      val videosStr = if (ret.size > 0) ret.mkString(",") else "null"
      songId + "\t" + videosStr
    }
      .rdd.saveAsTextFile(output)*/

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
      .write.option("sep", "\t").csv(outputOnline)
  }
}
