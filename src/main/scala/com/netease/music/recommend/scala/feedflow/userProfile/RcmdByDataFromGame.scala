package com.netease.music.recommend.scala.feedflow.userProfile

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

/**
  * 根据游戏的标签数据进行召回
  * Created by hzlvqiang on 2018/1/11.
  */
object RcmdByDataFromGame {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val sc = new SparkContext(conf);
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("user_star_data", true, "user_star_data")
    options.addOption("user_tag_data", true, "user_tag_data")
    options.addOption("video_tags", true, "video_tags")
    options.addOption("video_feature", true, "video_feature")
    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("user_rced_video_from_event", true, "user_rced_video_from_event")
    options.addOption("ml_resort", true, "ml_resort")
    options.addOption("output", true, "output directory")


    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userStarInput = cmd.getOptionValue("user_star_data")
    val userTagInput = cmd.getOptionValue("user_tag_data")
    val inputUserRecedInput = cmd.getOptionValue("user_rced_video")
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._


    println("读取视频特征数据...")
    val inputVideoFeatrue = cmd.getOptionValue("video_feature")
    println("inputVideoFeatrue:" + inputVideoFeatrue)
    val viter = spark.read.parquet(inputVideoFeatrue).toDF().select("videoId", "vType", "rawPrediction").collect()
    // println("vparray size:" + vparray.)
    val videoPredM = getVideoPredM(viter)
    //val videoPredM = getVideoPredM(viter)
    println("vieoPrefM size:" + videoPredM.size)

    println("获取tag1+tag3组合tag")
    val tag13s = spark.read.parquet(userTagInput).map( line => {
      //|  os|  user_id| device_id|imei|imei_key|android_id|android_id_key| idfa| idfa_key| last_time| key|tag_id| cnt|tag_name|
      // val uid = line.getAs[Long]("user_id").toString()
      val tag1 = line.getAs[String]("tag_1_name")
      val tag3 = line.getAs[String]("tag_3_name")
      tag1 + ":" + tag3
    }).distinct().collect()

    println("读取tag_videos数据...")
    val videoTags = spark.read.textFile(cmd.getOptionValue("video_tags")).collect()
    val tagVideosM = getVideoTagsM(videoTags, videoPredM, tag13s)
    println(tagVideosM("周杰伦")(0))
    println(tagVideosM("周杰伦")(1))
    println(tagVideosM("周杰伦")(2))
    println("tagVideosM size:" + tagVideosM.size)
    val tagVideosMBroad = sc.broadcast(tagVideosM)

    val mlResortData = spark.read.textFile(cmd.getOptionValue("ml_resort"))
      .map(line => {
        val ts = line.split("\t", 2)
        val recallNum = ts(1).split(",").length
        (ts(0), ts(0) + "\t" + recallNum.toString(), recallNum)
        /*
        if (recallNum >= 60) {
          (ts(0), recallNum)
        } else {
          (null, false)
        }*/
      }).filter(_._1 != null).toDF("userId", "userIdCnt", "recallnum")
    mlResortData.select($"userIdCnt").repartition(1).write.text(outputPath + "/notactive")


    println("读取用户已经推荐数据1...")
    val userRcedVideosData = spark.read.textFile(inputUserRecedInput).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userId", "rcedVideos")
    println("读取用户已经推荐数据2...")
    val inputUserRcedFromEvent = cmd.getOptionValue("user_rced_video_from_event")
    val userRcedVideosFromEventData = spark.read.textFile(inputUserRcedFromEvent).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userId", "rcedVideosFromEvent")

    val userVideByStarData = spark.read.parquet(userStarInput).map( line => {
      //|  os|  user_id| device_id|imei|imei_key|android_id|android_id_key| idfa| idfa_key| last_time| key|tag_id| cnt|tag_name|
      val uid = line.getAs[Long]("user_id").toString()
      val tag = line.getAs[String]("tag_name")
      val lstTime = line.getAs[Long]("last_time")
      val cnt = line.getAs[Double]("cnt").toInt
      if (cnt >= 5) {
        (uid, (tag, cnt, lstTime))
      } else {
        (null, ("", 0, 0L))
      }
    }).filter(_._1 != null).rdd.groupByKey.map({case (key, values) =>
      val sortedTagCnt = values.toArray[(String, Int, Long)].sortWith(_._3 > _._3) // 按时间排列
      val sortedTagCntS = mutable.ArrayBuffer[String]()
      for (tagcnt <- sortedTagCnt) {
        sortedTagCntS.append(tagcnt._1)
      }
      if (key.equals("119360518")) {
        println("user:119360518's sortedTagCnt")
        println(sortedTagCnt.mkString(","))
      }
      (key, sortedTagCntS.mkString(","))
    }).toDF("userId", "tagcnts")
      .join(userRcedVideosData, Seq("userId"), "left")
      .join(userRcedVideosFromEventData, Seq("userId"), "left")
      .join(mlResortData, Seq("userId"), "left")
      .map({ row =>
        val recallNum = row.getAs[Int]("recallnum")
        if (recallNum > 30) {
          "\t"
        } else {
          val userId = row.getAs[String]("userId")
          val tagcnts = row.getAs[String]("tagcnts")
          val recedStrs1 = row.getAs[String]("rcedVideos")
          val recedVides = mutable.HashSet[String]()
          if (recedStrs1 != null) {
            for (vid <- recedStrs1.split(",")) {
              recedVides.add(vid)
            }
          }
          val recedStrs2 = row.getAs[String]("rcedVideosFromEvent")
          if (recedStrs2 != null) {
            for (vid <- recedStrs2.split(",")) {
              recedVides.add(vid)
            }
          }

          val outBuff = mutable.HashSet[String]()
          val iter = tagcnts.split(",").iterator
          val tagVideosM = tagVideosMBroad.value
          while (iter.hasNext && outBuff.size < 20) {
            val tag = iter.next()
            val videos = tagVideosM.getOrElse(tag, null)
            if (videos != null) {
              val vidIter = videos.iterator
              var hasMatch = false
              while (vidIter.hasNext && !hasMatch) {
                val vid = vidIter.next()
                if (!recedVides.contains(vid) && !outBuff.contains(vid + ":video")) {
                  outBuff.add(vid + ":video")
                  hasMatch = true
                }
              }
            }
          }
          userId + "\t" + outBuff.mkString(",")
        }
      })
      userVideByStarData.filter(!_.endsWith("\t")).write.text(outputPath + "/gmStar")

      // tagdata
      val userVideByTagData = spark.read.parquet(userTagInput).map( line => {
        //|  os|  user_id| device_id|imei|imei_key|android_id|android_id_key| idfa| idfa_key| last_time| key|tag_id| cnt|tag_name|
        val uid = line.getAs[Long]("user_id").toString()
        val tag1 = line.getAs[String]("tag_1_name")
        val tag3 = line.getAs[String]("tag_3_name")
        val cnt = line.getAs[Double]("cnt").toInt
        val lstTime = line.getAs[Long]("last_time")
        if (cnt < 5) {
          (null, ("", "", 0, 0L))
        } else {
          (uid, (tag1, tag3, cnt, lstTime))
        }
        }).filter(_._1 != null).rdd.groupByKey.map({case (key, values) =>
          val sortedTagsCnt = values.toArray[(String, String, Int, Long)].sortWith(_._4 > _._4)
          val sortedTagsCntS = mutable.ArrayBuffer[String]()
          for (tagcnt <- sortedTagsCnt) {
            sortedTagsCntS.append(tagcnt._1 + ":" + tagcnt._2)
          }
          if (key.equals("119360518")) {
            println("user:119360518's sortedTagCnt")
            println(sortedTagsCnt.mkString(","))
          }
          (key, sortedTagsCntS.mkString(","))
        }).toDF("userId", "tags")
          .join(userRcedVideosData, Seq("userId"), "left")
          .join(userRcedVideosFromEventData, Seq("userId"), "left")
          .join(mlResortData, Seq("userId"), "left")
          .map({ row =>
            val recallNum = row.getAs[Int]("recallnum")
            if (recallNum < 30) {
              (null, null, null)
            } else {
              val userId = row.getAs[String]("userId")
              val tags = row.getAs[String]("tags")
              val recedStrs1 = row.getAs[String]("rcedVideos")
              val recedVides = mutable.HashSet[String]()
              if (recedStrs1 != null) {
                for (vid <- recedStrs1.split(",")) {
                  recedVides.add(vid)
                }
              }
              val recedStrs2 = row.getAs[String]("rcedVideosFromEvent")
              if (recedStrs2 != null) {
                for (vid <- recedStrs2.split(",")) {
                  recedVides.add(vid)
                }
              }

              //val outBuff1 = mutable.HashSet[String]()
              val outBuff3 = mutable.HashSet[String]()
              val outBuff13 = mutable.HashSet[String]()
              val iter = tags.split(",").iterator
              val tagVideosM = tagVideosMBroad.value
              while (iter.hasNext && (outBuff3.size < 20 || outBuff13.size < 20)) {
                val tag13s = iter.next()
                val tag13 = tag13s.split(":")
                /*
              val videos1 = tagVideosM.getOrElse(tag13(0), null)
              if (videos1 != null) {
                val vidIter = videos1.iterator
                var matchNum = 0
                while (vidIter.hasNext && matchNum < 2) {
                  val vid = vidIter.next()
                  if (!recedVides.contains(vid) && !outBuff1.contains(vid + ":video")) {
                    outBuff1.add(vid + ":video")
                    matchNum += 1
                  }
                }
              }
              */
                if (outBuff3.size < 20 && tag13.length >= 2) {
                  val videos3 = tagVideosM.getOrElse(tag13(1), null)
                  if (videos3 != null) {
                    val vidIter = videos3.iterator
                    var matchNum = 0
                    while (vidIter.hasNext && matchNum < 2) {
                      val vid = vidIter.next()
                      if (!recedVides.contains(vid) && !outBuff3.contains(vid + ":video")) {
                        outBuff3.add(vid + ":video")
                        matchNum += 1
                      }
                    }
                  }
                }
                if (outBuff13.size < 20) {
                  val videos13 = tagVideosM.getOrElse(tag13s, null)
                  if (videos13 != null) {
                    val iter13 = videos13.iterator
                    var matchNum = 0
                    while (iter13.hasNext && matchNum < 2) {
                      val vid13 = iter13.next()
                      if (!recedVides.contains(vid13) && !outBuff13.contains(vid13 + ":video")) {
                        outBuff13.add(vid13 + ":video")
                        matchNum += 1
                      }
                    }
                  }
                }

              }

              ("", userId + "\t" + outBuff3.mkString(","), userId + "\t" + outBuff13.mkString(","))
            }
          }).filter(_._1 != null).toDF("gmTag1", "gmTag3", "gmTag13")

    // userVideByTagData.filter(!$"gmTag1".endsWith("\t")).select($"gmTag1").repartition(10).write.text(outputPath + "/gmTag1")
    userVideByTagData.filter(!$"gmTag3".endsWith("\t")).select($"gmTag3").repartition(10).write.text(outputPath + "/gmTag3")
    userVideByTagData.filter(!$"gmTag13".endsWith("\t")).select($"gmTag13").repartition(10).write.text(outputPath + "/gmTag13")

  }


  def getVideoPredM(rows: Array[Row]) = {
    val idtypePredM = mutable.HashMap[String, Double]()
    for (row <- rows) {
      if (row.getAs[String]("vType").equals("video")) {
        val vid = row.getAs[Long]("videoId").toString
        val rawPred = row.getAs[Double]("rawPrediction")
        idtypePredM.put(vid, rawPred)
      }
    }
    idtypePredM
  }

  def getVideoTagsM(strings: Array[String], videoScoreM : mutable.HashMap[String, Double], tag13s: Array[String]) = {

    val tags1 = mutable.HashSet[String]()
    val tags3 = mutable.HashSet[String]()
    for (tag13 <- tag13s) {
      val ts = tag13.split(":")
      tags1.add(ts(0))
      tags3.add(ts(1))
    }

    val tagVideosM = mutable.HashMap[String, mutable.HashSet[String]]()
    // vid \t type \t tag_TYPE,tag_TYPE
    for (string <- strings) {
      val ts = string.split("\t")
      val tags = ts(2).split(",")
      if (ts(1).equals("video")) {
        // val idtype = ts(0) + ":" + ts(1)
        val vtags = mutable.ArrayBuffer[String]()
        for (tag <- tags) {
          val wtype = tag.split("_")
          if (!wtype(1).equals("CC") && !wtype(1).equals("WD") && !wtype(1).equals("OT") && !wtype(1).equals("KW")) {
            val tag = wtype(0)
            vtags.append(tag)
            if (!tagVideosM.contains(tag)) {
              tagVideosM.put(tag, mutable.HashSet[String]())
            }
            var videos = tagVideosM.apply(tag)
            videos.add(ts(0))
          }
        }
        for (i <- 0 until vtags.length) {
          for (j <- 0 until vtags.length) {
            if (tags1.contains(vtags(i)) && tags3.contains(vtags(j))) {
              val tagij = vtags(i) + ":" + vtags(j)
              if (!tagVideosM.contains(tagij)) {
                tagVideosM.put(tagij, mutable.HashSet[String]())
              }
              var videos = tagVideosM.apply(tagij)
              videos.add(ts(0))
            }

          }
        }
      }
    }
    // 按照质量排序
    val tagVideosResM = mutable.HashMap[String, Array[String]]()
    for ((tag, videos) <- tagVideosM) {
      if (videos.size >= 20 && videos.size <= 4000) { // 忽略掉一些低频和高频干扰
        val sortedVideos = videos.toArray[String].sortWith { (v1, v2) =>
          val v1Score = videoScoreM.getOrElse(v1, 0.0)
          val v2Score = videoScoreM.getOrElse(v2, 0.0)
          v1Score > v2Score
        }
        tagVideosResM.put(tag, sortedVideos)
      }
    }
    tagVideosResM
  }
}
