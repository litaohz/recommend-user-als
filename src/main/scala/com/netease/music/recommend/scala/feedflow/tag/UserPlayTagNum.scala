package com.netease.music.recommend.scala.feedflow.tag

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  *
  * 计算tag平均消费数量
  *
  * Created by hzlvqiang on 2017/12/8.
  */
object UserPlayTagNum {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val options = new Options()
    options.addOption("user_video_pref", true, "user_video_pref")
    options.addOption("video_tags", true, "video_tags")
    options.addOption("min_play_num", true, "min_play_num")
    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)
    println("加载video tags!")
    val inputVidTags = cmd.getOptionValue("video_tags")
    val idtypeTagsM = getVideoTagsM(spark.read.textFile(inputVidTags).collect())
    println("tagVideosM size:" + idtypeTagsM.size)
    val idtypeTagsMBroad = sc.broadcast(idtypeTagsM)

    val minVideoPrefNum = cmd.getOptionValue("min_play_num").toInt
    println("筛选每个用户播放视频")
    val userVideoPrefData = spark.read.parquet(cmd.getOptionValue("user_video_pref"))
      .filter($"videoPref" > 0).select("actionUserId", "videoId", "vType", "videoPref")

    import org.apache.spark.sql.functions._
    val userVideoCntData = userVideoPrefData.groupBy($"actionUserId").agg(count($"videoId") as "vCnt")
    println("计算每个用户对每个tag的播放总量")
    val tagTcntData = userVideoPrefData.join(userVideoCntData, Seq("actionUserId"), "left").filter($"vCnt" > minVideoPrefNum)
      .map(row => {
        (row.getAs[Long]("actionUserId"), row.getAs[Long]("videoId").toString + ":" + row.getAs[String]("vType"))
      }).rdd.groupByKey().flatMap({case (key, values) =>
      val tagCntM = mutable.HashMap[String, Int]()
      for (value <- values) {
        val tags = idtypeTagsMBroad.value.getOrElse(value.toString, null)
        if (tags != null) {
          for (tag <- tags) {
            val cnt = tagCntM.getOrElse(tag, 0)
            tagCntM.put(tag, cnt + 1)
          }
        }
      }
      tagCntM
    }).toDF("tag", "tcnt")

    tagTcntData.show(10)
    println("计算所有用户对每个tag的平均播放量")
    val tagAvTcnt = tagTcntData.groupBy($"tag").agg(avg($"tcnt") as "avTcnt", max($"tcnt") as "maxTcnt", mean($"tcnt") as "meanTcnt").orderBy($"avTcnt".desc)

    tagAvTcnt.repartition(1).write.csv(cmd.getOptionValue("output"))

  }

  def getVideoTagsM(strings: Array[String]) = {
    val videoTagsM = mutable.HashMap[String, mutable.HashSet[String]]()
    // vid \t type \t tag_TYPE,tag_TYPE
    for (string <- strings) {
      val ts = string.split("\t")
      val idtype = ts(0) + ":" + ts(1)
      if (!videoTagsM.contains(idtype)) {
        videoTagsM.put(idtype, new mutable.HashSet[String]())
      }
      val tagSet = videoTagsM.getOrElse(idtype, null)
      val tags = ts(2).split(",")
      for (tag <- tags) {
        val wtype = tag.split("_")
        if (!wtype(1).equals("CC") && !wtype(1).equals("WD") && !wtype(1).equals("OT") && !wtype(1).equals("KW")) {
          val tag = wtype(0)
          tagSet.add(tag)
        }
      }
    }
    videoTagsM
  }

}
