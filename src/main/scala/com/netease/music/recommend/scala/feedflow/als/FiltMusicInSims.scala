package com.netease.music.recommend.scala.feedflow.als

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.util.Random
/**
  * Created by hzlvqiang on 2018/5/16.
  */
object FiltMusicInSims {

  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val options = new Options()
    options.addOption("sims_input", true, "sims_input")
    options.addOption("video_pool", true, "input")
    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val musicCategories = mutable.HashSet[String]()
    // 1_音乐资讯 16_音乐现场 7001 音乐 20_舞蹈 10_ACG
    // 8001,18002 短片：音乐类短片
    //  [23,9001] 综艺：音乐节目
    //  [22,18001] 电影：电影OST
    //  [10,17008]: ACG:动漫音乐
    //  [10,19003] ACG:宅舞
    //  [10,15018]： ACG:唱见
    for (cat <- "1,16,7001,20,10_17008,10_19003,1_15018,8001_18002,23_9001,22_18001".split(",")) {
      musicCategories.add(cat)
    }
    val musicCategoriesBroad = sc.broadcast(musicCategories)

    val musicVideoList = spark.read.parquet(cmd.getOptionValue("video_pool"))
      .withColumn("isMusicC", isMusicC(musicCategoriesBroad.value)($"category"))
        // .filter($"isMusicC"===true || $"isMusicVideo" === true)
      .filter($"isMusicC"===true)
      .select("videoId").collect()
    println("musicVideoList size:" + musicVideoList.size)
    println(musicVideoList.head)
    val musicVideoSet = mutable.HashSet[String]()
    for (vid <- musicVideoList) {
      musicVideoSet.add(vid.getAs[Long]("videoId").toString())
    }
    val musicVideoSetBroad = sc.broadcast(musicVideoSet)
    println("musicVideoSetBroad size : " + musicVideoSetBroad.value.size)

    // println(musicVideoSetBroad.value)

    val musicSimsData = spark.read.textFile(cmd.getOptionValue("sims_input"))
      .map(line => {
        // 28315609	1417396-video:0.7083068,927670-video:0.66050625,
        val ts = line.split("\t")
        val musicIdtypescores = mutable.ArrayBuffer[String]()
        val random = new Random()
        for (idtypescore <- ts(1).split(",")) {
          val idtypeScore = idtypescore.split(":")
          val idtype = idtypeScore(0).split("-")
          if (musicVideoSetBroad.value.contains(idtype(0))) {
            musicIdtypescores.append(idtypescore)
          }
        }
        var res = ""
        if (musicIdtypescores.size > 0) {
          res = ts(0) + "\t" + musicIdtypescores.mkString(",")
        }
        res
      }).filter(!_.isEmpty)
    println("sample data...")
    musicSimsData.show(10)

    musicSimsData.repartition(4).write.text(cmd.getOptionValue("output"))
  }

  def isMusicC(musicCategories: mutable.HashSet[String]) = udf( (category:String) => {
    if (musicCategories.contains(category) || musicCategories.contains(category.split("_")(0))) {
      true
    } else {
      false
    }
  })
}
