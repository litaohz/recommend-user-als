package com.netease.music.recommend.scala.feedflow.pool

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.collection.mutable
/**
  * Created by hzlvqiang on 2018/5/24.
  */
object PoolStats {

  case class VideoStats(idtype: String, topCat: String, score: Double, play: Long, sub: Long, share: Long, cmt: Long, like: Long, jcmt: Long, dislike: Long, report: Long, exposure: Long)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("video_pool", true, "input")
    options.addOption("Music_VideoRcmdMeta", true, "input")

    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val vliadVideoCatJSON = spark.read.json(cmd.getOptionValue("Music_VideoRcmdMeta"))
    println("Load meta json data..." )
    vliadVideoCatJSON.show(10, false)

    val vliadVideoCatList = vliadVideoCatJSON
      .withColumn("videoId", $"videoId")
      .withColumn("topCategory", getTopCat($"category"))
      .collect()
    println(vliadVideoCatList.head)

    val validVideoCatM = mutable.HashMap[String, String]()
    for (vidRow <- vliadVideoCatList) {
      val idtype = vidRow.getAs[Long]("videoId").toString + ":video"
      val topCat = vidRow.getAs[String]("topCategory")
      validVideoCatM.put(idtype, topCat)
    }
    println("validVideoCatM size:" + validVideoCatM.size)
    println(validVideoCatM.head)
    val validVideoCatMBroad = sc.broadcast(validVideoCatM)

    val data = spark.read.textFile(cmd.getOptionValue("video_pool"))
      .map(line => {
        // vid    Time    Play    Sub Share   Cmt Like    Jcmt dislike report score
        val ts = line.split("\t")

        val play = ts(2).toLong
        //if (play < 100) {
        //  score = 0.5
        //}
        val sub = ts(3).toLong
        val share = ts(4).toLong
        val cmt = ts(5).toLong
        val like = ts(6).toLong
        val jcmt = ts(7).toLong

        val idtype = ts(0) + ":video"
        //val action = ts(3).toLong + ts(4).toLong + ts(5).toLong + ts(6).toLong
        //val disaction = ts(8).toLong + ts(9).toLong
        val dislike = ts(8).toLong
        val report = ts(9).toLong
        val exposure = ts(11).toLong
        var score = (sub + share + cmt + like - dislike - report) / (play + 100.0)
        if (score > 1) {
          score = 1
        }
        val topCat = validVideoCatMBroad.value.getOrElse(idtype, "null")
        if (validVideoCatMBroad.value.contains(idtype)) {
          VideoStats(idtype, topCat, score, play, sub, share, cmt, like, jcmt, dislike, report, exposure)
        } else {
          VideoStats(null, topCat, score, play, sub, share, cmt, like, jcmt, dislike, report, exposure)
        }
      })
      .filter($"idtype".isNotNull).sort(-$"score")


    println("1. 全部统计")
    data.show(10, false)
    data.repartition(1).write.parquet(cmd.getOptionValue("output") + "/all")
    println("2. play>=500中 top20%")
    //var topList20 = mutable.MutableList[VideoStats]()
    var dList = data.filter($"play" >= 1000).collect().sortWith(_.score > _.score)
    var topcatVideosList = mutable.HashMap[String, mutable.ArrayBuffer[VideoStats]]()
    for (i <- 0 until dList.size) {
      /*
      if (i < (dList.size * 0.2).toInt) {
        topList20 += dList(i)
      } else {*/
        val videos = topcatVideosList.getOrElse(dList(i).topCat, new mutable.ArrayBuffer[VideoStats]())
        topcatVideosList.put(dList(i).topCat, videos)
        videos.append(dList(i))
      //}

    }
    //topList20.toDF().select("idtype").repartition(1).write.text(cmd.getOptionValue("output") + "/allcat.paly500.top0.20")

    println("3. play>=1000，各个分类 top20%")
    var eachCatTop20 =  mutable.MutableList[VideoStats]()
    for ((topCat, videos) <- topcatVideosList) {
      println("#cat " + topCat + " num=" + videos.size)
      for (i <- 0 until (videos.size * 0.2).toInt) {
        println(videos(i).score)
        eachCatTop20 += videos(i)
      }
    }
    eachCatTop20.toDF().repartition(1).write.parquet(cmd.getOptionValue("output") + "/eachcat.paly1000up.top0.20.raw")
    eachCatTop20.toDF().select("idtype").repartition(1).write.text(cmd.getOptionValue("output") + "/eachcat.paly1000up.top0.20")

    println("4. play<1000 && >=100中 top20%")
    dList = data.filter($"play" < 1000 && $"play" >= 100).collect().sortWith(_.score > _.score)
    topcatVideosList = mutable.HashMap[String, mutable.ArrayBuffer[VideoStats]]()
    for (i <- 0 until dList.size) {
      /*if (i < (dList.size * 0.2).toInt) {
        topList20 += dList(i)
      } else {*/
      val videos = topcatVideosList.getOrElse(dList(i).topCat, new mutable.ArrayBuffer[VideoStats]())
      topcatVideosList.put(dList(i).topCat, videos)
      videos.append(dList(i))
      //}

    }
    //topList20.toDF().select("idtype").repartition(1).write.text(cmd.getOptionValue("output") + "/allcat.paly500.top0.20")

    println("5. play>=500，各个分类 top20%")
    eachCatTop20 =  mutable.MutableList[VideoStats]()
    for ((topCat, videos) <- topcatVideosList) {
      println("#cat " + topCat + " num=" + videos.size)
      for (i <- 0 until (videos.size * 0.3).toInt) {
        println(videos(i).score)
        eachCatTop20 += videos(i)
      }
    }
    eachCatTop20.toDF().repartition(1).write.parquet(cmd.getOptionValue("output") + "/eachcat.paly100-1000.top0.30.raw")
    eachCatTop20.toDF().select("idtype").repartition(1).write.text(cmd.getOptionValue("output") + "/eachcat.paly100-1000.top0.30")

    println("6. play>=500，各个分类 buttom6%")
    dList = data.filter($"play" >= 500).collect().sortWith(_.score > _.score)
    topcatVideosList = mutable.HashMap[String, mutable.ArrayBuffer[VideoStats]]()
    for (i <- 0 until dList.size) {
      val videos = topcatVideosList.getOrElse(dList(i).topCat, new mutable.ArrayBuffer[VideoStats]())
      topcatVideosList.put(dList(i).topCat, videos)
      videos.append(dList(i))
    }
    val eachCatButtom20 =  mutable.MutableList[VideoStats]()
    for ((topCat, videos) <- topcatVideosList) {
      println("#cat " + topCat + " num=" + videos.size)
      for (i <- (videos.size * 0.90).toInt until videos.size) {
        println(videos(i).score)
        eachCatButtom20 += videos(i)
      }
    }
    eachCatButtom20.toDF().repartition(1).write.parquet(cmd.getOptionValue("output") + "/eachcat.paly500up.bottum0.1.raw")
    eachCatButtom20.toDF().select("idtype").repartition(1).write.text(cmd.getOptionValue("output") + "/eachcat.paly500up.bottum0.1")

    var eachCatButtom =  mutable.MutableList[VideoStats]()
    for ((topCat, videos) <- topcatVideosList) {
      println("#cat " + topCat + " num=" + videos.size)
      for (i <- (videos.size * 0.85).toInt until videos.size) {
        println(videos(i).score)
        eachCatButtom += videos(i)
      }
    }
    eachCatButtom.toDF().repartition(1).write.parquet(cmd.getOptionValue("output") + "/eachcat.paly500up.bottum0.15.raw")
    eachCatButtom.toDF().select("idtype").repartition(1).write.text(cmd.getOptionValue("output") + "/eachcat.paly500up.bottum0.15")

    eachCatButtom =  mutable.MutableList[VideoStats]()
    for ((topCat, videos) <- topcatVideosList) {
      println("#cat " + topCat + " num=" + videos.size)
      for (i <- (videos.size * 0.92).toInt until videos.size) {
        println(videos(i).score)
        eachCatButtom += videos(i)
      }
    }
    eachCatButtom.toDF().repartition(1).write.parquet(cmd.getOptionValue("output") + "/eachcat.paly500up.bottum0.08.raw")
    eachCatButtom.toDF().select("idtype").repartition(1).write.text(cmd.getOptionValue("output") + "/eachcat.paly500up.bottum0.08")

  }

  def getTopCat = udf((category: String) => {
    if (category == null) {
      "null"
    } else {
      category.substring(1).split(",")(0)
    }
  })

}
