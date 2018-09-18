package com.netease.music.recommend.scala.feedflow.quota

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * 获取同时曝光多个alg用户群体点击率
  */
object GetAlgsClickrate {

  case class Video(videoId:Long, vType:String)
  case class Impress(videoId:Long, vType:String, actionUserId:String, page:String, alg:String, os:String, impress:String)
  case class PlayAction(videoId:Long, vType:String, actionUserId:String, time:Long, source:String, os:String)
  case class Click(videoId:Long, vType:String, actionUserId:String, os:String)

  case class AlgMatrix(alg12: String, alg1Impress:Long, alg1Play:Long, alg1PlayRate:Double, alg2Impress:Long, alg2Play:Long, alg2PlayRate:Double, uv:Long)



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    //    options.addOption("videoPoolNdays", true, "log input directory")
    options.addOption("impress", true, "log input directory")
    options.addOption("click", true, "lick directory")
    // options.addOption("playend", true, "log input directory")
    options.addOption("filterIphone", true, "filterIphone")
    // options.addOption("alg", true, "alg")
    options.addOption("outputAll", true, "output directory")
    options.addOption("outputClick", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val impressInput = cmd.getOptionValue("impress")
    val clickInput = cmd.getOptionValue("click")

    val outputAllPath = cmd.getOptionValue("outputAll")
    val outputClickPath = cmd.getOptionValue("outputClick")

    import spark.implicits._

    val impressTable = spark.sparkContext.textFile(impressInput)
      .map { line =>
        val info = line.split("\t")
        val impress = info(0)
        val page = info(1)
        val os = info(2)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val vType = info(6)
        var alg = info(8)
        val splitIdx = info(8).lastIndexOf('_')
        if (splitIdx > 0) {
          alg = alg.substring(0, splitIdx)
        }
       // val alg = info[8].substring(0, splitIdx)
        Impress(videoId, getVType(vType), actionUserId, page, alg, os, impress)
      }.toDF
      .filter($"vType" === "video")
      .filter($"impress" === "impress")
      .groupBy("videoId", "actionUserId", "alg")
      .agg(count("videoId").as("useless"))
      .drop("useless")

    val clickTable = spark.sparkContext.textFile(clickInput)
      .map { line =>
        val info = line.split("\t")
        val vType = info(6)
        val os = info(2)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        Click(videoId, getVType(vType), actionUserId, os)
      }.toDF
      .groupBy("videoId", "actionUserId")
      .agg(count($"videoId").as("click"))

    val rawTable = impressTable
      .join(clickTable, Seq("videoId", "actionUserId"), "left_outer")
      .na.fill(0, Seq("click"))

    val userAlgTable = rawTable
      .rdd
      .map(row => {
        (row.getAs[String]("actionUserId"), (row.getAs[String]("alg"), row.getAs[Long]("click"), row.getAs[String]("videoId")))
      })
      .groupByKey() // groupby uid
      .flatMap({ case (key, values) => {
      val algM: scala.collection.mutable.Map[String, Array[Long]] = scala.collection.mutable.Map()
      var videoIdSet:scala.collection.mutable.Set[String] = scala.collection.mutable.HashSet()

      for (value <- values) {
        if (!videoIdSet.contains(value._3) ) {
          if (!algM.contains(value._1)) {
            var array = Array[Long](0, 0L)
            algM.put(value._1, array)
          }
          videoIdSet.add(value._3)
          algM(value._1)(0) += 1L
          if (value._2 > 0 ) {
            algM(value._1)(1) += 1L
          }
        }
      }
      val iter1 = algM.keys
      val iter2 = algM.keys
      var res = mutable.ArrayBuffer[Tuple2[String, Tuple2[Array[Long], Array[Long]]]]()
      for (key1 <- iter1) {
        // （1）单alg
        val newKey0 = key1
        res.append((newKey0, (algM(key1), algM(key1))))
        if (algM.size == 1) {
          // （2）ongly 该alg
          val onlyKey0 = key1 + ":noOtherAlg"
          res.append((onlyKey0, (algM(key1), algM(key1))))
        }
        // （3）组合情况
        for (key2 <- iter2) yield {
          val newKey12 = key1 + ":" + key2
          res.append((newKey12, (algM(key1), algM(key2))))
        }
      }
      // alg1:alg2 -> ([alg1_impress, alg1_play], [alg1_play, alg2_play])
      res
    }})

    userAlgTable.groupByKey() // groupby algs
      .map({case (key, values) => {// alg1:alg2 -> ([alg1_impress, alg1_play], [alg1_play, alg2_play])
    var alg1 = Array[Long](0, 0)
      var alg2 = Array[Long](0, 0)
      var uv:Long = 0
      for (value <- values) {
        alg1(0) += value._1(0)
        alg1(1) += value._1(1)
        alg2(0) += value._2(0)
        alg2(1) += value._2(1)
        uv += 1
      }
      AlgMatrix(key, alg1(0), alg1(1), alg1(1).toDouble/alg1(0), alg2(0), alg2(1), alg2(1).toDouble/alg2(0), uv)
    }}).toDF("alg1[:alg2]", "alg1_impress", "alg1_play", "alg1_playrate", "alg2_impress", "alg2_play", "alg2_playrate", "uv")
      .write.csv(outputAllPath)

    // 根据下拉点击区分有效无效用户
    val userAlgTable2 = rawTable
      .rdd
      .map(row => {
        (row.getAs[String]("actionUserId"), (row.getAs[String]("alg"), row.getAs[Long]("click"), row.getAs[String]("videoId")))
      })
      .groupByKey() // groupby uid
      .flatMap({ case (key, values) => {
      val algM: scala.collection.mutable.Map[String, Array[Long]] = scala.collection.mutable.Map()
      var videoIdSet:scala.collection.mutable.Set[String] = scala.collection.mutable.HashSet()
      var userImpress = 0
      var userClick = 0
      for (value <- values) {
        if (!videoIdSet.contains(value._3) ) {
          if (!algM.contains(value._1)) {
            var array = Array[Long](0, 0L)
            algM.put(value._1, array)
          }
          userImpress += 1
          videoIdSet.add(value._3)
          algM(value._1)(0) += 1L
          if (value._2 > 0 ) {
            algM(value._1)(1) += 1L
            userClick += 1
          }
        }
      }
      var res = mutable.ArrayBuffer[Tuple2[String, Tuple2[Array[Long], Array[Long]]]]()
      res.append(("clickNoclick", (Array[Long](0, 0), Array[Long](0, 0))))

      if(userClick == 0) { // 无点击用户
        res(0)._2._1(0) = 1
        res(0)._2._2(0) = 1
      } else { // 有点击用户
        res(0)._2._1(1) = 1
        res(0)._2._2(1) = 1
        val iter1 = algM.keys
        val iter2 = algM.keys
        for (key1 <- iter1) {
          // （1）单alg
          val newKey0 = key1
          res.append((newKey0, (algM(key1), algM(key1))))
          if (algM.size == 1) {
            // （2）ongly 该alg
            val onlyKey0 = key1 + ":noOtherAlg"
            res.append((onlyKey0, (algM(key1), algM(key1))))
          }
          // （3）组合情况
          for (key2 <- iter2) yield {
            val newKey12 = key1 + ":" + key2
            res.append((newKey12, (algM(key1), algM(key2))))
          }
        }
      }
      // alg1:alg2 -> ([alg1_impress, alg1_play], [alg1_play, alg2_play])
      res
    }})
    userAlgTable2.groupByKey() // groupby algs
      .map({case (key, values) => {// alg1:alg2 -> ([alg1_impress, alg1_play], [alg1_play, alg2_play])
    var alg1 = Array[Long](0, 0)
      var alg2 = Array[Long](0, 0)
      var uv:Long = 0
      for (value <- values) {
        alg1(0) += value._1(0)
        alg1(1) += value._1(1)
        alg2(0) += value._2(0)
        alg2(1) += value._2(1)
        uv += 1
      }
      AlgMatrix(key, alg1(0), alg1(1), alg1(1).toDouble/alg1(0), alg2(0), alg2(1), alg2(1).toDouble/alg2(0), uv)
    }}).toDF("alg1[:alg2]", "alg1_impress", "alg1_play", "alg1_playrate", "alg2_impress", "alg2_play", "alg2_playrate", "uv")
      .write.csv(outputClickPath)
  }

  def getVType(vtype:String):String = {
    if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }
}


