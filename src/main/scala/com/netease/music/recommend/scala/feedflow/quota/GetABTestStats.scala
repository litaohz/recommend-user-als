package com.netease.music.recommend.scala.feedflow.quota

import com.netease.music.recommend.scala.feedflow.utils.ABTest
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.io.Source

/**
  * 获取同时曝光多个alg用户群体点击率
  */
object GetABTestStats {

  case class Video(videoId: Long, vType: String)

  case class Impress(videoId: Long, vType: String, actionUserId: String, page: String, alg: String, os: String, impress: String)

  case class PlayAction(videoId: Long, vType: String, actionUserId: String, time: Long, source: String, os: String)

  case class Click(videoId: Long, vType: String, actionUserId: String, os: String)

  case class Playend(videoId: Long, vType: String, actionUserId: String, os: String, time: Double, playendNum: Long)

  // case class AlgMatrix(alg12: String, alg1Impress:Long, alg1Play:Long, alg1PlayRate:Double, alg2Impress:Long, alg2Play:Long, alg2PlayRate:Double, uv:Long)
  case class OutputMatrix(abtest: String,
                          uvImp: Double, uvClk: Double, uvCrt: Double, uvPend: Double, uvPendCrt: Double, uvAvTime: Double,
                          pvImp: Double, pvClk: Double, pvCrt: Double, pvPend: Double, pvPendCrt: Double, pvAvTime: Double)

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
    options.addOption("click", true, "click directory")
    options.addOption("playend", true, "log input directory")
    // options.addOption("alg", true, "alg")
    options.addOption("output", true, "output directory")
    options.addOption("abtest", true, "abtest")
    options.addOption("dislike", true, "user dislike video&mv")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val impressInput = cmd.getOptionValue("impress")
    val clickInput = cmd.getOptionValue("click")
    val playendInput = cmd.getOptionValue("playend")
    val abtestInput = cmd.getOptionValue("abtest")
    val dislikeInput = cmd.getOptionValue("dislike")

    val outputPath = cmd.getOptionValue("output")

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
        // Impress(videoId, getVType(vType), actionUserId, page, alg, os, impress)
        Impress(videoId, vType, actionUserId, page, alg, os, impress)
      }.toDF
      .filter($"vType" === "video" || $"vType" === "mv")
      .filter($"impress" === "impress")
      .filter($"page" === "recommendvideo")
      .groupBy("videoId", "actionUserId")
      .agg(count("videoId").as("useless"))
      .drop("useless")

    val clickTable = spark.sparkContext.textFile(clickInput)
      .map { line =>
        val info = line.split("\t")
        val vType = info(6)
        val os = info(2)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        // Click(videoId, getVType(vType), actionUserId, os)
        Click(videoId, vType, actionUserId, os)
      }.toDF
      .groupBy("videoId", "actionUserId")
      .agg(count($"videoId").as("click"))

    val playendTable = spark.sparkContext.textFile(playendInput)
      .map { line =>
        // action + "\t" + page + "\t" + os + "\t" + userId + "\t" + logTime + "\t" +
        //videoId + "\t" + vType + "\t" + position + "\t" + source + "\t" + download + "\t" +
        //  is_autoplay + "\t" + is_newversion + "\t" + is_nextplay + "\t" + prev_id + "\t" + prev_type + "\t" +
        //  time + "\t" + end + "\t" + resource + "\t" + resourceid
        val info = line.split("\t")
        val os = info(2)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val vType = info(6)
        var time = 0.0
        try {
          time = info(15).toDouble
          if (time > 1800 || time < 0) { // 过滤异常
            time = 0
          }
        } catch {
          case ext: Exception => {
            println("FAILED to parse:" + time + " in " + line)
          }
        }
        val playEnd = info(16)
        var playendNum = 0
        if (playEnd.equals("playend")) {
          playendNum = 1
        }
        Playend(videoId, vType, actionUserId, os, time, playendNum)
      }.toDF()
      .groupBy("videoId", "actionUserId")
      .agg(sum($"time").as("sumTime"), sum($"playendNum").as("playendSum"))
      .na.fill(0, Seq("playendNum"))

    val dislikeTable = spark.sparkContext.textFile(dislikeInput)
      .map { line =>
        // uid \t vid:type,...
        val info = line.split("\t")
        val actionUserId = info(0)
        actionUserId
      }
    val disUsersArr = dislikeTable.collect()


    val rawTable1 = impressTable
      .join(clickTable, Seq("videoId", "actionUserId"), "left_outer")
      .na.fill(0, Seq("click", "impress"))
    val rawTable = rawTable1
      .join(playendTable, Seq("videoId", "actionUserId"), "left_outer")
      .na.fill(0, Seq("sumTime", "impress"))

    val configText = spark.read.textFile(abtestInput).collect()
    println("configText before ")
    for (cf <- configText) {
      println(cf)
    }

    // TODO 说明中有换行的特殊处理
    for (i <- 0 until configText.length) {
      if (configText(i).contains("videorcmd-specialrcmd")) {
        configText(i) = configText(i) + "\t补充说明\t1"
      }
    }
    println("configText after ")
    for (cf <- configText) {
      println(cf)
    }

    val userTable = rawTable
      .rdd
      .map(row => {
        (row.getAs[String]("actionUserId"), (
          row.getAs[String]("videoId"), row.getAs[Long]("click"),
          row.getAs[Double]("sumTime"), row.getAs[Long]("playendSum")))
      })
      .groupByKey() // groupby uid
      .flatMap({ case (key, values) => {
      //val algM: scala.collection.mutable.Map[String, Array[Long]] = scala.collection.mutable.Map()
      //var videoIdSet: scala.collection.mutable.Set[String] = scala.collection.mutable.HashSet()
      var impress = 0L
      var click = 0L
      var sumTime = 0.0
      var playendSum = 0L
      for (value <- values) {
        //if (!videoIdSet.contains(value._1)) {
        impress += 1L
        if (value._2 > 0 || value._3 > 0) {
          click += 1L
        }
        var time4video = value._3
        if (time4video < 0) {
          time4video = 0
        } else if (time4video > 1800) {
          time4video = 1800
        }
        sumTime += time4video
        playendSum += value._4
        //}
      }
      /*
        println("configText")
        for (cf <- configText) {
          println(cf);
        }*/
      val musicABTest = new ABTest(configText)

      var uvClick = 0
      var uvPlayend = 0
      if (click > 0) {
        uvClick = 1
      }
      if (playendSum > 0) {
        uvPlayend = 1
      }
      val res = mutable.ArrayBuffer[Tuple2[String, Array[Double]]]()
      for (codeGroup <- musicABTest.getCodeGroupNames(key.toLong)) { // key:actionUserId
        if ((!codeGroup.equals("videorcmd-dis-c") && !codeGroup.equals("videorcmd-dis-t")) || disUsersArr.contains(key)) {
          // (codeGroup, "1\t" + uvClick + "\t" + uvPlayend + "\t" + impress + "\t" + click + "\t" + sumTime + "\t" + playendSum)
          // res.append(("", ""))var value = "1\t" + uvClick + "\t" + uvPlayend + "\t" + impress + "\t" + click + "\t" + sumTime + "\t" + playendSum
          val value = Array(1, uvClick, uvPlayend, impress, click, sumTime, playendSum)
          res.append((codeGroup, value))
          // res.append((codeGroup, value))
        }
      }
      res
    }
    })

    val sumTable = userTable.groupByKey()
      .map({ case (key, values) => {
        val cumValue = Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        for (value <- values) {
          for (i <- 0 until value.size) {
            cumValue(i) += value(i)
          }
        }
        // 小流量
        /*
        key + "\t" +
          //uv曝光              uv点击                    uv点击率                             uv完整播放              uv完整播放率                         uv平均时长
          cumValue(0) + "\t" + cumValue(1) + "\t" + (cumValue(1)/cumValue(0)) + "\t" + cumValue(2) + "\t" + (cumValue(2)/cumValue(0)) + "\t" + (cumValue(5)/cumValue(0)) + "\t" +
          //pv曝光              pv点击                    pv点击率                             pv完整播放              pv完整播放率                         pv平均时长
          cumValue(3) + "\t" + cumValue(4) + "\t" + (cumValue(4)/cumValue(3)) + "\t" + cumValue(6) + "\t" + (cumValue(6)/cumValue(3)) + "\t" + (cumValue(5)/cumValue(3))
          */
        OutputMatrix(key.toString, cumValue(0), cumValue(1), (cumValue(1) / cumValue(0)), cumValue(2), (cumValue(2) / cumValue(0)), (cumValue(5) / cumValue(0)),
          cumValue(3), cumValue(4), (cumValue(4) / cumValue(3)), cumValue(6), (cumValue(6) / cumValue(3)), (cumValue(5) / cumValue(3)));
      }

      }).toDF("abtestname", "uv曝光", "uv点击", "uv点击率", "uv完整播放", "uv完整播放率", "uv平均时长", "pv曝光", "pv点击", "pv点击率", "pv完整播放", "pv完整播放率", "pv平均时长")

    sumTable.coalesce(1).write.csv(outputPath)
    //saveAsTextFile(outputPath)

  }

  def getVType(vtype: String): String = {
    if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }
}


