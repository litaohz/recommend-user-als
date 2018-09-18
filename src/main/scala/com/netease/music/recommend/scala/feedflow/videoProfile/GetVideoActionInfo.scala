package com.netease.music.recommend.scala.feedflow.videoProfile


import com.netease.music.recommend.scala.feedflow.videoProfile.GetVideoHotScore.{getClass, getExistsPath}
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetVideoActionInfo {

  case class Video(videoId:Long, vType:String)
  case class Action(videoId:Long, vType:String, actionUserId:String)
  case class PlayAction(videoId:Long, vType:String, actionUserId:String, time:Long, isNewversion:String)
  case class ClickAction(videoId:Long, vType:String, actionUserId:String, actionType:String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoPoolNdays", true, "log input directory")
    options.addOption("click", true, "log input directory")
    options.addOption("zan", true, "log input directory")
    options.addOption("comment", true, "log input directory")
    options.addOption("commentreply", true, "log input directory")
    options.addOption("subscribemv", true, "log input directory")
    options.addOption("subscribevideo", true, "log input directory")
    options.addOption("play", true, "log input directory")
    options.addOption("playend", true, "log input directory")
    options.addOption("dataDate", true, "dataDate")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoPoolNdaysInput = cmd.getOptionValue("videoPoolNdays")
    val clickInput = cmd.getOptionValue("click")
    val zanInput = cmd.getOptionValue("zan")
    val commentInput = cmd.getOptionValue("comment")
    val commentreplyInput = cmd.getOptionValue("commentreply")
    val subscribemvInput = cmd.getOptionValue("subscribemv")
    val subscribevideoInput = cmd.getOptionValue("subscribevideo")
//    val playInput = cmd.getOptionValue("play")
    val playendInput = cmd.getOptionValue("playend")
    val dataDate = cmd.getOptionValue("dataDate")
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val existsVideoPoolInputPaths = getExistsPath(videoPoolNdaysInput, fs)
    logger.warn("existing videoPoolInput path:\t" + existsVideoPoolInputPaths.mkString(","))
    val videoPoolNdaysTable = spark.read.parquet(existsVideoPoolInputPaths.toSeq : _*)
//      .map{line =>
//        val info = line.split("\t")
//        val videoId = info(0).toLong
//        val vType = info(1)
//        Video(videoId, getVType(vType))
//      }.toDF
      .groupBy($"videoId", $"vType")
      .agg(count($"videoId").as("cnt"))
      .select($"videoId", $"vType")


    val filterSetForGoodClick = Seq[String]("zan", "share", "notlike", "report", "unsubscribe", "unzan_cmmt", "unzan_vdo", "unzan")
    val usefulVType = Seq("mv", "video")
    val clickTable = spark.sparkContext.textFile(clickInput)
      .filter(_.split("\t").length>9)
      .map {line =>
        val info = line.split("\t")
        val vType = info(6)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val actionType = info(8)
        ClickAction(videoId, getVType(vType), actionUserId, actionType)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"videoId" > 100)
      .filter(!$"actionType".isin(filterSetForGoodClick : _*))
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("videoId", "vType")
      .agg(countDistinct("actionUserId").as("actionCnt"))
      .join(videoPoolNdaysTable, Seq("videoId", "vType"), "left_outer")

    val zanTable = spark.sparkContext.textFile(zanInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(1)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        Action(videoId, getVType(vType), actionUserId)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("videoId", "vType")
      .agg(countDistinct("actionUserId").as("zanCnt"))
      .join(videoPoolNdaysTable, Seq("videoId", "vType"), "left_outer")

    val commentTable = spark.sparkContext.textFile(commentInput+","+commentreplyInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(1)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        Action(videoId, getVType(vType), actionUserId)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("videoId", "vType")
      .agg(countDistinct("actionUserId").as("commentCnt"))
      .join(videoPoolNdaysTable, Seq("videoId", "vType"), "left_outer")

    val subscribeTable = spark.sparkContext.textFile(subscribemvInput+","+subscribevideoInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(1)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        Action(videoId, getVType(vType), actionUserId)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("videoId", "vType")
      .agg(countDistinct("actionUserId").as("subscribeCnt"))
      .join(videoPoolNdaysTable, Seq("videoId", "vType"), "left_outer")

    val playTable = spark.sparkContext.textFile(playendInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(6)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val isNewversion = info(11)
        var time = 0L
        try {
          time = info(15).toLong
        } catch {
          case ex :NumberFormatException => time = 0
        }
        PlayAction(videoId, getVType(vType), actionUserId, time, isNewversion)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"videoId" > 100 && $"time">0)
      .filter($"vType".isin(usefulVType : _*))
      .filter($"isNewversion"===1)
      .groupBy("videoId", "vType", "actionUserId")
      .agg(sum("time").as("viewTime"))
      .withColumn("viewPref", getViewPref($"viewTime"))
      .groupBy("videoId", "vType")
      .agg(sum("viewPref").as("playScore"))
      .join(videoPoolNdaysTable, Seq("videoId", "vType"), "left_outer")

    val videoHotScoreTable = playTable
      .join(zanTable, Seq("videoId", "vType"), "outer")
      .join(commentTable, Seq("videoId", "vType"), "outer")
      .join(subscribeTable, Seq("videoId", "vType"), "outer")
      .join(clickTable, Seq("videoId", "vType"), "outer")
      .na.fill(0, Seq("playScore", "zanCnt", "commentCnt", "subscribeCnt", "actionCnt"))
      .withColumn("hotScore1day", getHotScore($"playScore", $"zanCnt", $"commentCnt", $"subscribeCnt", $"actionCnt"))
      .withColumn("date", getDataDate(dataDate)())
    videoHotScoreTable.repartition(1).write.parquet(outputPath)
  }

  def getViewPref = udf((viewTime:Long) => {
    if (viewTime > 300)
      3.0
    else if (viewTime > 120)
      2.0
    else if (viewTime > 60)
      1.8
    else if (viewTime > 30)
      1.6
    else if (viewTime > 20)
      1.4
    else if (viewTime > 15)
      1.2
    else
      1.0
  })

  def getHotScore = udf((playTime:Double, zanCnt:Long, commentCnt:Long, subscribeCnt:Long, actionCnt:Long) => {
    playTime * 0.2 + subscribeCnt * 0.5 + commentCnt * 0.15 + zanCnt * 0.05 + actionCnt * 0.01
  })

  def getDataDate(date:String) = udf(()=> {
    date
  })

  def getVType(vtype:String):String = {
    if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }
}
