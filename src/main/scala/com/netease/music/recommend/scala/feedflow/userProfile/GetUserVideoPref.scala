package com.netease.music.recommend.scala.feedflow.userProfile

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.videoProfile.GetVideoHotScore.getExistsPath
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable.ArrayBuffer

object GetUserVideoPref {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoPoolNdays", true, "log input directory")
    options.addOption("impress", true, "log input directory")
    options.addOption("mainpage", true, "log input directory")
    options.addOption("oldMainpage", true, "log input directory")
    options.addOption("zan", true, "log input directory")
    options.addOption("comment", true, "log input directory")
    options.addOption("commentreply", true, "log input directory")
    options.addOption("subscribemv", true, "log input directory")
    options.addOption("subscribevideo", true, "log input directory")
    options.addOption("playend", true, "log input directory")
    options.addOption("click", true, "log input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoPoolNdaysInput = cmd.getOptionValue("videoPoolNdays")
    val impressInput = cmd.getOptionValue("impress")
    val mainpageInput = cmd.getOptionValue("mainpage")
    val oldMainpageInput = cmd.getOptionValue("oldMainpage")
    val zanInput = cmd.getOptionValue("zan")
    val commentInput = cmd.getOptionValue("comment")
    val commentreplyInput = cmd.getOptionValue("commentreply")
    val subscribemvInput = cmd.getOptionValue("subscribemv")
    val subscribevideoInput = cmd.getOptionValue("subscribevideo")
    val playendInput = cmd.getOptionValue("playend")
    val clickInput = cmd.getOptionValue("click")
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val existsVideoPoolInputPaths = getExistsPath(videoPoolNdaysInput, fs)
    logger.warn("existing videoPoolInput path:\t" + existsVideoPoolInputPaths.mkString(","))
    val videoPoolNdaysTable = spark.read.parquet(existsVideoPoolInputPaths.toSeq : _*)
      .groupBy($"videoId", $"vType")
      .agg(
        collect_set($"artistIds").as("aids")
        ,collect_set($"bgmIds").as("sids")
        //        ,countDistinct($"artistIds").as("aCnt"), countDistinct($"bgmIds").as("sCnt")
      )
      .withColumn("artistIds", getIdsFromSeq($"aids"))
      .withColumn("songIds", getIdsFromSeq($"sids"))
      .drop("aids", "sids")

    // 视频流曝光日志
    val flowImpressTable = spark.sparkContext.textFile(impressInput)
      .map {line =>
        val info = line.split("\t")
        val page = info(1)
        val actionUserId = info(3)
        val logtime = info(4).toLong
        val logDate = getLogDate(logtime)
        val videoId = info(5).toLong
        val vType = info(6)
        val alg = info(8)
        var groupId = -1l
        if (info.length >= 13) {
          try {
            groupId = info(12).toLong
          } catch {
            case ex :NumberFormatException => groupId = -1
          }
        }
        (videoId, getVType(vType), actionUserId, logDate, page, groupId, alg, logtime)
      }.toDF("videoId", "vType", "actionUserId", "logDate", "page", "groupId", "alg", "logtime")
      .filter($"actionUserId">0)
      .filter($"page".isin(usefulPage : _*))
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("videoId", "vType", "actionUserId", "logDate")
      .agg(
        count("videoId").as("impressCnt")
        ,collect_set("page").as("impressPageSet")
        ,collect_set("alg").as("algSet")
        ,max($"logtime").as("lastLogtime")
      )
    // 老首页曝光
    val oldMainpageImpressTable = spark.sparkContext.textFile(oldMainpageInput)
      .map {line =>
        val info = line.split("\t")
        val page = info(1)
        val actionUserId = info(3)
        val logtime = info(4).toLong
        val logDate = getLogDate(logtime)
        val videoId = info(5).toLong
        val vType = info(6)
        val alg = info(8)
        (videoId, getVType(vType), actionUserId, logDate, page, alg, logtime)
      }.toDF("videoId", "vType", "actionUserId", "logDate", "page", "alg", "logtime")
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("videoId", "vType", "actionUserId", "logDate")
      .agg(
        count("videoId").as("impressCnt")
        ,collect_set("page").as("impressPageSet")
        ,collect_set("alg").as("algSet")
        ,max($"logtime").as("lastLogtime")
      )
    // 新首页曝光日志
    val newMainpageImpressTable = spark.sparkContext.textFile(mainpageInput)
      .filter {line =>
        val info = line.split("\t")
        if (info.length>=13) {
          val action = info(0)
          val vType = info(10)
          if (action.equals("impress") && usefulVType.contains(vType) && !info(11).isEmpty)
            true
          else
            false
        } else
          false
      }
      .map { line =>
        val info = line.split("\t")
        //val action = info(0)
        val page = info(1)
        val actionUserId = info(3)
        val logtime = info(4).toLong
        val logDate = getLogDate(logtime)
        //val actionType= info(8)
        val vType = info(10)
        val videoId = info(11).toLong
        val alg = info(12)
        (videoId, vType, actionUserId, logDate, page, alg, logtime)
      }.toDF("videoId", "vType", "actionUserId", "logDate", "page", "alg", "logtime")
      .groupBy("videoId", "vType", "actionUserId", "logDate")
      .agg(
        count("videoId").as("impressCnt")
        ,collect_set("page").as("impressPageSet")
        ,collect_set("alg").as("algSet")
        ,max($"logtime").as("lastLogtime")
      )
    // 合并曝光日志
    val impressTable = flowImpressTable
      .union(oldMainpageImpressTable)
      .union(newMainpageImpressTable)
      .groupBy("videoId", "vType", "actionUserId", "logDate")
      .agg(
        sum("impressCnt").as("impressCnt")
        ,collect_list("impressPageSet").as("impressPageSetL")
        ,collect_list("algSet").as("algSetL")
        ,max($"lastLogtime").as("lastLogtime_impressTable")
      )
      .map {row =>
        val videoId = row.getAs[Long](0)
        val vType = row.getAs[String](1)
        val actionUserId = row.getAs[String](2)
        val logDate = row.getAs[String](3)
        val impressCnt = row.getAs[Long](4)
        val impressPageSetL = row.getAs[Seq[Seq[String]]](5)
        val impressPageSet = ArrayBuffer[String]()
        impressPageSetL.foreach{set =>
          if (!set.isEmpty) {
            set.foreach{page =>
              if (!impressPageSet.contains(page) && !page.isEmpty)
                impressPageSet.append(page)
            }
          }
        }
        val algSetL = row.getAs[Seq[Seq[String]]](6)
        val algSet = ArrayBuffer[String]()
        algSetL.foreach{set =>
          if (!set.isEmpty) {
            set.foreach{alg =>
              if (!algSet.contains(alg) && !alg.isEmpty)
                algSet.append(alg)
            }
          }
        }
        val lastLogtime_impressTable = row.getAs[Long](7)
        (videoId, vType, actionUserId, logDate, impressCnt, impressPageSet, algSet, lastLogtime_impressTable)
      }.toDF("videoId", "vType", "actionUserId", "logDate", "impressCnt", "impressPageSet", "algSet", "lastLogtime_impressTable")

    val zanTable = spark.sparkContext.textFile(zanInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(1)
        val actionUserId = info(3)
        val logtime = info(4).toLong
        val logDate = getLogDate(logtime)
        val videoId = info(5).toLong
        (videoId, getVType(vType), actionUserId, logDate, logtime)
      }.toDF("videoId", "vType", "actionUserId", "logDate", "logtime")
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("videoId", "vType", "actionUserId", "logDate")
      .agg(
        count("videoId").as("zanCnt")
        ,max($"logtime").as("lastLogtime_zanTable")
      )

    val commentTable = spark.sparkContext.textFile(commentInput+","+commentreplyInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(1)
        val actionUserId = info(3)
        val logtime = info(4).toLong
        val logDate = getLogDate(logtime)
        val videoId = info(5).toLong
        (videoId, getVType(vType), actionUserId, logDate, logtime)
      }.toDF("videoId", "vType", "actionUserId", "logDate", "logtime")
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("videoId", "vType", "actionUserId", "logDate")
      .agg(
        count("videoId").as("commentCnt")
        ,max($"logtime").as("lastLogtime_commentTable")
      )

    val subscribeTable = spark.sparkContext.textFile(subscribemvInput+","+subscribevideoInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(1)
        val actionUserId = info(3)
        val logtime = info(4).toLong
        val logDate = getLogDate(logtime)
        val videoId = info(5).toLong
        (videoId, getVType(vType), actionUserId, logDate, logtime)
      }.toDF("videoId", "vType", "actionUserId", "logDate", "logtime")
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("videoId", "vType", "actionUserId", "logDate")
      .agg(
        count("videoId").as("subscribeCnt")
        ,max($"logtime").as("lastLogtime_subscribeTable")
      )

    val playTable = spark.sparkContext.textFile(playendInput)
      .map {line =>
        val info = line.split("\t")
        val vType = info(6)
        val actionUserId = info(3)
        val logtime = info(4).toLong
        val logDate = getLogDate(logtime)
        val videoId = info(5).toLong
        val source = info(8)
        var download = 0l
        if (!info(9).isEmpty)
          download = info(9).toLong
        var isNextplay = 0l
        if (!info(12).isEmpty)
          isNextplay = info(12).toLong
        var time = 0L
        try {
          time = info(15).toLong
        } catch {
          case ex :NumberFormatException => time = 0
        }
        val playType = info(16)
        var groupId = -1l
        if (info.length >= 20) {
          try {
            groupId = info(19).toLong
          } catch {
            case ex :NumberFormatException => groupId = -1
          }
        }
        (videoId, getVType(vType), actionUserId, time, logDate, playType, source, groupId, download, isNextplay, logtime)
      }.toDF("videoId", "vType", "actionUserId", "time", "logDate", "playType", "source", "groupId", "download", "isNextplay", "logtime")
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .filter($"videoId" > 100 && $"time" > 0)
      .groupBy("videoId", "vType", "actionUserId", "logDate")
      .agg(
        sum("time").as("viewTime")
        ,collect_set($"playType").as("playTypes")
        ,collect_set($"source").as("playSourceSet")
        ,max($"download").as("download")
        ,max($"isNextplay").as("isNextplay")
        ,max($"logtime").as("lastLogtime_playTable")
      )

    val clickTable = spark.sparkContext.textFile(clickInput)
      .filter(_.split("\t").length>9)
      .map {line =>
        val info = line.split("\t")
        val vType = info(6)
        val actionUserId = info(3)
        val logtime = info(4).toLong
        val logDate = getLogDate(logtime)
        val videoId = info(5).toLong
        val actionType = info(8)
        var groupId = -1l
        if (info.length >= 13) {
          try {
            groupId = info(12).toLong
          } catch {
            case ex :NumberFormatException => groupId = -1
          }
        }
        (videoId, getVType(vType), actionUserId, logDate, actionType, groupId, logtime)
      }.toDF("videoId", "vType", "actionUserId", "logDate", "actionType", "groupId", "logtime")
      .filter($"videoId" > 100)
      .filter($"actionUserId">0)
      .filter($"vType".isin(usefulVType : _*))
      .groupBy("videoId", "vType", "actionUserId", "logDate", "actionType")  // 对各种click type去重（userId级别）
      .agg(
        count($"videoId").as("actionCnt")
        ,max($"logtime").as("lastLogtime_clickTable")
      )
      .drop($"actionCnt")

    val dislikeFromMainpageTable = spark.sparkContext.textFile(mainpageInput)
      .filter {line =>
        val info = line.split("\t")
        if (info.length>=12) {
          val actionUserId = info(3).toLong
          val target = info(8)
          val targetid = info(9)
          val resourcetype = info(10)
          //val usefulVType = Seq("mv", "video")
          if (actionUserId>0 && target.equals("dislike") && targetid.equals("button") && usefulVType.contains(resourcetype)) {
            val resourceid = info(11).toLong
            if (resourceid>0)
              true
            else
              false
          } else
            false
        } else
          false
      }
      .map { line =>
        val info = line.split("\t")
        val actionUserId = info(3)
        val logtime = info(4).toLong
        val logDate = getLogDate(logtime)
        val actionType= info(8)
        val vType = info(10)
        val videoId = info(11).toLong
        (videoId, vType, actionUserId, logDate, actionType, logtime)
      }.toDF("videoId", "vType", "actionUserId", "logDate", "actionType", "logtime")
      .groupBy("videoId", "vType", "actionUserId", "logDate", "actionType")  // 对各种click type去重（userId级别）
      .agg(
        count($"videoId").as("actionCnt")
        ,max($"logtime").as("lastLogtime_dislikeFromMainpageTable")
      )
      .drop($"actionCnt")
      .withColumn("dislike", lit(1l))

    val furtherClickTable = clickTable
      .groupBy("videoId", "vType", "actionUserId", "logDate")
      .agg(
        sum(($"actionType"==="share").cast(IntegerType)).as("shareClickCnt")
        ,sum((!$"actionType".isin(filterSetForGoodClick : _*)).cast(IntegerType)).as("totalActionCnt")
        ,sum(($"actionType".isin(actionSetForBadClick : _*)).cast(IntegerType)).as("totalInverseActionCnt")
        ,collect_list($"actionType").as("actionTypeList")
        ,max($"lastLogtime_clickTable").as("lastLogtime_furtherClickTable")
      )

    val videoPrefTable = playTable
      .join(zanTable, Seq("videoId", "vType", "actionUserId", "logDate"), "outer")
      .join(commentTable, Seq("videoId", "vType", "actionUserId", "logDate"), "outer")
      .join(subscribeTable, Seq("videoId", "vType", "actionUserId", "logDate"), "outer")
      .join(dislikeFromMainpageTable, Seq("videoId", "vType", "actionUserId", "logDate"), "outer")
      .join(furtherClickTable, Seq("videoId", "vType", "actionUserId", "logDate"), "outer")
      //      .join(shareClickTable, Seq("videoId", "vType", "actionUserId", "logDate"), "outer")
      //      .join(goodClickTable, Seq("videoId", "vType", "actionUserId", "logDate"), "outer")
      //      .join(badClickTable, Seq("videoId", "vType", "actionUserId", "logDate"), "outer")
      .join(impressTable, Seq("videoId", "vType", "actionUserId", "logDate"), "outer")
      .na.fill(-1, Seq("lastLogtime_playTable", "lastLogtime_zanTable", "lastLogtime_commentTable", "lastLogtime_subscribeTable", "lastLogtime_dislikeFromMainpageTable", "lastLogtime_furtherClickTable", "lastLogtime_impressTable"))
      .withColumn("lastLogtime", maxColumn($"lastLogtime_playTable", $"lastLogtime_zanTable", $"lastLogtime_commentTable", $"lastLogtime_subscribeTable", $"lastLogtime_dislikeFromMainpageTable", $"lastLogtime_furtherClickTable", $"lastLogtime_impressTable"))
      .drop("lastLogtime_playTable", "lastLogtime_zanTable", "lastLogtime_commentTable", "lastLogtime_subscribeTable", "lastLogtime_dislikeFromMainpageTable", "lastLogtime_furtherClickTable", "lastLogtime_impressTable")
      .join(videoPoolNdaysTable, Seq("videoId", "vType"), "outer")
      //      .withColumn("prefSource", getPrefSource($"page", $"source"))
      .filter($"actionUserId".isNotNull)
      .na.fill(0, Seq("viewTime", "zanCnt", "commentCnt", "subscribeCnt", "shareClickCnt", "totalActionCnt", "totalInverseActionCnt", "impressCnt", "dislike", "download", "isNextplay"))
      .na.fill("0", Seq("artistIds", "songIds"))
      //      .drop("page", "source")
      .withColumn("videoPref", getVideoPref($"viewTime", $"zanCnt", $"commentCnt", $"subscribeCnt", $"shareClickCnt", $"totalActionCnt", $"totalInverseActionCnt", $"playTypes", $"impressCnt", $"dislike"))

    videoPrefTable.repartition(6).write.parquet(outputPath + "/parquet")
    videoPrefTable
      .select(
        "videoId", "vType", "actionUserId", "logDate", "viewTime"
        ,"zanCnt", "commentCnt", "subscribeCnt", "shareClickCnt", "totalActionCnt"
        ,"impressCnt" ,"videoPref", "artistIds", "songIds"
      )
      .repartition(16)
      .write.option("sep", "\t").csv(outputPath + "/csv")
  }

  def getLogDate(timeInMilis:Long):String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(new Date(timeInMilis))
  }

  def getVideoPref = udf((viewTime:Double, zanCnt:Long, commentCnt:Long, subscribeCnt:Long, shareClickCnt:Long, totalActionCnt:Long, totalInverseActionCnt:Long, playTypes:Seq[String], impressCnt:Long, dislike:Long) => {
    //    val viewPref = {
    //      if (viewTime <= 180 && playTypes != null && playTypes.contains("playend"))
    //        16
    //      else
    //        Math.sqrt(viewTime)
    //    }
    if (totalInverseActionCnt > 0 || dislike > 0)
      -5.0
    else {
      val viewPref = Math.sqrt(viewTime)
      val commentPref = {
        if (commentCnt > 0)
          viewPref * 0.5
        else
          0.0
      }
      viewPref + subscribeCnt * 16 + commentPref + zanCnt * 2 + shareClickCnt * 3 + totalActionCnt * 1 + impressCnt * 0
    }
  })

  def maxColumn = udf((lastLogtime_playTable: Long, lastLogtime_zanTable: Long, lastLogtime_commentTable: Long, lastLogtime_subscribeTable: Long, lastLogtime_dislikeFromMainpageTable: Long, lastLogtime_furtherClickTable: Long, lastLogtime_impressTable: Long) => {
    var ret = -1l
    if (ret < lastLogtime_playTable)
      ret = lastLogtime_playTable
    if (ret < lastLogtime_zanTable)
      ret = lastLogtime_zanTable
    if (ret < lastLogtime_commentTable)
      ret = lastLogtime_commentTable
    if (ret < lastLogtime_subscribeTable)
      ret = lastLogtime_subscribeTable
    if (ret < lastLogtime_dislikeFromMainpageTable)
      ret = lastLogtime_dislikeFromMainpageTable
    if (ret < lastLogtime_furtherClickTable)
      ret = lastLogtime_furtherClickTable
    if (ret < lastLogtime_impressTable)
      ret = lastLogtime_impressTable
    ret
  })

  def getNegativeFeedback = udf((negativeActionTypes:Seq[String]) => {
    var negativeScore = 0.0f
    negativeActionTypes.foreach{negativeActionType =>
      if (negativeActionType.equals(""))
        negativeScore += 0.0f
      else if (negativeActionType.equals(""))
        negativeScore += 0.0f
      else if (negativeActionType.equals(""))
        negativeScore += 0.0f
      else if (negativeActionType.equals(""))
        negativeScore += 0.0f
    }
  })

  def getVType(vtype:String):String = {
    if (vtype.equals("video_classify"))
      vtype
    else if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }

  def getIdsFromSeq = udf((seq : Seq[String]) => {
    val set = collection.mutable.Set[String]()
    seq.foreach(line =>{
      if (line.contains("_tab_")) {
        for (idStr <- line.split("_tab_")) {
          if (idStr.toLong > 0)
            set.add(idStr)
        }
      } else {
        if (line.toLong > 0)
          set.add(line)
      }
    })
    if (set.size > 0)
      set.mkString("_tab_")
    else
      "0"
  })

  def getLongFromSeq = udf((seq : Seq[Long]) => {
    val map = collection.mutable.Map[Long, Int]()
    seq.foreach(line =>{
      val cnt = map.getOrElse(line, 0)
      map.put(line, cnt)
    })

    var result = 0l
    var maxCnt = 0
    map.map{line =>
      if (line._2 > maxCnt) {
        result = line._1
        maxCnt = line._2
      }
    }
    result
  })

  def getIntFromSeq = udf((seq : Seq[Int]) => {
    val map = collection.mutable.Map[Int, Int]()
    seq.foreach(line =>{
      val cnt = map.getOrElse(line, 0)
      map.put(line, cnt)
    })

    var result = 0
    var maxCnt = 0
    map.map{line =>
      if (line._2 > maxCnt) {
        result = line._1
        maxCnt = line._2
      }
    }
    result
  })

  def getStringFromSeq = udf((seq : Seq[String]) => {
    val map = collection.mutable.Map[String, Int]()
    seq.foreach(line =>{
      val cnt = map.getOrElse(line, 0)
      map.put(line, cnt)
    })

    var result = "null"
    var maxCnt = 0
    map.map{line =>
      if (line._2 > maxCnt) {
        result = line._1
        maxCnt = line._2
      }
    }
    result
  })
}
