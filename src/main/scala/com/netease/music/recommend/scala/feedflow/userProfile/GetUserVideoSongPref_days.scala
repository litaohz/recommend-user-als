package com.netease.music.recommend.scala.feedflow.userProfile

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.userProfile.GetUserVideoArtistPref_days.collectPrefsWithThred
import com.netease.music.recommend.scala.feedflow.utils.userFunctions.splitByDelim
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable

object GetUserVideoSongPref_days {

  //  case class PrefInfoWithRank(prefInfo:String, rank:Int)
//  case class TodayUserPrefInfo(userId:String, prefsToday:String)
  case class PrefInfoWithRank(prefInfo:String, rank:Int)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("userVideoPref", true, "input directory")
    options.addOption("complementSongPref_days", true, "input directory")
    options.addOption("days", true, "days")
    options.addOption("minPrefsThred", true, "min pref song thred of for each user")
    options.addOption("minSourceThred", true, "min sourceid thred of a qualified artist pref for users")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userVideoPrefInput = cmd.getOptionValue("userVideoPref")
    val complementSongPref_daysInput = cmd.getOptionValue("complementSongPref_days")
    val days = cmd.getOptionValue("days").toInt
    val minPrefsThred = cmd.getOptionValue("minPrefsThred").toInt
    val minSourceThred = cmd.getOptionValue("minSourceThred").toInt
    val outputPath = cmd.getOptionValue("output")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val timeInMillis = System.currentTimeMillis
    val millisInDay = 24 * 3600 * 1000l
    val qualifiedLogDateSeq = for (i <- (1 to days)) yield {
      sdf.format(new Date(timeInMillis - i * millisInDay))
    }
    val existsUserVideoPrefInputPaths = getExistsPathUnderDirectory(userVideoPrefInput, fs)
      .filter{path =>
        var remain = false
        qualifiedLogDateSeq.foreach(qualifiedLogDate => if (path.contains(qualifiedLogDate)) remain = true)
        remain
      }
    import spark.implicits._

    val window = Window.partitionBy($"actionUserId").orderBy($"videoPrefDays".desc)
    val userVideoSongPrefTable = spark.read.parquet(existsUserVideoPrefInputPaths.toSeq : _*)
      .repartition(1000)
      .filter($"videoPref">0 && $"songIds"=!="0")
      .filter($"logDate".isin(qualifiedLogDateSeq : _*))
      .withColumn("songId", explode(splitByDelim("_tab_")($"songIds")))
      .groupBy("songId", "actionUserId")
      .agg(sum("videoPref").as("videoPrefDays"), collect_set($"videoId").as("relativeVideoIds"))
      .withColumn("videoSongPrefInfo", getPrefInfoWithSource($"songId", $"videoPrefDays", $"relativeVideoIds"))
      .select($"actionUserId", $"videoSongPrefInfo", row_number().over(window).as("prefRank"))
      .cache

    val finalDataRDDNoThredFilter = userVideoSongPrefTable
      .rdd
      .map {line =>
        (line.getString(0), PrefInfoWithRank(line.getString(1), line.getInt(2)))
      }
      .groupByKey.map ({
      case (key, value) => collectPrefs(key, value)
    })
      .filter {line =>
        if (line.equals("null"))
          false
        else
          true
      }
    finalDataRDDNoThredFilter.repartition(6).saveAsTextFile(outputPath + "/all")

    val songPref_daysThredFilterTodayTable = userVideoSongPrefTable
      .rdd
      .map {line =>
        (line.getString(0), (line.getString(1), line.getInt(2)))
      }
      .groupByKey.map ({
      case (key, value) => collectPrefsWithThred(key, value, minSourceThred)
    })
      .filter {line =>
        if (line.equals("null"))
          false
        else
          true
      }
      .map{line =>
        val info = line.split("\t")
        (info(0), info(1))
      }
      .toDF("userId", "prefsToday")
      .cache
    val songPref_daysWeekagoTable = spark.read.option("sep", "\t")
      .csv(complementSongPref_daysInput)
      .toDF("userId", "songPrefsWeekago")
    val finalDataRDDThredFilter = songPref_daysThredFilterTodayTable
      .join(songPref_daysWeekagoTable, Seq("userId"), "outer")
      .na.fill("null", Seq("prefsToday", "songPrefsWeekago"))
      .na.fill("null", Seq("prefsToday", "songPrefsWeekago"))
      .withColumn("songPrefsFinal", mergePrefs(minPrefsThred, minSourceThred)($"prefsToday", $"songPrefsWeekago"))
      .filter($"songPrefsFinal"=!="null")
      .select($"userId", $"songPrefsFinal")
    finalDataRDDThredFilter.repartition(6).write.option("sep", "\t").csv(outputPath + "/filter")

  }

  def mergePrefs(minPrefsThred:Int, minSourceThred:Int) = udf((prefsToday:String, prefsOld:String) => {
    if (prefsOld.equals("null"))
      prefsToday
    else {
      var prefNumToday = prefsToday.split(",").length
      if (prefsToday.equals("null"))
        prefNumToday = 0

      if (prefNumToday >= minPrefsThred)
        prefsToday
      else {
        val prefsSet = mutable.Set[(String, Float, String)]()
        val recalledIdS = mutable.Set[String]()
        // 将今天的新pref加入set
        if (prefNumToday > 0) {
          for (prefInfoNew <- prefsToday.split(",")) {
            val info = prefInfoNew.split(":")
            prefsSet.add((info(0), info(1).toFloat, info(2)))
            recalledIdS.add(info(0))
          }
        }
        val prefNumOld = prefsOld.split(",").length
        val end = Math.min(prefNumOld, minPrefsThred - prefNumToday)
        var addedNum = 0
        for (prefInfo <- prefsOld.split(",")) {
          if (addedNum < end) {
            val info = prefInfo.split(":")
            val sourceIdCnt = info(2).split("-").length
            if (sourceIdCnt >= minSourceThred && !recalledIdS.contains(info(0))) {
              prefsSet.add((info(0), info(1).toFloat, info(2)))
              recalledIdS.add(info(0))
              addedNum += 1
            }
          }
        }
        if (prefsSet.size <= 0)      // 从一个星期前的entity pref补充后，如果没有满足minSourceThred的entity pref，返回“null”
          "null"
        else {
          val prefLinkedSet = mutable.LinkedHashSet[String]()
          prefsSet.toArray.sortWith(_._2 > _._2)
            .foreach { prefInfo =>
              prefLinkedSet.add(prefInfo._1 + ":" + prefInfo._2 + ":" + prefInfo._3)
            }
          prefLinkedSet.mkString(",")
        }
      }
    }
  })

  def collectPrefs(userId:String, prefTuples:Iterable[PrefInfoWithRank]):String = {
    val videoPrefLinkedSet = mutable.LinkedHashSet[String]()
    var prefThread = 3.0
    if (prefTuples.size > 80)
      prefThread = 10.0
    else if (prefTuples.size > 30)
      prefThread = 6.0
    else if (prefTuples.size < 5)
      prefThread = 2.0
    prefTuples.toArray.sortWith(_.rank < _.rank)
      .foreach {prefInfoWithRank =>
        val prefDays = prefInfoWithRank.prefInfo.split(":")(1).toDouble
        if (prefDays >= prefThread)
          videoPrefLinkedSet.add(prefInfoWithRank.prefInfo)
      }

    if (videoPrefLinkedSet.size > 0)
      userId + "\t" + videoPrefLinkedSet.mkString(",")
    else
      "null"
  }

  def getExistsPathUnderDirectory(inputDir:String, fs:FileSystem):collection.mutable.Set[String] = {
    val set = collection.mutable.Set[String]()
    for (status <- fs.listStatus(new Path(inputDir))) {
      val input = status.getPath.getParent + "/" + status.getPath.getName + "/parquet"
      set.add(input)
    }
    set
  }

  def getPrefInfoWithSource = udf((id:Long, prefDays:Double, relativeVideoIds:Seq[Long]) => {
    id + ":" + prefDays + ":" + relativeVideoIds.mkString("-")
  })
}
