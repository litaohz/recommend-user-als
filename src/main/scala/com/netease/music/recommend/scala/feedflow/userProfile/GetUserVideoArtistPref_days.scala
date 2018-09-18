package com.netease.music.recommend.scala.feedflow.userProfile

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.userProfile.GetUserVideoSongPref_days.{getPrefInfoWithSource, mergePrefs}
import com.netease.music.recommend.scala.feedflow.utils.userFunctions.splitByDelim
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable

object GetUserVideoArtistPref_days {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("userVideoPref", true, "input directory")
    options.addOption("complementArtistPref_days", true, "input directory")
    options.addOption("days", true, "days")
    options.addOption("minPrefsThred", true, "min pref artists thred of for each user")
    options.addOption("minSourceThred", true, "min sourceid thred of a qualified artist pref for users")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userVideoPrefInput = cmd.getOptionValue("userVideoPref")
    val complementArtistPref_daysInput = cmd.getOptionValue("complementArtistPref_days")
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

    val window = Window.partitionBy($"actionUserId").orderBy($"artistPref".desc)
    val userVideoArtistPrefTable = spark.read.parquet(existsUserVideoPrefInputPaths.toSeq : _*)
      .repartition(1000)
      .filter($"videoPref">0 && $"artistIds"=!="0")
      .withColumn("artistId", explode(splitByDelim("_tab_")($"artistIds")))
      .groupBy("artistId", "actionUserId")
      .agg(sum("videoPref").as("artistPrefOriginal"), collect_set($"videoId").as("relativeVideoIds"))
      .withColumn("artistPref", getPrefBySourceCnt(size($"relativeVideoIds"), $"artistPrefOriginal"))
      .withColumn("videoArtistPrefInfo", getPrefInfoWithSource($"artistId", $"artistPref", $"relativeVideoIds"))
      .select(
        $"actionUserId"
        ,$"videoArtistPrefInfo"
        ,row_number().over(window).as("prefRank")
      )
      .cache

    val finalDataRDDNoFilter = userVideoArtistPrefTable
      .rdd
      .map {line =>
        (line.getString(0), (line.getString(1), line.getInt(2)))
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
    finalDataRDDNoFilter.repartition(6).saveAsTextFile(outputPath + "/all")

    val artistPref_daysThredFilterTodayTable = userVideoArtistPrefTable
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
//    artistPref_daysThredFilterTodayTable.write.parquet(outputPath + "/tmp")
    val artistPref_daysWeekagoTable = spark.read.option("sep", "\t")
      .csv(complementArtistPref_daysInput)
      .toDF("userId", "artistPrefsWeekago")
    val finalDataRDDThredFilter = artistPref_daysThredFilterTodayTable
      .join(artistPref_daysWeekagoTable, Seq("userId"), "outer")
      .na.fill("null", Seq("prefsToday", "artistPrefsWeekago"))
      .withColumn("artistPrefsFinal", mergePrefs(minPrefsThred, minSourceThred)($"prefsToday", $"artistPrefsWeekago"))
      .filter($"artistPrefsFinal"=!="null")
      .select($"userId", $"artistPrefsFinal")
    finalDataRDDThredFilter.repartition(6).write.option("sep", "\t").csv(outputPath + "/filter")

  }

  def collectPrefs(userId:String, prefTuples:Iterable[(String, Int)]):String = {
    val videoPrefLinkedArrayBuffer = mutable.ArrayBuffer[String]()
    var prefThread = 3.0
    val minRecallArtists = 3
    var count = 0
    if (prefTuples.size > 80)
      prefThread = 10.0
    else if (prefTuples.size > 30)
      prefThread = 6.0
    else if (prefTuples.size < 5)
      prefThread = 2.0

    prefTuples.toArray.sortWith(_._2 < _._2)
      .foreach {tup =>
        val prefDays = tup._1.split(":")(1).toDouble
        if (prefDays >= prefThread || count < minRecallArtists) {
          videoPrefLinkedArrayBuffer += tup._1
          count += 1
        }
      }

    if (videoPrefLinkedArrayBuffer.size > 0)
      userId + "\t" + videoPrefLinkedArrayBuffer.mkString(",")
    else
      "null"
  }

  def collectPrefsWithThred(userId:String, prefTuples:Iterable[(String, Int)], thred:Int):String = {
    val videoPrefLinkedSet = mutable.LinkedHashSet[String]()
    val prefThread = 2.0
    prefTuples.toArray.sortWith(_._2 < _._2)
      .foreach {tup =>
        val info = tup._1.split(":")
//        val id = info(0)
        val prefDays = info(1).toDouble
        val sourceids = info(2)
        if (prefDays >= prefThread) {
          val sourceVideoIdCnt = sourceids.split("-").length
          if (sourceVideoIdCnt >= thred)
            videoPrefLinkedSet.add(tup._1)
        }
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
}
