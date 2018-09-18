package com.netease.music.recommend.scala.feedflow.userProfile

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.utils.userFunctions.getDefaultColumn
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object GetUserSearchProfile_days {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("userSearchSongPref", true, "input directory")
    options.addOption("days", true, "days")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userSearchSongPrefInput = cmd.getOptionValue("userSearchSongPref")
    val days = cmd.getOptionValue("days").toInt
    val outputPath = cmd.getOptionValue("output")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val existsUserSearchPrefInputPaths = getExistsPathUnderDirectory(userSearchSongPrefInput, fs)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val timeInMillis = System.currentTimeMillis
    val millisInDay = 24 * 3600 * 1000l
    val qualifiedLogDateSeq = for (i <- (1 to days)) yield {
      sdf.format(new Date(timeInMillis - i * millisInDay))
    }
    import spark.implicits._
    val qualifiedExistedUserVideoPrefInputPaths = mutable.Set[String]()
    existsUserSearchPrefInputPaths.foreach(path => {
      var qualified = false
      qualifiedLogDateSeq.foreach(qualifiedLogDate => {
        if (path.contains(qualifiedLogDate))
          qualified = true
      })
      if (qualified) {
        qualifiedExistedUserVideoPrefInputPaths += path
      }
    })

    val userSearchSongPrefTableS = mutable.Set[DataFrame]()
    qualifiedExistedUserVideoPrefInputPaths.foreach (existsUserSearchSongPrefInputPath => {
      val dateStr = existsUserSearchSongPrefInputPath.split("/")(9)
      val userSearchSongPrefTable = spark.read.option("sep", "\t")
        .csv(existsUserSearchSongPrefInputPath)
        .toDF("userId", "searchPrefs")
        .withColumn("date", getDefaultColumn(dateStr)())
      userSearchSongPrefTableS += userSearchSongPrefTable
    })
    val userSearchSongPrefTableA = userSearchSongPrefTableS.toArray
    var userSearchSongPref_daysTable = userSearchSongPrefTableA(0)
    for (i <- 1 to userSearchSongPrefTableA.length-1) {
      userSearchSongPref_daysTable = userSearchSongPref_daysTable.union(userSearchSongPrefTableA(i))
    }

    val finalDataset = userSearchSongPref_daysTable
      .groupBy($"userId")
      .agg(collect_set($"searchPrefs").as("searchPrefSet"))
      .withColumn("searchSongPref_days", getDaysPref($"searchPrefSet"))
      .select("userId", "searchSongPref_days")

    finalDataset.repartition(16).write.option("sep", "\t").csv(outputPath)
  }


  def getDaysPref = udf((prefSet:Seq[String]) => {
    val prefM = mutable.Map[String, Float]()
    prefSet.foreach(line => {
      for (infoStr <- line.split(",")) {
        val info = infoStr.split(":")
        val id = info(0)
        val pref = info(1).toFloat
        if (prefM.contains(id))
          prefM.put(id, prefM.get(id).get + pref)
        else
          prefM.put(id, pref)
      }
    })
    val result = new StringBuilder()
    prefM
        .toArray
        .sortWith(_._2 > _._2)
      .foreach(pair => {
        result.append(pair._1 + ":" + pair._2 + ",")
      })
    result.deleteCharAt(result.length-1).toString
  })

  def getExistsPathUnderDirectory(inputDir:String, fs:FileSystem):collection.mutable.Set[String] = {
    val set = collection.mutable.Set[String]()
    for (status <- fs.listStatus(new Path(inputDir))) {
      val input = status.getPath.getParent + "/" + status.getPath.getName + "/userSongList"
      set.add(input)
    }
    set
  }

  def getPrefInfoWithSource = udf((id:Long, videoPrefDays:Double, relativeVideoIds:Seq[Long]) => {
    id + ":" + videoPrefDays + ":" + relativeVideoIds.mkString("-")
  })
}
