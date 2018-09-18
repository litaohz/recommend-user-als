package com.netease.music.recommend.scala.feedflow.hot

import com.netease.music.recommend.scala.feedflow.GetVideoPool.{getCategoryId, getIds, getJsonValue}
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ColumnName, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
/**
  * Created by hzlvqiang on 2018/3/28.
  */
object GetHotVideoFromSpecUpUser {

  def isInSet(upUserset: mutable.HashSet[Long]) = udf((upUid: Long) => {
    if (upUserset.contains(upUid)) {
      true
    } else {
      false
    }
  })

  def getWhiteUpuserset(strings: Array[String]) = {
    val whiteUpusers = mutable.HashSet[Long]()
    for (str <- strings) {
      whiteUpusers.add(str.trim().toLong)
    }
    whiteUpusers
  }

  def getValidVids(rows: Array[Row]) = {
    val validSet = mutable.HashSet[Long]()
    for (row <- rows) {
      validSet.add(row.getAs[Long]("vid"))
    }
    validSet
  }

  def getSorted(videoTypePredictions: Array[Row]) = {
    val vidtypePrdList = mutable.ArrayBuffer[(String, Double)]()
    for (row <- videoTypePredictions) {
      val vid = row.getAs[Long]("videoId").toString
      val prediction = row.getAs[Double]("downgradeScore")
      vidtypePrdList.append((vid+":video", prediction))
    }
    val sortedVidtype = vidtypePrdList.sortWith(_._2 > _._2)
    sortedVidtype
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    import spark.implicits._
    val options = new Options
    options.addOption("white_upusers", true, "input")
    options.addOption("Music_VideoRcmdMeta", true, "input")
    options.addOption("video_feature_warehouse", true, "input")
    options.addOption("output", true, "output")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    println("原创用户列表...")
    val whiteUpuserset = getWhiteUpuserset(spark.read.textFile(cmd.getOptionValue("white_upusers")).collect())
    println("upUserset size:" + whiteUpuserset.size)

    println("视频审核表....")
    val videoRcmdMetaTable = spark.read.json(cmd.getOptionValue("Music_VideoRcmdMeta"))
      .withColumn("vid", $"videoId")
      .withColumn("isInWhiteUpuser", isInSet(whiteUpuserset)($"pubUid"))
      .withColumn("smallFlow", getJsonValue("smallFlow")($"extData"))
      .filter(!$"smallFlow")
      .filter($"isInWhiteUpuser")
    val validVids = getValidVids(videoRcmdMetaTable.select("vid").collect())

    val videoTypePrediction = spark.read.parquet(cmd.getOptionValue("video_feature_warehouse"))
        .withColumn("isValid", isInSet(validVids)($"videoId"))
        .filter($"isValid")
        .select($"videoId", $"downgradeScore").collect()

    val sortedVidtypePrds = getSorted(videoTypePrediction)

    val result = mutable.ArrayBuffer[String]()
    for (vtp <- sortedVidtypePrds) {
      result.append(vtp._1 + ":" + vtp._2.toString)
    }
    result.toDF("data").coalesce(1).write.text(cmd.getOptionValue("output"))
  }

}
