package com.netease.music.recommend.scala.feedflow.userProfile

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * 根据近期行为有无行为，区分活跃非活跃用户
  * Created by hzlvqiang on 2018/4/19.
  */
object TellActiveUserVideoRcmd {

  def comb = udf((id: Long, pref: Float) => {
    id.toString + ":" +  pref.toString
  })
  def comb3 = udf((id:Long, idtype:String, pref:Float) => {
    id.toString + ":" + idtype + ":" + pref.toFloat
  })

  def combPrefData = udf(( idTypePref: Seq[String]) => {
    var maxPref = 0F
    val result = mutable.ArrayBuffer[String]()
    if (idTypePref != null) {
      for (idp <- idTypePref) {
        result.append(idp)
        val idps = idp.split(":")
        val pref = idps(idps.size-1).toFloat
        if (pref > maxPref) {
          maxPref = pref
        }
      }
    }
    maxPref.toString + "\t" + result.mkString(",")
  })

  def isActive = udf((prefdata:String) => {
    val pd = prefdata.split("\t", 2)
    val pref = pd(0).toFloat
    if (pref > 0) {
      true
    } else {
      false
    }
  })

  def flushText = udf((userId: Long, prefData: String) => {
    userId.toString + "\t" + prefData
  })

  def hasVideorcmdAction(str: String) = udf((impressPageSet: Seq[String]) => {
    if (impressPageSet != null && impressPageSet.contains(str)) {
      true
    } else {
      false
    }
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("user_video_pref", true, "log input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    // |videoId|vType|actionUserId|logDate |viewTime|playTypes |playSourceSet |zanCnt|commentCnt|subscribeCnt|shareClickCnt
    // |totalActionCnt|totalInverseActionCnt|actionTypeList|impressCnt|impressPageSet |algSet |artistIds|songIds|videoPref |
    val videoInputs = cmd.getOptionValues("user_video_pref")
    val userVideoPrefData = spark.read.parquet(videoInputs.toSeq : _*)
        .withColumn("recommendvideoAction", hasVideorcmdAction("recommendvideo")($"impressPageSet"))
      .filter($"recommendvideoAction" === true) // 有视频流曝光
      .select("videoId", "vType", "actionUserId", "videoPref")
      .withColumn("videoidTypePref", comb3($"videoId", $"vType", $"videoPref"))
      .groupBy($"actionUserId")
      .agg(collect_list($"videoidTypePref").as("videoidTypePref_list"))

    // 视频流有过播放
    val activeUserData = userVideoPrefData
      .withColumn("prefData", combPrefData($"videoidTypePref_list"))
      .withColumn("isActive", isActive($"prefData"))
      .filter($"isActive" === true)
      .withColumn("text", flushText($"actionUserId", $"prefData"))

    activeUserData.select("text").write.text(cmd.getOptionValue("output"))

  }

}
