package com.netease.music.recommend.scala.feedflow.userProfile

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 根据近期行为有无行为，区分活跃非活跃用户
  * Created by hzlvqiang on 2018/4/19.
  */
object TellActiveUser {

  def comb = udf((id: Long, pref: Float) => {
    id.toString + ":" +  pref.toString
  })
  def comb3 = udf((id:Long, idtype:String, pref:Float) => {
    id.toString + ":" + idtype + ":" + pref.toFloat
  })

  def combPrefData = udf((videoFromEventPref: Seq[String], idTypePref: Seq[String]) => {
    var maxPref = 0F
    val result = mutable.ArrayBuffer[String]()
    if (videoFromEventPref != null) {
      for(vfp <- videoFromEventPref) {
        result.append(vfp)
        val vfps = vfp.split(":")
        val pref = vfps(vfps.size-1).toFloat
        if (pref > maxPref) {
          maxPref = pref
        }
      }
    }
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

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("user_video_pref", true, "log input directory")
    options.addOption("user_event_pref", true, "log input directory")
    options.addOption("event_video_map", true, "log input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    val eventInputs = cmd.getOptionValues("user_event_pref")
    val userEventPrefData = spark.read.textFile(eventInputs.toSeq : _*)
      .map(line => {
        // user_id \t evt_id \t pref \t impress_cnt \t click_cnt \u0001 category_category2, ... \t language \t effective \t artistId \t excellent
        val ts = line.split("\t")
        val userId = ts(0).toLong
        val eventId = ts(1).toLong
        val pref = ts(3).toFloat
        (userId, eventId, pref)
      }).toDF("actionUserId", "eventId", "pref")

    // id,eventId,userId,videoId,state,db_update_time,db_create_time,extJsonInfo
    val eventVideoMapInput = cmd.getOptionValue("event_video_map")
    val eventVideoMapData = spark.read.textFile(eventVideoMapInput)
      .map(line => {
        //id,eventId,userId,videoId,state,db_update_time,db_create_time,extJsonInfo
        val ts = line.split("\01", -1)
        val eventId = ts(1).toLong
        val videoId = ts(3).toLong
        (eventId, videoId)
      }).toDF("eventId", "videoId")

    println("evenId of pref data to userId")
    val userVideoPrefFromEventData = userEventPrefData.join(eventVideoMapData, Seq("eventId"), "left")
      .na.fill(-1, Seq("videoId"))
      .filter($"videoId" =!= -1)
      .withColumn("videoidFEPref", comb($"videoId", $"pref"))
      .groupBy($"actionUserId")
      .agg(collect_list($"videoidFEPref").as("videoidFEPref_list"))

    // |videoId|vType|actionUserId|logDate |viewTime|playTypes |playSourceSet |zanCnt|commentCnt|subscribeCnt|shareClickCnt
    // |totalActionCnt|totalInverseActionCnt|actionTypeList|impressCnt|impressPageSet |algSet |artistIds|songIds|videoPref |
    val videoInputs = cmd.getOptionValues("user_video_pref")
    val userVideoPrefData = spark.read.parquet(videoInputs.toSeq : _*)
      .select("videoId", "vType", "actionUserId", "videoPref")
      .withColumn("videoidTypePref", comb3($"videoId", $"vType", $"videoPref"))
      .groupBy($"actionUserId")
      .agg(collect_list($"videoidTypePref").as("videoidTypePref_list"))

    val activeUserData = userVideoPrefFromEventData.join(userVideoPrefData, Seq("actionUserId"), "outer")
      .withColumn("prefData", combPrefData($"videoidFEPref_list", $"videoidTypePref_list"))
      .withColumn("isActive", isActive($"prefData"))
      .filter($"isActive" === true)
      .withColumn("text", flushText($"actionUserId", $"prefData"))

    activeUserData.select("text").write.text(cmd.getOptionValue("output"))

  }

}
