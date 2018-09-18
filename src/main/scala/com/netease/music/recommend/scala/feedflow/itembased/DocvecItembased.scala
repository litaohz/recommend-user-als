package com.netease.music.recommend.scala.feedflow.itembased

import com.netease.music.recommend.scala.feedflow.userbased.{abtestGroupName, testUserSet}
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by hzzhangjunfei1 on 2018/8/8.
  */
object DocvecItembased {

  def seq2Vec = udf((features:String) => Vectors.dense(features.split(",").map(_.toDouble)))

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("vSimsFromDocvec", true, "input directory")
    options.addOption("userSearchPlayList", true, "input directory")
    options.addOption("userPref_days", true, "input directory")
    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("abtestConfig", true, "abtestConfig file")
    options.addOption("abtestExpName", true, "abtest Exp Name")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val vSimsFromDocvec = cmd.getOptionValue("vSimsFromDocvec")
    val userSearchPlayList = cmd.getOptionValue("userSearchPlayList")
    val userPref_days = cmd.getOptionValue("userPref_days")
    val user_rced_video = cmd.getOptionValue("user_rced_video")
    val output = cmd.getOptionValue("output")

    val configText = spark.read.textFile(cmd.getOptionValue("abtestConfig")).collect()
    val abtestExpName = cmd.getOptionValue("abtestExpName")
    for (cf <- configText) {
      if (cf.contains(abtestExpName)) {
        println(cf)
      }
    }

    import spark.implicits._
    val userRcedVideosDf = spark.read.textFile(user_rced_video).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userId", "rcedVideos")

    val vSimsM = spark.sparkContext.textFile(vSimsFromDocvec)
      .map { row =>
        val info = row.split("\t")
        val idtype = info(0)
        val simIdtypes = info(1)
        (idtype, simIdtypes)
      }.collect
      .toMap
    val bc_vSimsM = spark.sparkContext.broadcast(vSimsM)

    val userSearchPlayListDf = spark.sparkContext.textFile(userSearchPlayList)
      .map{line =>
        val info = line.split("\t")
        val userId = info(0)
        val searchPlayListStr = info(1)
        (userId, searchPlayListStr)
      }.toDF("userId", "searchPlayListStr")

    val userPref_daysDf = spark.sparkContext.textFile(userPref_days)
      .map{line =>
        val info = line.split("\t")
        val userId = info(0)
        val userPref_daysStr = info(1)
        (userId, userPref_daysStr)
      }.toDF("userId", "userPref_daysStr")

    val finalRdd = userSearchPlayListDf
      .join(userPref_daysDf, Seq("userId"), "outer")
      .join(userRcedVideosDf, Seq("userId"), "left_outer")
      .rdd
      .flatMap{row =>
        val userId = row.getAs[String]("userId")
        val rcedVideosStr = row.getAs[String]("rcedVideos")
        val searchPlayListStr = row.getAs[String]("searchPlayListStr")
        val userPref_daysStr = row.getAs[String]("userPref_daysStr")

        val rcedVideos = mutable.HashSet[String]()
        if (rcedVideosStr != null) {
          for (rvid <- rcedVideosStr.split(",")) {
            rcedVideos.add(rvid + ":video")
          }
        }
        val idtypeReaosnsM4searchPlayList = mutable.Map[String, mutable.Set[String]]()
        val idtypeReaosnsM4userPrefs_days = mutable.Map[String, (mutable.Set[String], Double)]()
        val reasonRecallcntM = mutable.Map[String, Int]()
        val recall_thred4reason = 5
        val recall_thred4user = 40

        if (testUserSet.contains(userId.toLong) || abtestGroupName(configText, abtestExpName, userId).equals("t1")) {
          if (searchPlayListStr != null) {
            searchPlayListStr.split(",").foreach { item =>
              if (bc_vSimsM.value.contains(item)) {
                val simIdtypes = bc_vSimsM.value(item).split(",")
                simIdtypes.foreach { simidtypeScoreStr =>
                  val idtypeScore = simidtypeScoreStr.split(":")
                  val simIdtype = idtypeScore(0).replaceAll("-", ":")
                  //val recallcnt_eachReason = resonRecallcntM.getOrElse(item, 0)
                  if (!rcedVideos.contains(simIdtype)/* && recallcnt_eachReason < recall_thred4reason*/) {
                    val reasonS = idtypeReaosnsM4searchPlayList.getOrElse(simIdtype, mutable.Set[String]())
                    idtypeReaosnsM4searchPlayList.put(simIdtype, reasonS)
                    reasonS.add(item)
                    //resonRecallcntM.put(item, recallcnt_eachReason+1)
                  }
                }
              }
            }
          }
        }

        if (testUserSet.contains(userId.toLong) || abtestGroupName(configText, abtestExpName, userId).equals("t2")) {
          if (userPref_daysStr != null) {
            userPref_daysStr.split(",").foreach { item =>
              val itemInfo = item.split(":")
              val idtype = itemInfo(0) + "-" + itemInfo(1)
              if (bc_vSimsM.value.contains(idtype)) {
                val prefScore = itemInfo(2).toDouble
                val simIdtypes = bc_vSimsM.value(idtype).split(",")
                simIdtypes.foreach { simidtypeScoreStr =>
                  val idtypeScore = simidtypeScoreStr.split(":")
                  val simIdtype = idtypeScore(0).replaceAll("-", ":")
                  //val recallcnt_eachReason = resonRecallcntM.getOrElse(idtype, 0)
                  if (!rcedVideos.contains(simIdtype)/* && recallcnt_eachReason < recall_thred4reason*/) {
                    val simScore = idtypeScore(1).toDouble
                    val (reasonS, simDotPref_now) = idtypeReaosnsM4userPrefs_days.getOrElse(simIdtype, (mutable.Set[String](), 0.0d))
                    reasonS.add(idtype)
                    idtypeReaosnsM4userPrefs_days.put(simIdtype, (reasonS, Math.max(simDotPref_now, simScore * prefScore)))
                    //resonRecallcntM.put(idtype, recallcnt_eachReason+1)
                  }
                }
              }
            }
          }
        }

        if (testUserSet.contains(userId.toLong)) {
          println("for:" + userId)
          println("recedVideos 4 " + userId + " : " + rcedVideos.mkString(","))
          println("idtypeReaosnsM4searchPlayList 4 " + userId + " : " + idtypeReaosnsM4searchPlayList.map(tup => tup._1 + "-" + tup._2.mkString(",")).mkString("|||"))
          println("idtypeReaosnsM4userPrefs_days 4 " + userId + " : " + idtypeReaosnsM4userPrefs_days.map(tup => tup._1 + "-" + tup._2._1.mkString(",") + "=====" + tup._2._2).mkString("|||"))
          println("reasonRecallcntM 4 " + userId + " : " + reasonRecallcntM.map(tup => tup._1 + ":" + tup._2).mkString(","))
        }

        val resultBuffer = mutable.ArrayBuffer[(String, String, String)]()
        if (idtypeReaosnsM4searchPlayList.size > 0) {
          val recallsBySearchPlayList = ArrayBuffer[String]()
          idtypeReaosnsM4searchPlayList.toArray
            .filter { tup => !rcedVideos.contains(tup._1) }
            .sortWith(_._2.size > _._2.size)
            .foreach{tup =>
              if (recallsBySearchPlayList.size < recall_thred4user) {
                val idtype = tup._1
                val sourceS = tup._2
                if (!rcedVideos.contains(idtype) && !isFilterBySourceThredRule(sourceS, reasonRecallcntM, recall_thred4reason)) {
                  recallsBySearchPlayList.append(idtype + ":" + sourceS.mkString("&"))
                  updateResonRecallcntM(sourceS, reasonRecallcntM)
                  rcedVideos.add(idtype)
                }
              }
            }
          resultBuffer.append(("a", userId, recallsBySearchPlayList.mkString(",")))
        }
        if (idtypeReaosnsM4userPrefs_days.size > 0) {
          val recallsByuserPrefs_days = ArrayBuffer[String]()
          idtypeReaosnsM4userPrefs_days.toArray
            .filter { tup => !rcedVideos.contains(tup._1) }
            .sortWith(_._2._2 > _._2._2)
            .foreach{tup =>
              if (recallsByuserPrefs_days.size < recall_thred4user) {
                val idtype = tup._1
                val sourceS = tup._2._1
                if (!rcedVideos.contains(idtype) && !isFilterBySourceThredRule(sourceS, reasonRecallcntM, recall_thred4reason)) {
                  recallsByuserPrefs_days.append(idtype + ":" + sourceS.mkString("&"))
                  updateResonRecallcntM(sourceS, reasonRecallcntM)
                  rcedVideos.add(idtype)
                }
              }
            }
          resultBuffer.append(("b", userId, recallsByuserPrefs_days.mkString(",")))
        }
        resultBuffer
      }
        .cache

    finalRdd.filter(tup => tup._1.equals("a")).map(tup => tup._2 + "\t" + tup._3)
      .repartition(32) // 合并小文件
      .saveAsTextFile(output + "/ibSeqs")
    finalRdd.filter(tup => tup._1.equals("b")).map(tup => tup._2 + "\t" + tup._3)
      .repartition(32) // 合并小文件
      .saveAsTextFile(output + "/ibPrefs")
  }


}
