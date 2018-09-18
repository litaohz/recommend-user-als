package com.netease.music.recommend.scala.feedflow.userbased

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

import scala.collection.mutable

/**
  * Created by hzzhangjunfei1 on 2018/8/8.
  */
object DocvecUserbased {

  def seq2Vec = udf((features:String) => Vectors.dense(features.split(",").map(_.toDouble)))

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("simsFromKmeans", true, "input directory")
    options.addOption("userSearchPlayList", true, "input directory")
    options.addOption("userPref_days", true, "input directory")
    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("abtestConfig", true, "abtestConfig file")
    options.addOption("abtestExpName", true, "abtest Exp Name")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val simsFromKmeans = cmd.getOptionValue("simsFromKmeans")
    val userSearchPlayList = cmd.getOptionValue("userSearchPlayList")
    val userPref_days = cmd.getOptionValue("userPref_days")
    val user_rced_video = cmd.getOptionValue("user_rced_video")
    val parts = 3500
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

    val simsFromKmeansDf = spark.sparkContext.textFile(simsFromKmeans)
      .flatMap { row =>
        val info = row.split("\t")
        val userId = info(0)
        val sims = info(1).split(",")
        for (simStr <- sims) yield {
          val simInfo = simStr.split(":")
          val simUserId = simInfo(0)
          val simScore = simInfo(1).toDouble
          (userId, simUserId, simScore)
        }
      }.toDF("userId", "simUserId", "simScore")

    val userSearchPlayListDf = spark.sparkContext.textFile(userSearchPlayList)
      .map{line =>
        val info = line.split("\t")
        val simUserId = info(0)
        val searchPlayListStr = info(1)
        (simUserId, searchPlayListStr)
      }.toDF("simUserId", "searchPlayListStr")

    val userPref_daysDf = spark.sparkContext.textFile(userPref_days)
      .map{line =>
        val info = line.split("\t")
        val simUserId = info(0)
        val userPref_daysStr = info(1)
        (simUserId, userPref_daysStr)
      }.toDF("simUserId", "userPref_daysStr")

    val finalRdd = simsFromKmeansDf
      .join(userSearchPlayListDf, Seq("simUserId"), "left_outer")
      .join(userPref_daysDf, Seq("simUserId"), "left_outer")
      .join(userRcedVideosDf, Seq("userId"), "left_outer")
      .rdd
      .map{row =>
        val userId = row.getAs[String]("userId")
        val rcedVideos = row.getAs[String]("rcedVideos")
        val simUserId = row.getAs[String]("simUserId")
        val simScore = row.getAs[Double]("simScore")
        val searchPlayListStr = row.getAs[String]("searchPlayListStr")
        val userPref_daysStr = row.getAs[String]("userPref_daysStr")
        ((userId, rcedVideos), (simUserId, simScore, searchPlayListStr, userPref_daysStr))
      }.groupByKey
      .flatMap ({ case (key, values) =>
        val userId = key._1
        val rcedVstr = key._2
        val rcedVideos = mutable.HashSet[String]()
        if (rcedVstr != null) {
          for (rvid <- rcedVstr.split(",")) {
            rcedVideos.add(rvid + ":video")
          }
        }
        val idtypeReaosnsM4searchPlayList = mutable.Map[String, mutable.Set[String]]()
        val idtypeReaosnsM4userPrefs_days = mutable.Map[String, (mutable.Set[String], Double)]()

        values.foreach { tup =>
          val simUserId = tup._1
          val simScore = tup._2
          val searchPlayListStr = tup._3
          if (testUserSet.contains(userId.toLong) || abtestGroupName(configText, abtestExpName, userId).equals("t1")) {
            if (searchPlayListStr != null) {
              searchPlayListStr.split(",").foreach { item =>
                val idtype = item.replaceAll("-", ":")
                if (!rcedVideos.contains(idtype)) {
                  val reasonS = idtypeReaosnsM4searchPlayList.getOrElse(idtype, mutable.Set[String]())
                  idtypeReaosnsM4searchPlayList.put(idtype, reasonS)
                  reasonS.add(simUserId + "-user")
                }
              }
            }
          }
          val userPref_daysStr = tup._4
          if (testUserSet.contains(userId.toLong) || abtestGroupName(configText, abtestExpName, userId).equals("t2") || abtestExpName.equals("videorcmd-dveb")) {
            if (userPref_daysStr != null) {
              userPref_daysStr.split(",").foreach { item =>
                val itemInfo = item.split(":")
                val idtype = itemInfo(0) + ":" + itemInfo(1)
                if (!rcedVideos.contains(idtype)) {
                  val (reasonS, simDotPref_now) = idtypeReaosnsM4userPrefs_days.getOrElse(idtype, (mutable.Set[String](), 0.0d))
                  reasonS.add(simUserId + "-user")
                  idtypeReaosnsM4userPrefs_days.put(idtype, (reasonS, Math.max(simDotPref_now, simScore * itemInfo(2).toDouble)))
                }
              }
            }
          }
        }
        if (testUserSet.contains(userId.toLong)) {
          println("for:" + userId)
          println("recedVideos 4 " + userId + " : " + rcedVideos.mkString(","))
          println("values 4" + userId + " : " + values.mkString("|||||||||||"))
          println("idtypeReaosnsM4searchPlayList 4 " + userId + " : " + idtypeReaosnsM4searchPlayList.map(tup => tup._1 + "-" + tup._2.mkString(",")).mkString("|||"))
          println("idtypeReaosnsM4userPrefs_days 4 " + userId + " : " + idtypeReaosnsM4userPrefs_days.map(tup => tup._1 + "-" + tup._2._1.mkString(",") + "=====" + tup._2._2).mkString("|||"))
        }

        val resultBuffer = mutable.ArrayBuffer[(String, String, String)]()
        if (idtypeReaosnsM4searchPlayList.size > 0) {
          val result = idtypeReaosnsM4searchPlayList.toArray
            .filter { tup => !rcedVideos.contains(tup._1) }
            .sortWith(_._2.size > _._2.size)
            .slice(0, 30)
          result.foreach(tup => rcedVideos.add(tup._1))
          val recallsBySearchPlayList = result
            .map{tup => tup._1 + ":" + tup._2.mkString("&")}
            .mkString(",")
          resultBuffer.append(("a", userId, recallsBySearchPlayList))
        }
        if (idtypeReaosnsM4userPrefs_days.size > 0) {
          val result = idtypeReaosnsM4userPrefs_days.toArray
//            .map(tup => (tup._1, (tup._2._1, tup._2._2/tup._2._1.size)))
            .filter { tup => !rcedVideos.contains(tup._1) }
            .sortWith(_._2._2 > _._2._2)
            .slice(0, 30)
          result.foreach(tup => rcedVideos.add(tup._1))
          val recallsByuserPrefs_days = result
            .map{tup => tup._1 + ":" + tup._2._1.mkString("&")/* + ":" + tup._2._2.formatted("%.4f")*/}
            .mkString(",")
          resultBuffer.append(("b", userId, recallsByuserPrefs_days))
        }
//        if (idtypeReaosnsM4userPrefs_days.size > 0) {
//          val result = idtypeReaosnsM4userPrefs_days.toArray
//            .filter { tup => !rcedVideos.contains(tup._1) }
//            .sortWith(_._2._1.size > _._2._1.size)
//            .slice(0, 30)
//          result.foreach(tup => rcedVideos.add(tup._1))
//          val recallsByuserPrefs_days = result
//            .map{tup => tup._1 + ":" + tup._2._1.mkString("&")}
//            .mkString(",")
//          resultBuffer.append(("c", userId, recallsByuserPrefs_days))
//        }
        resultBuffer
      })

    finalRdd.filter(tup => tup._1.equals("a")).map(tup => tup._2 + "\t" + tup._3)
      .repartition(32) // 合并小文件
      .saveAsTextFile(output + "/ubSeqs")
    finalRdd.filter(tup => tup._1.equals("b")).map(tup => tup._2 + "\t" + tup._3)
      .repartition(32) // 合并小文件
      .saveAsTextFile(output + "/ubPrefs")
//    finalRdd.filter(tup => tup._1.equals("c")).map(tup => tup._2 + "\t" + tup._3).saveAsTextFile(output + "/ubPrefs2")
  }


}
