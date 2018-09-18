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
object SceneDocvecItembased {

  def seq2Vec = udf((features:String) => Vectors.dense(features.split(",").map(_.toDouble)))

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("vSimsFromDocvec", true, "input directory")
    options.addOption("vSimsFromDocvec_mix", true, "input directory")
    options.addOption("userSeqs", true, "input directory")
    options.addOption("userPref_days", true, "input directory")
    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("stopwords", true, "stopwords file")
    options.addOption("abtestConfig", true, "abtestConfig file")
    options.addOption("abtestExpName", true, "abtest Exp Name")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val vSimsFromDocvec = cmd.getOptionValue("vSimsFromDocvec")
    val vSimsFromDocvec_mix = cmd.getOptionValue("vSimsFromDocvec_mix")
    val userSeqs = cmd.getOptionValue("userSeqs")
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

    val stopwordsPath = cmd.getOptionValue("stopwords")
    println("stopwordsPath:" + stopwordsPath)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //println("fs 4 stopwordsPath:" + fs)
    val stopwordsS = loadLines2SetChar(fs, new Path(stopwordsPath))
    println("stopwordsS size:" + stopwordsS.size + "=========" + stopwordsS.mkString(","))

    import spark.implicits._
    val userRcedVideosDf = spark.read.textFile(user_rced_video).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userId", "rcedVideos")


    val playsearchDf = spark.sparkContext.textFile(vSimsFromDocvec)
      .map { row =>
        val info = row.split("\t")
        val idtype = info(0)
        val simIdtypes = info(1).split(",")
        (idtype, simIdtypes)
      }.toDF("idtype", "simIdtypes_playsearch")
    val mixDf = spark.sparkContext.textFile(vSimsFromDocvec_mix)
      .map { row =>
        val info = row.split("\t")
        val idtype = info(0)
        val simIdtypes = info(1).split(",").filter(item => (item.contains("-video:") || item.contains("-mv:")))
        (idtype, simIdtypes)
      }.toDF("idtype", "simIdtypes_mix")
    val mergeRdd = mixDf
      .join(playsearchDf, Seq("idtype"), "outer")
      .rdd
      .map{row =>
        val idtype = row.getAs[String]("idtype")
        val simIdtypes_mix = row.getAs[Seq[String]]("simIdtypes_mix")
        val simIdtypes_playsearch = row.getAs[Seq[String]]("simIdtypes_playsearch")

        val addedS = mutable.HashSet[String]()
        val sims_playsearch = ArrayBuffer[(String, Float)]()
        if (simIdtypes_playsearch != null) {
          simIdtypes_playsearch.foreach{str =>
            val itemInfo = str.split(":")
            val idtype = itemInfo(0)
            val resourcetype = idtype.split("-")(1)
            if (recommendableResourcetypeS.contains(resourcetype) && addedS.add(idtype)) {
              val simScore = itemInfo(1).toFloat
              sims_playsearch.append((idtype, simScore))
            }
          }
        }
        val sims_mix = ArrayBuffer[(String, Float)]()
        if (simIdtypes_mix != null) {
          simIdtypes_mix.foreach{str =>
            val itemInfo = str.split(":")
            val idtype = itemInfo(0)
            val resourcetype = idtype.split("-")(1)
            if (recommendableResourcetypeS.contains(resourcetype) && addedS.add(idtype)) {
              val simScore = itemInfo(1).toFloat
              sims_mix.append((idtype, simScore))
            }
          }
        }
        (idtype,
          (sims_mix.map(t => t._1 + ":" + t._2.formatted("%.4f")).mkString(","),
            sims_playsearch.map(t => t._1 + ":" + t._2.formatted("%.4f")).mkString(",")
          )
        )
      }.filter(tup => (tup._2._1.length > 0 || tup._2._2.length > 0))
        .cache

//    mergeRdd
//      .map(tup => tup._1 + "\t" + tup._2._1 + "\t" + tup._2._2)
//      .repartition(8) // 合并小文件
//      .saveAsTextFile(output + "/relations")

    val vSimsM = mergeRdd/*.repartition(8)*/.collect
      .toMap
    val bc_vSimsM = spark.sparkContext.broadcast(vSimsM)

    val userSeqsDf = spark.sparkContext.textFile(userSeqs)
      .map{line =>
        val info = line.split("\t")
        val userId = info(0)
        val userSeqsStr = info(1)
        (userId, userSeqsStr)
      }.toDF("userId", "userSeqsStr")
    val userPref_daysDf = spark.sparkContext.textFile(userPref_days)
      .map{line =>
        val info = line.split("\t")
        val userId = info(0)
        val userPref_daysStr = info(1)
        (userId, userPref_daysStr)
      }.toDF("userId", "userPref_daysStr")
    val finalRdd = userSeqsDf
      .join(userPref_daysDf, Seq("userId"), "outer")
      .join(userRcedVideosDf, Seq("userId"), "left_outer")
      .rdd
      .flatMap{row =>
        val userId = row.getAs[String]("userId")
        val rcedVideosStr = row.getAs[String]("rcedVideos")
        val userSeqsStr = row.getAs[String]("userSeqsStr")
        val userPref_daysStr = row.getAs[String]("userPref_daysStr")

        val rcedVideos = mutable.HashSet[String]()
        if (rcedVideosStr != null) {
          for (rvid <- rcedVideosStr.split(",")) {
            rcedVideos.add(rvid + ":video")
          }
        }
        val idtypeReaosnsM4userSeqsList = mutable.Map[String, mutable.Set[String]]()
        val idtypeReaosnsM4userPrefs_days = mutable.Map[String, (mutable.Set[String], Double)]()
        val reasonRecallcntM = mutable.Map[String, Int]()
        val recall_thred4reason = 5
        val recall_thred4user = 30

        if (testUserSet.contains(userId.toLong) || abtestGroupName(configText, abtestExpName, userId).equals("t1")) {
          if (userSeqsStr != null) {
            userSeqsStr.split(",").foreach {item =>
              if (bc_vSimsM.value.contains(item)) {
                val relations = bc_vSimsM.value(item)

                val relations_mix = relations._1.split(",")
                if (!relations._1.isEmpty) {
                  relations_mix.foreach { tupStr =>
                    val tup = tupStr.split(":")
                    val simIdtype = tup(0).replaceAll("-", ":")
                    //val recallcnt_eachReason = reasonRecallcntM.getOrElse(item, 0)
                    if (!rcedVideos.contains(simIdtype)/* && recallcnt_eachReason < recall_thred4reason*/) {
                      val reasonS = idtypeReaosnsM4userSeqsList.getOrElse(simIdtype, mutable.Set[String]())
                      idtypeReaosnsM4userSeqsList.put(simIdtype, reasonS)
                      reasonS.add(item)
                      //reasonRecallcntM.put(item, recallcnt_eachReason+1)
                    }
                  }
                }
                val relations_playsearch = relations._2.split(",")
                if (!relations._2.isEmpty) {
                  relations_playsearch.foreach { tupStr =>
                    val tup = tupStr.split(":")
                    val simIdtype = tup(0).replaceAll("-", ":")
                    //val recallcnt_eachReason = reasonRecallcntM.getOrElse(item, 0)
                    if (!rcedVideos.contains(simIdtype)/* && recallcnt_eachReason < recall_thred4reason*/) {
                      val reasonS = idtypeReaosnsM4userSeqsList.getOrElse(simIdtype, mutable.Set[String]())
                      idtypeReaosnsM4userSeqsList.put(simIdtype, reasonS)
                      reasonS.add(item)
                      //reasonRecallcntM.put(item, recallcnt_eachReason+1)
                    }
                  }
                }
              }
            }
          }
        }

        if (testUserSet.contains(userId.toLong) || abtestGroupName(configText, abtestExpName, userId).equals("t2")) {
          if (userPref_daysStr != null) {
            userPref_daysStr.split(",").foreach {item =>
              val itemInfo = item.split(":")
              val idtype = itemInfo(0) + "-" + itemInfo(1)
              if (bc_vSimsM.value.contains(idtype)) {
                val prefScore = itemInfo(2).toDouble
                val relations = bc_vSimsM.value(idtype)
                val relations_mix = relations._1.split(",")
                if (!relations._1.isEmpty) {
                  relations_mix.foreach { tupStr =>
                    val tup = tupStr.split(":")
                    val simIdtype = tup(0).replaceAll("-", ":")
                    //val recallcnt_eachReason = reasonRecallcntM.getOrElse(idtype, 0)
                    if (!rcedVideos.contains(simIdtype)/* && recallcnt_eachReason < recall_thred4reason*/) {
                      //                      try {
                      val simScore = tup(1).toDouble
                      val (reasonS, simDotPref_now) = idtypeReaosnsM4userPrefs_days.getOrElse(simIdtype, (mutable.Set[String](), 0.0d))
                      reasonS.add(idtype)
                      idtypeReaosnsM4userPrefs_days.put(simIdtype, (reasonS, Math.max(simDotPref_now, simScore * prefScore)))
                      //reasonRecallcntM.put(idtype, recallcnt_eachReason + 1)
                      //                      } catch {
                      //                        case ex: ArrayIndexOutOfBoundsException => {
                      //                          println("zhp:relations_mix|||||" + relations._1 + "|||||" + relations._2 + "|||||" + tupStr + "|||||" + tup(0) + "|||||" + ex.getMessage + "|||||" + relations_mix.mkString(",") + "|||||" + relations_mix.length)
                      //                          throw ex
                      //                        }
                      //                      }
                    }
                  }
                }
                val relations_playsearch = relations._2.split(",")
                if (!relations._2.isEmpty) {
                  relations_playsearch.foreach { tupStr =>
                    val tup = tupStr.split(":")
                    val simIdtype = tup(0).replaceAll("-", ":")
                    //val recallcnt_eachReason = reasonRecallcntM.getOrElse(idtype, 0)
                    if (!rcedVideos.contains(simIdtype)/* && recallcnt_eachReason < recall_thred4reason*/) {
                      //                      try {
                      val simScore = tup(1).toDouble
                      val (reasonS, simDotPref_now) = idtypeReaosnsM4userPrefs_days.getOrElse(simIdtype, (mutable.Set[String](), 0.0d))
                      reasonS.add(idtype)
                      idtypeReaosnsM4userPrefs_days.put(simIdtype, (reasonS, Math.max(simDotPref_now, simScore * prefScore)))
                      //reasonRecallcntM.put(idtype, recallcnt_eachReason + 1)
                      //                      } catch {
                      //                        case ex: ArrayIndexOutOfBoundsException => {
                      //                          println("zhp:relations_playsearch|||||" + relations._1 + "|||||" + relations._2 + "|||||" + tupStr + "|||||" + tup(0) + "|||||" + ex.getMessage + "|||||" + relations_playsearch.mkString(",") + "|||||" + relations_playsearch.length)
                      //                          throw ex
                      //                        }
                      //                      }
                    }
                  }
                }
              }
            }
          }
        }

        if (testUserSet.contains(userId.toLong)) {
          println("for:" + userId)
          println("recedVideos 4 " + userId + " : " + rcedVideos.mkString(","))
          println("idtypeReaosnsM4userSeqs 4 " + userId + " : " + idtypeReaosnsM4userSeqsList.map(tup => tup._1 + "-" + tup._2.mkString(",")).mkString("|||"))
          println("idtypeReaosnsM4userPrefs_days 4 " + userId + " : " + idtypeReaosnsM4userPrefs_days.map(tup => tup._1 + "-" + tup._2._1.mkString(",") + "=====" + tup._2._2).mkString("|||"))
          println("reasonRecallcntM 4 " + userId + " : " + reasonRecallcntM.map(tup => tup._1 + ":" + tup._2).mkString(","))
        }

        val resultBuffer = mutable.ArrayBuffer[(String, String, String)]()
        if (idtypeReaosnsM4userSeqsList.size > 0) {
          val recallsBySeqs = ArrayBuffer[String]()
          idtypeReaosnsM4userSeqsList.toArray
            .filter { tup => !rcedVideos.contains(tup._1) }
            .sortWith(_._2.size > _._2.size)
            .slice(0, recall_thred4user)
            .foreach{tup =>
              if (recallsBySeqs.size < recall_thred4user) {
                val idtype = tup._1
                val sourceS = tup._2.map{reason =>
                  val idType = reason.split("-")
                  (norm(idType.slice(0, idType.length-1).mkString("-"), stopwordsS) + "-" + idType(idType.length-1)).replaceAll("-word", "-tag")
                }
                if (!rcedVideos.contains(idtype) && !isFilterBySourceThredRule(sourceS, reasonRecallcntM, recall_thred4reason)) {
                  recallsBySeqs.append(idtype + ":" + sourceS.mkString("&"))
                  updateResonRecallcntM(sourceS, reasonRecallcntM)
                  rcedVideos.add(idtype)
                }
              }
            }
          resultBuffer.append(("a", userId, recallsBySeqs.mkString(",")))
        }
        if (idtypeReaosnsM4userPrefs_days.size > 0) {
          val recallsByuserPrefs_days = ArrayBuffer[String]()
          idtypeReaosnsM4userPrefs_days.toArray
            .filter { tup => !rcedVideos.contains(tup._1) }
            .sortWith(_._2._2 > _._2._2)
            .foreach{tup =>
              if (recallsByuserPrefs_days.size < recall_thred4user) {
                val idtype = tup._1
                val sourceS = tup._2._1.map{reason =>
                  val idType = reason.split("-")
                  (norm(idType.slice(0, idType.length-1).mkString("-"), stopwordsS) + "-" + idType(idType.length-1)).replaceAll("-word", "-tag")
                }
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
