package com.netease.music.recommend.scala.feedflow.mainpage

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.cf.setContainsItem
import com.netease.music.recommend.scala.feedflow.mainpage.mergeKwd.usefulResourcetypeS
import com.netease.music.recommend.scala.feedflow.userProfile.GetUserVideoPref_days.{abtestGroupName, getExistsPathUnderDirectory}
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * merge首页关键词推荐结果
  * Created by hzzhangjunfei on 2018/07/02.
  */
object mergeResource {

  val NOW_TIME = System.currentTimeMillis()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass)

    val options = new Options

    options.addOption("mergeInput", true, "mergeInput")
    options.addOption("userPref", true, "user pref to get reced idtype")
    options.addOption("days4UserPref", true, "days 4 user pref")
    options.addOption("days4Kwd", true, "days 4 kwd")
    options.addOption("output", true, "output directory")
//    options.addOption("abtestConfig", true, "abtestConfig file")
//    options.addOption("abtestExpName", true, "abtest Exp Name")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val mergeInput = cmd.getOptionValue("mergeInput")
    val userPref = cmd.getOptionValue("userPref")
    val days4UserPref = cmd.getOptionValue("days4UserPref").toInt
    val days4Kwd = cmd.getOptionValue("days4Kwd").toInt

    import spark.implicits._
    val fs = FileSystem.get(new Configuration)
//    val configText = spark.read.textFile(cmd.getOptionValue("abtestConfig")).collect()
//    val abtestExpName = cmd.getOptionValue("abtestExpName")
//    for (cf <- configText) {
//      if (cf.contains(abtestExpName)) {
//        println(cf)
//      }
//    }

    // 已推荐idtype
    val existsQualifiedUserPrefPaths = getQualifiedExistsHdfsPaths(userPref, fs, days4UserPref)
    logger.info("existsQualifiedUserPrefPaths:" + existsQualifiedUserPrefPaths.mkString(","))
    val recedResources = spark.read.parquet(existsQualifiedUserPrefPaths : _*)
      .filter(setContainsItem(usefulResourcetypeS)($"vType"))
      .withColumn("idtype", concat_ws("-", $"videoId", $"vType"))
      .groupBy($"actionUserId")
      .agg(
        collect_set($"idtype").as("recedIdtypes")
      ).toDF("userId", "recedIdtypes")


    val mergeInputDir = new Path(mergeInput)
    val mergeInputTableAB = ArrayBuffer[DataFrame]()
    for (status <- fs.listStatus(mergeInputDir)) {
      val path = status.getPath.toString
      println("zhp:" + path)
      val rawTable = spark.sparkContext.textFile(path)
        .map {line =>
          val info = line.split("\t")
          val uidRtype = info(0).split("_")
          val recs = info(1)

          val userId = uidRtype(0)
          val resourcetype = uidRtype(1)
          (userId, resourcetype, recs)
        }.toDF("userId", "resourcetype", "recs")
      mergeInputTableAB.append(rawTable)
    }
    var mergedData = mergeInputTableAB(0)
    if (mergeInputTableAB.length > 1) {
      for (i <- 1 until mergeInputTableAB.length) {
        val rawTable = mergeInputTableAB(i)
        mergedData = mergedData.union(rawTable)
        println("zhp:index:" + i + ",rawTable:" + rawTable.first)
      }
    }
    mergedData = mergedData
//      .filter(abtestGroupName(configText, abtestExpName)($"userId")==="t2" || $"userId"==="135571358")
      .groupBy("userId", "resourcetype")
      .agg(
        collect_set("recs").as("recsSet")
      )

    val rcmdData = mergedData
      .join(recedResources, Seq("userId"), "left_outer")
      .map(row => {
        val userId = row.getAs[String]("userId")
        val resourcetype = row.getAs[String]("resourcetype")
        val recsSet = row.getAs[Seq[String]]("recsSet")
        var recedIdtypes = row.getAs[Seq[String]]("recedIdtypes")
        if (recedIdtypes == null)
          recedIdtypes = Seq[String]()
        if (userId.equals("135571358")) {
          println("for 135571358," + resourcetype)
          println("recedIdtypes:" + recedIdtypes)
          println("recsSet:" + recsSet)
          //println("recedVids:" + recedVids)
        }
        val idtypeReasonM = mutable.LinkedHashMap[String, mutable.HashSet[String]]()
        for (recs <- recsSet) {
          for (rec <- recs.split(",")) {
              val recInfo = rec.split(":")
              val resourceid = recInfo(0)
              val resourcetype = recInfo(1)
              val idtype = resourceid + "-" + resourcetype

              if (!recedIdtypes.contains(idtype)) { // 过滤已推荐idtype
                val alg = recInfo(2)
                var reasonIdtype = "unknow"
                if (recInfo.length >= 4) {
                  reasonIdtype = recInfo(3)
                  if (!reasonIdtype.contains("-"))
                    reasonIdtype = "kwd-" + reasonIdtype
                }
                val algWithReason = alg + "-" + reasonIdtype
                val reasonS = idtypeReasonM.getOrElse(idtype, mutable.HashSet[String]())
                idtypeReasonM.put(idtype, reasonS)
                reasonS.add(algWithReason)

              }
          }
        }

        // 排序
        val result = mutable.ArrayBuffer[String]()
        for ((idtype, reasonS) <- idtypeReasonM.toArray.sortWith(_._2.size > _._2.size)) {
            val alg = reasonS.iterator.next.split("-")(0)
            val reasonS_final = reasonS.filter(line => !line.split("-")(1).equals("unknow"))
            var recInfo = idtype.replaceAll("-", ":") + ":" + alg
            if (reasonS_final.size > 0) { // 有推荐理由的增加推荐理由信息
              val reasonStr = reasonS.mkString("&")
              recInfo += ("_" + reasonStr)
            }
            result.append(recInfo)
        }
        if (result.size > 0)
          userId + "_" + resourcetype + "\t" + result.mkString(",")
        else
          ""
      })
      .filter(_ != null).filter(_ != "").filter(_ != None)

    // 使用partitionBy的作用：为了改名字（使用part-*****这种类型的名字）
    rcmdData.map({case line => (line.toString.split("_",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/merged_resource_rcmd")
  }

  def getQualifiedExistsHdfsPaths(inputDir:String, fs: FileSystem, days:Int):Seq[String]= {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val timeInMillis = System.currentTimeMillis
    val millisInDay = 24 * 3600 * 1000l
    val qualifiedLogDateSeq4UserPref = for (i <- (1 to days)) yield {
      sdf.format(new Date(timeInMillis - i * millisInDay))
    }

    val existsUserPrefInputPaths = getExistsPathUnderDirectory(inputDir, fs)
    val existsQualifiedUserPrefPaths = existsUserPrefInputPaths.filter{path =>
      var remain = false
      qualifiedLogDateSeq4UserPref.foreach{qualifiedDate =>
        if (path.contains(qualifiedDate))
          remain = true
      }
      remain
    }
    existsQualifiedUserPrefPaths.toSeq
  }
}
