package com.netease.music.recommend.scala.feedflow.mainpage

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.cf.setContainsItem
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
object mergeKwd {

  val NOW_TIME = System.currentTimeMillis()
  val usefulResourcetypeS = Set[String]("video", "mv", "eventactivity", "concert")

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
    options.addOption("mainpage", true, "log input directory")
    options.addOption("oldMainpage", true, "log input directory")
    options.addOption("days4Kwd", true, "days 4 kwd")
    options.addOption("output", true, "output directory")
//    options.addOption("abtestConfig", true, "abtestConfig file")
//    options.addOption("abtestExpName", true, "abtest Exp Name")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val mergeInput = cmd.getOptionValue("mergeInput")
    val userPref = cmd.getOptionValue("userPref")
    val days4UserPref = cmd.getOptionValue("days4UserPref").toInt
    val mainpageInput = cmd.getOptionValue("mainpage")
    val oldMainpageInput = cmd.getOptionValue("oldMainpage")
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
    // 老首页曝光
    val recedKwd_oldMainpage = spark.sparkContext.textFile(oldMainpageInput)
      .map {line =>
        // keyword: recommendimpress	mainpage	android	129232837	  1520170808000	388455	video	  2	          bySort	null	      -1	      simTagv3-1447101-video	-1	      电视剧
        // guess  : recommendimpress	mainpage	android	1292328832	1520147571000	1428679	video	  1	          hot	    null	      -1	      null	                  -1	      猜你喜欢
        // format : action            page      os      uid         logTime       vid     vType   position    alg     netstatus   sourceId  sourceInfo              groupId   groupName
        // num    : 1                 2         3       4           5             6       7        8          9       10          11        12                      13        14
        val info = line.split("\t")
        val actionUserId = info(3)
        if (info.length >= 14 && !info(13).equals("猜你喜欢")) {
          val kwd = info(13)
          (actionUserId, kwd)
        } else {
          (actionUserId, null)
        }
      }.toDF("userId", "kwd")
      .filter($"kwd".isNotNull)
      .groupBy("userId")
      .agg(
        collect_set("kwd").as("recedKwd_oldMainpage")
      )
    // 新首页曝光日志
    val recedKwd_newMainpage = spark.sparkContext.textFile(mainpageInput)
      .map { line =>
        // action + "\t" + page + "\t" + os + "\t" + userId + "\t" + logTime + "\t" +
        // scene + "\t" + title + "\t" + layout + "\t" + target + "\t" + targetid + "\t" +
        // resource + "\t" + resourceid + "\t" + alg + "\t" + sourceInfo + "\t" + position + "\t" +
        // row + "\t" + column + "\t" + version
        val info = line.split("\t")
        val actionUserId = info(3)
        val scene = info(5)
        if (scene.equals("keyword")) {
          val kwd = info(6)
          (actionUserId, kwd)
        } else {
          (actionUserId, null)
        }
      }.toDF("userId", "kwd")
      .filter($"kwd".isNotNull)
      .groupBy("userId")
      .agg(
        collect_set("kwd").as("recedKwd_newMainpage")
      )


    val mergeInputDir = new Path(mergeInput)
    val mergeInputTableAB = ArrayBuffer[DataFrame]()
    for (status <- fs.listStatus(mergeInputDir)) {
      val path = status.getPath.toString
      println("zhp:" + path)
      val rawTable = spark.sparkContext.textFile(path)
        .map {line =>
          val info = line.split("\t")
          val userId = info(0)
          val recs = info(1)
          (userId, recs)
        }.toDF("userId", "recs")
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
//      .filter(abtestGroupName(configText, abtestExpName)($"userId")==="t1" || $"userId"==="135571358")
      .groupBy("userId")
      .agg(
        collect_set("recs").as("recsSet")
      )

    val rcmdData = mergedData
      .join(recedResources, Seq("userId"), "left_outer")
      .join(recedKwd_oldMainpage, Seq("userId"), "left_outer")
      .join(recedKwd_newMainpage, Seq("userId"), "left_outer")
      .map(row => {
        val userId = row.getAs[String]("userId")
        val recsSet = row.getAs[Seq[String]]("recsSet")
        var recedIdtypes = row.getAs[Seq[String]]("recedIdtypes")
        if (recedIdtypes == null)
          recedIdtypes = Seq[String]()
        var recedKwd_oldMainpage = row.getAs[Seq[String]]("recedKwd_oldMainpage")
        if (recedKwd_oldMainpage == null)
          recedKwd_oldMainpage = Seq[String]()
        var recedKwd_newMainpage = row.getAs[Seq[String]]("recedKwd_newMainpage")
        if (recedKwd_newMainpage == null)
          recedKwd_newMainpage = Seq[String]()
        val recedAidOrKeywords = recedKwd_oldMainpage ++ recedKwd_newMainpage
        if (userId.equals("135571358")) {
          println("for 135571358")
          println("recedAidOrKeywords:" + recedAidOrKeywords)
          println("recedKwd_oldMainpage:" + recedKwd_oldMainpage)
          println("recedKwd_newMainpage:" + recedKwd_newMainpage)
          println("recedIdtypes:" + recedIdtypes)
          println("recsSet:" + recsSet)
          //println("recedVids:" + recedVids)
        }
        val idtypeReasonM = mutable.HashMap[String, mutable.HashSet[String]]()
        val kwdIdtypeM = mutable.HashMap[String, ArrayBuffer[String]]()
        for (recs <- recsSet) {
          for (rec <- recs.split(",")) {
              val recInfo = rec.split(":")
              val kwd = recInfo(0)
              val resourceid = recInfo(1)
              val resourcetype = recInfo(2)
              val idtype = resourceid + "-" + resourcetype

            /*try {*/
              if (!recedIdtypes.contains(idtype)) { // 过滤已推荐idtype
                val alg = recInfo(3)
                var reasonIdtype = "unknow"
                if (recInfo.length >= 5)
                  reasonIdtype = recInfo(4)
                var algWithReason = alg + "-" + reasonIdtype
                if (reasonIdtype.split("-").length == 3)
                  algWithReason = reasonIdtype
                val reasonS = idtypeReasonM.getOrElse(idtype, mutable.HashSet[String]())
                idtypeReasonM.put(idtype, reasonS)
                reasonS.add(algWithReason)

                val idtype_arr = kwdIdtypeM.getOrElse(kwd, ArrayBuffer[String]())
                kwdIdtypeM.put(kwd, idtype_arr)
                if (!idtype_arr.contains(idtype))
                  idtype_arr.append(idtype)
              }

            /*} catch {
            case ex:Exception => println("error info:idtype:" + idtype + ",userId:" + userId + ",recInfo:" + recInfo + ",recs:" + recs)
          }*/
          }
        }

        // 排序
        val result = mutable.ArrayBuffer[String]()
        val kwd_arr = ArrayBuffer[(String, Int, Int)]()
        for ((kwd, idtypeS) <- kwdIdtypeM) {
          val reasonS4kwd = mutable.HashSet[String]()
          val idReasonlength4kwd = ArrayBuffer[(String, Int)]()
          for (idtype <- idtypeS) {
            val reasonS = idtypeReasonM.getOrElse(idtype, mutable.HashSet[String]())
            reasonS4kwd ++= reasonS
            idReasonlength4kwd.append((idtype, reasonS.size))
          }
          kwd_arr.append((kwd, reasonS4kwd.size, idtypeS.size))
          val sortedIdtypes = idReasonlength4kwd.sortWith(_._2 > _._2).map(_._1)  // kwd下的idtype级别按照reasonS.size对resourceidtype排序
          kwdIdtypeM.put(kwd, sortedIdtypes)
        }
        kwd_arr.sortWith(_._3 > _._3).sortWith(_._2 > _._2) // kwd级别按照 1) 总reason number排序 2) 总reason number相同时，按照idtypeS.size排序
          .foreach{tup =>
          val kwd = tup._1
          val idtypeS = kwdIdtypeM(kwd)
          for (idtype <- idtypeS) {
            val reasonS = idtypeReasonM(idtype)
            val alg = reasonS.iterator.next.split("-")(0)
            val reasonS_final = reasonS.filter(line => !line.split("-")(1).equals("unknow"))
            var recInfo = kwd + ":" + idtype.replaceAll("-", ":") + ":" + alg
            if (reasonS_final.size > 0) { // 有推荐理由的增加推荐理由信息
              val reasonStr = reasonS.mkString("&")
              recInfo += ("_" + reasonStr)
            }
            result.append(recInfo)
          }
        }
        if (result.size > 0)
          userId + "\t" + result.mkString(",")
        else
          ""
      })
      .filter(_ != null).filter(_ != "").filter(_ != None)

    // 使用partitionBy的作用：为了改名字（使用part-*****这种类型的名字）
    rcmdData.map({case line => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(20)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/merged_kwd_rcmd")
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
