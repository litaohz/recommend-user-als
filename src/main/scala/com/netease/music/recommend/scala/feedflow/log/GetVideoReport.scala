package com.netease.music.recommend.scala.feedflow.log

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.collection.mutable
/**
  * 提取举报数据
  * Created by hzlvqiang on 2018/5/15.
  */
object GetVideoReport {

  def toLong= udf((line: Row) => {

  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("lst_video_report", true, "input")
    options.addOption("new_video_report", true, "input")
    options.addOption("output", true, "output directory")

    import spark.implicits._

    val parser = new PosixParser
    val cmd = parser.parse(options, args)
    val newVideoReport = spark.read.textFile(cmd.getOptionValue("new_video_report"))
        .map(line => {
          // click	mvplay	android	119377688	1526301384000	5898074	mv	-1	report	0	-1	null	-1
          val ts = line.split("\t")
          val uid = ts(3)
          val idtype = ts(5) + ":" + ts(6)
          val actionType = ts(8)
          (uid, idtype, actionType)
        }).toDF("uid", "idtype", "actionType")
      .filter($"actionType" === "report")

    var reportAll = newVideoReport.groupBy("uid").agg(collect_list($"idtype").as("new_idtype_list"))

    if (cmd.hasOption("lst_video_report")) {
      val lstVideoReport = spark.read.parquet(cmd.getOptionValue("lst_video_report") + "/uid_reportVideo")
        /*
        .map(line => {
          val ts = line.split("\t", 2)
          val uid = ts(0)
          val lstIdtypes = ts(1).split(",")
          (uid, lstIdtypes)
        }).toDF("uid", "lst_idtype_list")
        */
      reportAll = reportAll.join(lstVideoReport, Seq("uid"), "outer")
    }
    // reportAll.na.fill(null, Seq("lst_idtype_list", "new_idtype_list"))

    val uidReportData = reportAll.map(row => {
      val uid = row.getAs[String]("uid")
      val idtypeSet = mutable.HashSet[String]()
      try {
        val lstIdTypeList = row.getAs[Seq[String]]("report_idtypes")
        appendList(idtypeSet, lstIdTypeList)
      }catch {
        case e: Exception => {}
      }
      try {
        val newIdTypeList = row.getAs[Seq[String]]("new_idtype_list")
        appendList(idtypeSet, newIdTypeList)
      } catch {
        case e:Exception => {}
      }
      // val idtypeSet = join(lstIdTypeList, newIdTypeList)
      (uid, idtypeSet.toArray)
    }).toDF("uid", "report_idtypes")

    println("## uid -> report videos ##")
    uidReportData.repartition(5).write.parquet(cmd.getOptionValue("output") + "/uid_reportVideo")


    println("## video -> report video uid number ##")
    val idtypeReportNum = uidReportData.flatMap(row => {
      val idtypeUid = mutable.ArrayBuffer[(String, Int)]()
      val reportIdtypes = row.getAs[Seq[String]]("report_idtypes")
      for (idtype <- reportIdtypes) {
        idtypeUid.append((idtype, 1))
      }
      idtypeUid
    }).toDF("idtype", "uid")
        .groupBy($"idtype").agg(count($"uid").as("uid_cnt"))
        .withColumn("idtype_uidcnt", comb($"idtype", $"uid_cnt"))

    idtypeReportNum.select("idtype", "uid_cnt").repartition(2).write.parquet(cmd.getOptionValue("output") + "/idtype_reportUidNum")

    idtypeReportNum.select("idtype_uidcnt").repartition(2).write.text(cmd.getOptionValue("output") + "/idtype_reportUidNum.csv")

  }

  def appendList(idtypeSet: mutable.HashSet[String], idTypeList: Seq[String]) {
    if (idTypeList != null) {
      for (idtype <- idTypeList) {
        idtypeSet.add(idtype)
      }
    }
    idtypeSet
  }


  def join(lstIdTypeList: Seq[String], newIdTypeList: Seq[String]) = {
    val set = mutable.HashSet[String]()
    if (lstIdTypeList != null) {
      for (idtype <- lstIdTypeList) {
        set.add(idtype)
      }
    }
    if (newIdTypeList != null) {
      for (idtype <- newIdTypeList) {
        set.add(idtype)
      }
    }
    set.toArray
  }


  def comb = udf((uid: String, cnt: Int) => {
    uid + "\t" + cnt.toString
  })

}
