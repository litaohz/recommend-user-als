package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetUserPref4vec_days_t {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("userActions", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userActions = cmd.getOptionValue("userActions")
    val output = cmd.getOptionValue("output")
    val userActionsRdd = spark.read.parquet(userActions)
      .rdd
      .map {row =>
        val userId = row.getAs[String]("userId")
        var actionId = row.getAs[String]("actionId")
        val actionType = row.getAs[String]("actionType").toLowerCase
        if (actionType.equals("word"))
          actionId = actionId.replaceAll(",", "，").replaceAll(":", "：").replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", "").replaceAll("《", "<").replaceAll("》", ">")
        val idtype = actionId + "-" + actionType
        val logTime = row.getAs[String]("logTime").toLong
        val ds = row.getAs[String]("ds")
        (userId, (idtype, logTime, ds))
      }.groupByKey
      .map({ case (userId, values) =>

        var remain = false
        val validDsS = values
          .filter(tup => tup._1.endsWith("video") || tup._1.endsWith("mv"))
          .map(tup => tup._3)
          .toSet
        if (validDsS.size > 0)
          remain = true
        if (remain) {
          val resultStr = values
            .filter(tup => validDsS.contains(tup._3))
            .toArray.sortWith(_._2 < _._2)
            .map(tup => tup._1/* + ":" + tup._2 + ":" + tup._3*/).mkString(",")
          userId + "\t" + resultStr
        } else {
          "null"
        }
      }).filter(line => !line.equals("null"))

    userActionsRdd
      .repartition(16) // 合并小文件
      .saveAsTextFile(output)

  }
}
