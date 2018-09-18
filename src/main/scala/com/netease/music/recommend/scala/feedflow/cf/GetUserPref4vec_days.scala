package com.netease.music.recommend.scala.feedflow.cf

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.getExistsPathUnderDirectory
import com.netease.music.recommend.scala.feedflow.userProfile.GetUserVideoPref_days.isNotLike
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetUserPref4vec_days {


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

    import spark.implicits._
    val userActionsRdd = spark.read.parquet(userActions)
      .rdd
      .map {row =>
        val userId = row.getAs[String]("userId")
        val actionId = row.getAs[String]("actionId")
        val actionType = row.getAs[String]("actionType")
        val idtype = actionId + "-" + actionType
        val logTime = row.getAs[String]("logTime").toLong
        val ds = row.getAs[String]("ds")
        (userId, (idtype, logTime, ds))
      }.groupByKey
      .map({ case (userId, values) =>

        var remain = false
        values.foreach { tup =>
          if (tup._1.endsWith("video") || tup._1.endsWith("mv"))
            remain = true
        }
          if (remain) {
            userId + "\t" + values.toArray.sortWith(_._2 < _._2).map(tup => tup._1).mkString(",")
          } else {
            "null"
          }
      }).filter(line => !line.equals("null"))

    userActionsRdd
      .repartition(16) // 合并小文件
      .saveAsTextFile(output)

  }


  def getPlayratio = udf((viewTime:Long, duration:Double) => {
    if (duration > 0)
      viewTime / duration
    else
      0.0d
  })

}
