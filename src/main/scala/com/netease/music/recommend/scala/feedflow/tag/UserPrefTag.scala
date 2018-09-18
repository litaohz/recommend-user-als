package com.netease.music.recommend.scala.feedflow.tag

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession

/**
  *
  * 获取用户偏好tag
  *
  * Created by hzlvqiang on 2017/12/8.
  */
object UserPrefTag {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._
    val options = new Options()
    options.addOption("user_pref_tags", true, "user_video_pref")
    options.addOption("pref_ratio", true, "pref_ratio")
    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    spark.read.textFile(cmd.getOptionValue("user_pref_tags")).map(line => {
      // uid T1 tag:score,tag:score
      val uidTTags = line.split("\t")

      if (uidTTags(1).equals("T4") && !uidTTags(2).isEmpty) {
        val ts = uidTTags(2).split(",")
        //val sb = new mutable.StringBuilder()
        val topPrefs = mutable.ArrayBuffer[String]()
        for (i <- (ts.size * 0.6).toInt until ts.size) {
          topPrefs.append(ts(i).split(":")(0))
        }
        uidTTags(0) + "\t" + topPrefs.mkString(",")
      } else {
        null
      }
    }).filter(_ != null).map(line => (line.split("\t")(0).toLong, line)).rdd
      .partitionBy(new HashPartitioner(10))
      .map(_._2)
      .saveAsTextFile(cmd.getOptionValue("output"))


    //repartition(50).write.text(cmd.getOptionValue("output"))



  }



}
