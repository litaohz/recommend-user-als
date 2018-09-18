package com.netease.music.recommend.scala.feedflow.log

import com.netease.music.recommend.scala.feedflow.log.GetVideoReport.comb
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * 提取不敢兴趣数据
  * Created by hzlvqiang on 2018/5/15.
  */
object GetVideoRejection {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("Music_VideoTimelineRejectMeta", true, "input")
    options.addOption("output", true, "output directory")

    import spark.implicits._

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val uidRejectionData = spark.read.json(cmd.getOptionValue("Music_VideoTimelineRejectMeta"))
       // .withColumn("resourceId", $"resourceId")
      //  .withColumn("type", $"type")
      .withColumn("idtype", idtype($"resourceId", $"type"))

    println("uidRejectionData")
    uidRejectionData.show(10, false)


      val outData = uidRejectionData
      .filter($"idtype" =!= "")
      .groupBy("idtype").agg(count($"userId").as("uid_cnt"))
      .withColumn("idtype_uidcnt", comb($"idtype", $"uid_cnt"))

    outData.select("idtype_uidcnt").repartition(2).write.text(cmd.getOptionValue("output") + "/idtype_rejectUidNum.csv")

  }

  def idtype = udf((id: Long, itype: Long) => {
    if (itype == 1L || itype == 1 || itype == "1") {
      id.toString + ":video"
    } else if (itype == 2) {
      id.toString + ":mv"
    } else {
      ""
    }
  })


}
