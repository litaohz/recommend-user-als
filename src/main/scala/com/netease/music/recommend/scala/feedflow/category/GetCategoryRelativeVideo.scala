package com.netease.music.recommend.scala.feedflow.category

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.log4j
import org.apache.log4j.Logger

/**
  * Created by hzzhangjunfei1 on 2017/4/16.
  */
object GetCategoryRelativeVideo {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    //val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoInfoPoolInput", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val videoInfoPoolTable = spark.read.parquet(videoInfoPoolInput)

    // 获取分类相关联的动态
    val categoryRelativeEventRDD = videoInfoPoolTable
      .filter($"category"=!="0_0")
      .rdd
      .map(line => {
        (line.getString(8), line.mkString(":"))
      })
      .groupByKey
      .map({case (key, values) => collectVideoByKey(key, values)})
    categoryRelativeEventRDD.saveAsTextFile(output)

  }


}
