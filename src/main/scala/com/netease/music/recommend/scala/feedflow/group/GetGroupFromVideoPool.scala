package com.netease.music.recommend.scala.feedflow.group

import java.io.InputStream

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.netease.music.recommend.scala.feedflow.utils.userFunctions.splitByDelim


/**
  * Created by hzzhangjunfei1 on 2017/8/1.
  */
object GetGroupFromVideoPool {

  case class VideoTagInfo(videoId:Long, tagIdStr:String, tagNameStr:String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoPool", true, "input directory")
    options.addOption("videoTag", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoPoolInput = cmd.getOptionValue("videoPool")
    val videoTagInput = cmd.getOptionValue("videoTag")
    val outputPath = cmd.getOptionValue("output")

    val stream:InputStream = getClass.getResourceAsStream("/groupTagMappings")
    val tagGroupM = scala.collection.mutable.HashMap[String, scala.collection.mutable.Set[String]]()
    scala.io.Source.fromInputStream(stream)
      .getLines
      .foreach{line =>
        val info = line.split("\t")
        val group = info(0)
        val tags = info(1).split(",")
        tags.foreach {tag =>
          val groups = tagGroupM.getOrElse(tag, scala.collection.mutable.Set())
          groups.add(group)
          tagGroupM.put(tag, groups)
        }
      }
    logger.info("tagGroupMap:" + tagGroupM.toString)
    val broadcaseTagGroupM = spark.sparkContext.broadcast(tagGroupM)

    import spark.implicits._

    val tagNameMappingTable = spark.sparkContext.textFile(videoTagInput)
      .map { line =>
        val info = line.split("\01")
        val tagId = info(0)
        val tagName = info(1)
        TagMap(tagId, tagName)
      }
      .toDF

    val videoPoolTable = spark.read.parquet(videoPoolInput)

    val videoTagTable = videoPoolTable
      .filter($"vType"==="video")
      .select($"videoId", mergeTagIds($"userTagIds", $"auditTagIds").as("tagIds"))
      .withColumn("tagId", explode(splitByDelim("_tab_")($"tagIds")))
      .join(tagNameMappingTable, Seq("tagId"), "left_outer")
      .na.fill("null", Seq("tagName"))
      .filter($"tagName"=!="null")
      .select("videoId", "tagId", "tagName")

    val finalDataset = videoTagTable
      .rdd
        .map(line => (line.getLong(0), (line.getString(1), line.getString(2))))
      .groupByKey
      .map {case (key, value) => collect(key, value)}
      .toDF
      .withColumn("groups", getGroups(broadcaseTagGroupM)($"tagNameStr"))

    finalDataset.write.parquet(outputPath)

//    finalDataset
//      .withColumn("group", explode(splitByDelim("_tab_")($"groups")))
//      .groupBy($"group")
//      .agg(countDistinct($"videoId").as("videoCnt"))
  }

  def collect(key:Long, values:Iterable[(String, String)]):VideoTagInfo = {

    val tagIdStr = values.map(line => line._1).mkString("_tab_")
    val tagNameStr = values.map(line => line._2).mkString("_tab_")
    VideoTagInfo(key, tagIdStr, tagNameStr)
  }


}
