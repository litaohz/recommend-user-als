package com.netease.music.recommend.scala.feedflow.cf

import java.io.InputStream

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object SourceSimCFMerge {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("mergeCFInput", true, "input directory, mergeInput")
    options.addOption("source_resourcetype", true, "source item resource type to filter")
    options.addOption("sim_resourcetype", true, "similar item resource type to filter")
    options.addOption("expPart", true, "experiment name")
    options.addOption("videoPool", true, "video pool input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val mergeCFInput = cmd.getOptionValue("mergeCFInput")
    val source_resourcetype = cmd.getOptionValue("source_resourcetype")
    val sim_resourcetype = cmd.getOptionValue("sim_resourcetype")
    val expPart = cmd.getOptionValue("expPart")
    val videoPool = cmd.getOptionValue("videoPool")
    val output = cmd.getOptionValue("output")

    val conf_file_name = "/mergeCFMappings" + expPart
    println("conf_file_name:" + conf_file_name)
    val confStream:InputStream = getClass.getResourceAsStream(conf_file_name)
    val confM = scala.io.Source.fromInputStream(confStream).getLines.map {line =>
      val info = line.split("\t")
      (info(0), info(1))
    }.toMap[String, String]

    import spark.implicits._
    val videoPoolS = spark.read.parquet(videoPool)
      .select(
        concat_ws("-", $"videoId", $"vType").as("idtype")
      )
      .rdd
      .map(line => line.getAs[String](0))
      .collect
      .toSet[String]
    //val broad_videoPoolS = spark.sparkContext.broadcast(videoPoolS)

    val fs = FileSystem.get(new Configuration)
    val mergeCFInputDir = new Path(mergeCFInput)
    val mergeCFRawTableAB = scala.collection.mutable.ArrayBuffer[DataFrame]()
    for (status <- fs.listStatus(mergeCFInputDir)) {
      val path = status.getPath.toString
      val basedir = path.split("/")(10)
      if (confM.contains(basedir)) {
        println("zhp:\t" + basedir + "\t" + confM(basedir))
        val recalltype = confM(basedir)
        val broad_recalltype = spark.sparkContext.broadcast(recalltype)
        if (recalltype.equals("2")) {
          if (sim_resourcetype.equals("all") || source_resourcetype.equals("all")
            || (sim_resourcetype.equals("video") && source_resourcetype.equals("video"))) {
            val rawTable = spark.sparkContext.textFile(path)
              .flatMap { line =>
                val info = line.split("\t")
                val sourceidtype = info(0) + "-video"
                val simidtype = info(1) + "-video"
                val simscore = info(2).toFloat
                val recalltype = broad_recalltype.value
                Set(
                  (sourceidtype, simidtype, simscore, recalltype),
                  (simidtype, sourceidtype, simscore, recalltype)
                )
              }.toDF("source", "sim", "simscore", "recalltype")

            mergeCFRawTableAB.append(rawTable)
            println("zhp:recalltype:" + rawTable.first)
          }
        } else {
          val rawTable = spark.sparkContext.textFile(path)
            .filter { line =>
              val info = line.split("\t")
              sim_resourcetype.equals("all") || source_resourcetype.equals("all") || (info(0).endsWith(source_resourcetype) || info(0).endsWith(sim_resourcetype) || info(1).endsWith(source_resourcetype) || info(1).endsWith(sim_resourcetype))
            }
            .flatMap { line =>
              val info = line.split("\t")
              val sourceidtype = info(0)
              val simidtype = info(1)
              val simscore = info(2).toFloat
              val recalltype = broad_recalltype.value
              Set(
                (sourceidtype, simidtype, simscore, recalltype),
                (simidtype, sourceidtype, simscore, recalltype)
              )
            }.toDF("source", "sim", "simscore", "recalltype")

          mergeCFRawTableAB.append(rawTable)
          println("zhp:recalltype:" + rawTable.first)
        }
      }
    }
    var mergedTable = mergeCFRawTableAB(0)
    println("zhp:mergeCFRawTableAB.length:" + mergeCFRawTableAB.length)
    println("zhp:rawTable:" + mergedTable.first)
    for (i <- 1 until mergeCFRawTableAB.length) {
      val rawTable = mergeCFRawTableAB(i)
      mergedTable = mergedTable.union(rawTable)
      println("zhp:rawTable:" + rawTable.first)
    }
    if (!source_resourcetype.equals("all")) {
      mergedTable = mergedTable.filter($"source".endsWith(source_resourcetype))
    }
    if (!sim_resourcetype.equals("all")) {
      mergedTable = mergedTable.filter($"sim".endsWith(sim_resourcetype))
    }
    if (sim_resourcetype.equals("video")) {
      mergedTable = mergedTable.filter(setContainsItem(videoPoolS)($"sim"))
    } else if (sim_resourcetype.equals("all")) {
      mergedTable = mergedTable.filter(
        $"sim".endsWith("-video") && setContainsItem(videoPoolS)($"sim")
          || $"sim".endsWith("-mv")
          || $"sim".endsWith("-concert")
          || $"sim".endsWith("-eventactivity")
      )
    }
    println("zhp:totalCount:" + mergedTable.count())

    val result = mergedTable
      //.filter($"source".endsWith(source_resourcetype) && $"sim".endsWith(sim_resourcetype))
      //.filter(setContainsItem(videoPoolS)($"sim"))
      .groupBy($"source", $"sim")
      .agg(
        max($"simscore").as("simscore"),
        collect_set($"recalltype").as("recalltypeS")
      )
      //.withColumn("recalltypes", setConcat("&")($"recalltypeS"))
      .select("source", "sim", "simscore", "recalltypeS")
      .rdd
      .groupBy(tup => tup.getString(0))
      .map{line =>
        val source = line._1
        val values = line._2
        val sims = ArrayBuffer[String]()
        values.toArray
          .sortWith(_.getAs[Float]("simscore") > _.getAs[Float]("simscore"))  //次级排序
          .sortWith(_.getAs[Seq[String]]("recalltypeS").size > _.getAs[Seq[String]]("recalltypeS").size)  // 优先排序
          .foreach { value =>
            val sim = value.getAs[String]("sim")
            val simscore = value.getAs[Float]("simscore").formatted("%.4f")
            val recalltypeS = value.getAs[Seq[String]]("recalltypeS")
            sims.append(sim + ":" + simscore + ":" + recalltypeS.mkString("&"))
          }
        if (sims.size > 0)
          source + "\t" + sims.mkString(",")
        else
          "null"
      }
      .filter(line => !line.equals("null"))

    result.saveAsTextFile(output)
  }

}
