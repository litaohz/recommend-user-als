package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object SimCFMerge {

  // recalltype :
  // 0:mixedCF
  // 1:mixedMusicCF
  // 2:positiveCommentCF
  // 3:mixedNewCF
  // 4:allprefCF
  // 5:alldisCF
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("mixedCF", true, "input directory, feed flow music pref mixed video pref")
    options.addOption("mixedNewCF", true, "input directory, feed flow music pref mixed video pref")
    //options.addOption("mixedAttchSimCF", true, "input directory, feed flow music pref mixed video pref, attach sim videos")
    options.addOption("mixedMusicCF", true, "input directory, music pref mixed video pref")
    options.addOption("positiveCommentCF", true, "positive comment user cf")
    options.addOption("allprefCF", true, "all resourcetype pref cf")
    options.addOption("alldisCF", true, "all resourcetype dislike cf")
    options.addOption("source_resourcetype", true, "source item resource type to filter")
    options.addOption("sim_resourcetype", true, "similar item resource type to filter")
    options.addOption("videoPool", true, "video pool input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val mixedCF = cmd.getOptionValue("mixedCF")
    val mixedNewCF = cmd.getOptionValue("mixedNewCF")
    //val mixedAttchSimCF = cmd.getOptionValue("mixedAttchSimCF")
    val mixedMusicCF = cmd.getOptionValue("mixedMusicCF")
    val positiveCommentCF = cmd.getOptionValue("positiveCommentCF")
    val allprefCF = cmd.getOptionValue("allprefCF")
    val alldisCF = cmd.getOptionValue("alldisCF")
    val source_resourcetype = cmd.getOptionValue("source_resourcetype")
    val sim_resourcetype = cmd.getOptionValue("sim_resourcetype")
    val videoPool = cmd.getOptionValue("videoPool")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val mixedCF_rawTable = spark.sparkContext.textFile(mixedCF)
      .filter{line =>
        val info = line.split("\t")
        info(0).endsWith(source_resourcetype) || info(0).endsWith(sim_resourcetype) || info(1).endsWith(source_resourcetype) || info(1).endsWith(sim_resourcetype)
      }
      .flatMap { line =>
        val info = line.split("\t")
        val sourceidtype = info(0)
        val simidtype = info(1)
        val simscore = info(2).toFloat
        val recalltype = "0"
        Set(
          (sourceidtype, simidtype, simscore, recalltype),
          (simidtype, sourceidtype, simscore, recalltype)
        )
      }.toDF("source", "sim", "simscore", "recalltype")
    val mixedNewCF_rawTable = spark.sparkContext.textFile(mixedNewCF)
      .filter{line =>
        val info = line.split("\t")
        info(0).endsWith(source_resourcetype) || info(0).endsWith(sim_resourcetype) || info(1).endsWith(source_resourcetype) || info(1).endsWith(sim_resourcetype)
      }
      .flatMap { line =>
        val info = line.split("\t")
        val sourceidtype = info(0)
        val simidtype = info(1)
        val simscore = info(2).toFloat
        val recalltype = "3"
        Set(
          (sourceidtype, simidtype, simscore, recalltype),
          (simidtype, sourceidtype, simscore, recalltype)
        )
      }.toDF("source", "sim", "simscore", "recalltype")
    val mixedMusicCF_rawTable = spark.sparkContext.textFile(mixedMusicCF)
      .filter{line =>
        val info = line.split("\t")
        info(0).endsWith(source_resourcetype) || info(0).endsWith(sim_resourcetype) || info(1).endsWith(source_resourcetype) || info(1).endsWith(sim_resourcetype)
      }
      .flatMap { line =>
        val info = line.split("\t")
        val sourceidtype = info(0)
        val simidtype = info(1)
        val simscore = info(2).toFloat
        val recalltype = "1"
        Set(
          (sourceidtype, simidtype, simscore, recalltype),
          (simidtype, sourceidtype, simscore, recalltype)
        )
      }.toDF("source", "sim", "simscore", "recalltype")
    val allprefCF_rawTable = spark.sparkContext.textFile(allprefCF)
      .filter{line =>
        val info = line.split("\t")
        info(0).endsWith(source_resourcetype) || info(0).endsWith(sim_resourcetype) || info(1).endsWith(source_resourcetype) || info(1).endsWith(sim_resourcetype)
      }
      .flatMap { line =>
        val info = line.split("\t")
        val sourceidtype = info(0)
        val simidtype = info(1)
        val simscore = info(2).toFloat
        val recalltype = "4"
        Set(
          (sourceidtype, simidtype, simscore, recalltype),
          (simidtype, sourceidtype, simscore, recalltype)
        )
      }.toDF("source", "sim", "simscore", "recalltype")
    val alldisCF_rawTable = spark.sparkContext.textFile(alldisCF)
      .filter{line =>
        val info = line.split("\t")
        info(0).endsWith(source_resourcetype) || info(0).endsWith(sim_resourcetype) || info(1).endsWith(source_resourcetype) || info(1).endsWith(sim_resourcetype)
      }
      .flatMap { line =>
        val info = line.split("\t")
        val sourceidtype = info(0)
        val simidtype = info(1)
        val simscore = info(2).toFloat
        val recalltype = "5"
        Set(
          (sourceidtype, simidtype, simscore, recalltype),
          (simidtype, sourceidtype, simscore, recalltype)
        )
      }.toDF("source", "sim", "simscore", "recalltype")
    /*val mixedAttachSimCF_rawTable = spark.sparkContext.textFile(mixedAttchSimCF)
      .filter{line =>
        val info = line.split("\t")
        info(0).endsWith(source_resourcetype) || info(0).endsWith(sim_resourcetype) || info(1).endsWith(source_resourcetype) || info(1).endsWith(sim_resourcetype)
      }
      .flatMap { line =>
        val info = line.split("\t")
        val sourceidtype = info(0)
        val simidtype = info(1)
        val simscore = info(2).toFloat
        val recalltype = "6"
        Set(
          (sourceidtype, simidtype, simscore, recalltype),
          (simidtype, sourceidtype, simscore, recalltype)
        )
      }.toDF("source", "sim", "simscore", "recalltype")*/

    var full_rawTable = mixedCF_rawTable
      .union(mixedNewCF_rawTable)
      .union(mixedMusicCF_rawTable)
      .union(allprefCF_rawTable)
      .union(alldisCF_rawTable)
      //.union(mixedAttachSimCF_rawTable)

    if (sim_resourcetype.equals("video") && source_resourcetype.equals("video")) {
      val positiveCommentCF_rawTable = spark.sparkContext.textFile(positiveCommentCF)
        .flatMap { line =>
          val info = line.split("\t")
          val sourceidtype = info(0) + "-video"
          val simidtype = info(1) + "-video"
          val simscore = info(2).toFloat
          val recalltype = "2"
          Set(
            (sourceidtype, simidtype, simscore, recalltype),
            (simidtype, sourceidtype, simscore, recalltype)
          )
        }.toDF("source", "sim", "simscore", "recalltype")

      full_rawTable = full_rawTable
        .union(positiveCommentCF_rawTable)
    }

    val videoPoolS = spark.read.parquet(videoPool)
      .select(
        concat_ws("-", $"videoId", $"vType").as("idtype")
      )
      .rdd
      .map(line => line.getAs[String](0))
      .collect
      .toSet[String]
    //val broad_videoPoolS = spark.sparkContext.broadcast(videoPoolS)

    val result = full_rawTable
      .filter($"source".endsWith(source_resourcetype) && $"sim".endsWith(sim_resourcetype))
      .filter(setContainsItem(videoPoolS)($"sim"))
      .groupBy($"source", $"sim")
      .agg(
        max($"simscore").as("simscore"),
        collect_set($"recalltype").as("recalltypeS")
      )
      .withColumn("recalltypes", setConcat("&")($"recalltypeS"))
      .select("source", "sim", "simscore", "recalltypes")
      .rdd
      .groupBy(tup => tup.getString(0))
      .map{line =>
        val source = line._1
        val values = line._2
        val sims = ArrayBuffer[String]()
        values.toArray
          .sortWith(_.getAs[Float]("simscore") > _.getAs[Float]("simscore"))
          .foreach { value =>
            val sim = value.getAs[String]("sim")
            val simscore = value.getAs[Float]("simscore").formatted("%.4f")
            val recalltypes = value.getAs[String]("recalltypes")
            sims.append(sim + ":" + simscore + ":" + recalltypes)
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
