package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object SimVideoMerge {

  // recalltype :
  // 0:mixedCF
  // 1:mixedMusicCF
  // 2:positiveCommentCF
  // 3:mixedMusicCF
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("mixedCF", true, "input directory, feed flow music pref mixed video pref")
    options.addOption("mixedNewCF", true, "input directory, feed flow music pref mixed video pref")
    options.addOption("mixedMusicCF", true, "input directory, music pref mixed video pref")
    options.addOption("positiveCommentCF", true, "positive comment user cf")
    options.addOption("resType", true, "resource type to filter")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val mixedCF = cmd.getOptionValue("mixedCF")
    val mixedNewCF = cmd.getOptionValue("mixedNewCF")
    val mixedMusicCF = cmd.getOptionValue("mixedMusicCF")
    val positiveCommentCF = cmd.getOptionValue("positiveCommentCF")
    val resType = cmd.getOptionValue("resType")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val mixedCF_rawTable = spark.sparkContext.textFile(mixedCF)
      .filter{line =>
        val info = line.split("\t")
        info(0).endsWith(resType) && info(1).endsWith(resType)
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
        info(0).endsWith(resType) && info(1).endsWith(resType)
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
        info(0).endsWith(resType) && info(1).endsWith(resType)
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

    var full_rawTable = mixedCF_rawTable
      .union(mixedNewCF_rawTable)
      .union(mixedMusicCF_rawTable)

    if (resType.equals("video")) {
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

    val result = full_rawTable
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
