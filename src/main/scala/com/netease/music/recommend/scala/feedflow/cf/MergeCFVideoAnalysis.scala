package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType}

import scala.collection.mutable.ArrayBuffer

// 分类mergeCF中某个recalltype单独召回的video
object MergeCFVideoAnalysis {

  // recalltype :
  // 1:merge rest
  // 2:some recalltype
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("mergeCF", true, "input directory")
    options.addOption("recalltype", true, "recalltype")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val mergeCF = cmd.getOptionValue("mergeCF")
    val recalltype = cmd.getOptionValue("recalltype")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val mergeCF_rawTable = spark.read.option("sep", "\t").csv(mergeCF).toDF("source", "sims")
      .withColumn("siminfo", explode(split($"sims", ",")))
      .withColumn("sim", splitAndGet(":", 0)($"siminfo"))
      .withColumn("simscore", splitAndGet(":", 1)($"siminfo").cast(FloatType))
      .withColumn("recalltypes", splitAndGet(":", 2)($"siminfo"))
      .withColumn("recalltype", ($"recalltypes"===recalltype).cast(IntegerType))  // 0: merge other；1：the recalltype
      .select("source", "sim", "simscore", "recalltype")

    val result = mergeCF_rawTable
      .withColumn("recalltypeSimscore", concat_ws(":", $"recalltype", $"simscore"))
      .groupBy($"source", $"sim")
      .agg(
        collect_set($"recalltypeSimscore").as("recalltypeSimscoreS")
      )
      .withColumn("recalltypeSimscores", setConcat("&")($"recalltypeSimscoreS"))
      .select("source", "sim", "recalltypeSimscores")
      .rdd
      .groupBy(tup => tup.getString(0))
      .map{line =>
        val source = line._1
        val values = line._2
        val recalltypeSimM = scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap[String, Float]]()
        values.foreach{row =>
          val sim = row.getAs[String]("sim")
          val recalltypeSimscores = row.getAs[String]("recalltypeSimscores")
          for (recalltypeSimscore <- recalltypeSimscores.split("&")) {
            val info = recalltypeSimscore.split(":")
            val recalltype = info(0)
            val simscore = info(1).toFloat
            val siminfos = recalltypeSimM.getOrElse(recalltype, scala.collection.mutable.HashMap[String, Float]())
            recalltypeSimM.put(recalltype, siminfos)
            siminfos.put(sim, simscore)
          }
        }
        var mergeOtherStr = "null"
        var theRecalltypeStr = "null"
        if (recalltypeSimM.contains("0")) {
          mergeOtherStr = recalltypeSimM("0").toArray.sortWith(_._2>_._2).map(tup => tup._1 + ":" + tup._2.formatted("%.4f")).mkString(",")
          }
        if (recalltypeSimM.contains("1")) {
          theRecalltypeStr = recalltypeSimM("1").toArray.sortWith(_._2>_._2).map(tup => tup._1 + ":" + tup._2.formatted("%.4f")).mkString(",")
        }

        source + "\t" + mergeOtherStr + "\t" + theRecalltypeStr
      }

    result.saveAsTextFile(output)
  }

}
