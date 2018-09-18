package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object SimVideoCMP {

  // recalltype :
  // 0:mixedMusicCF
  // 1:mixedMusicCF2
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("CF1", true, "input directory")
    options.addOption("CF2", true, "input directory")
    options.addOption("source_resourcetype", true, "resource type to filter")
    options.addOption("sim_resourcetype", true, "resource type to filter")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val CF1 = cmd.getOptionValue("CF1")
    val CF2 = cmd.getOptionValue("CF2")
    val source_resourcetype = cmd.getOptionValue("source_resourcetype")
    val sim_resourcetype = cmd.getOptionValue("sim_resourcetype")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val CF_rawTable = spark.sparkContext.textFile(CF1)
      .filter{line =>
        val info = line.split("\t")
        info(0).endsWith(source_resourcetype) || info(0).endsWith(sim_resourcetype) || info(1).endsWith(source_resourcetype) || info(1).endsWith(sim_resourcetype)
        //info(0).endsWith(resType) && info(1).endsWith(resType)
      }
      .flatMap { line =>
        val info = line.split("\t")
        val sourceidtype = info(0)
        val simidtype = info(1)
        val simscore = info(2).toFloat
        val recalltype = "1"
        Set((sourceidtype, simidtype, simscore, recalltype), (simidtype, sourceidtype, simscore, recalltype))
      }.toDF("source", "sim", "simscore", "recalltype")
    val CF2_rawTable = spark.sparkContext.textFile(CF2)
      .filter{line =>
        val info = line.split("\t")
        info(0).endsWith(source_resourcetype) || info(0).endsWith(sim_resourcetype) || info(1).endsWith(source_resourcetype) || info(1).endsWith(sim_resourcetype)
        //info(0).endsWith(resType) && info(1).endsWith(resType)
      }
      .flatMap { line =>
        val info = line.split("\t")
        val sourceidtype = info(0)
        val simidtype = info(1)
        val simscore = info(2).toFloat
        val recalltype = "2"
        Set((sourceidtype, simidtype, simscore, recalltype), (simidtype, sourceidtype, simscore, recalltype))
      }.toDF("source", "sim", "simscore", "recalltype")

    val full_rawTable = CF_rawTable
      .union(CF2_rawTable)

    val result = full_rawTable
      .filter($"source".endsWith(source_resourcetype) && $"sim".endsWith(sim_resourcetype))
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
        var recalltype1Str = "null"
        var recalltype2Str = "null"
        var commonSimsStr = "null"
        if (recalltypeSimM.contains("1") && recalltypeSimM.contains("2")) {
          val simInfoM = scala.collection.mutable.HashMap[String, (Int, Float)]()
          val sims = ArrayBuffer[(String, Float)]()
          recalltypeSimM("1").foreach(entry => sims.append(entry))
          recalltypeSimM("2").foreach(entry => sims.append(entry))
          sims.foreach{tup =>
            val sim = tup._1
            var simscore = tup._2
            val info = simInfoM.getOrElse(sim, (0, 0.0f))
            if (info._2 > simscore)
              simscore = info._2
            simInfoM.put(sim, (info._1 + 1, simscore))
          }
          val commonSimsStr_tmp = simInfoM.filter(tup => tup._2._1 >= 2)
            .toArray.sortWith(_._2._2>_._2._2)
            .map{tup =>
              val sim = tup._1
              recalltypeSimM("1").remove(sim)
              recalltypeSimM("2").remove(sim)
              sim + ":" + tup._2._2.formatted("%.4f")
            }
            .mkString(",")
          if (commonSimsStr_tmp.length > 1)
            commonSimsStr = commonSimsStr_tmp
        }
        if (recalltypeSimM.contains("1")) {
          recalltype1Str = recalltypeSimM("1").toArray.sortWith(_._2>_._2).map(tup => tup._1 + ":" + tup._2.formatted("%.4f")).mkString(",")
        }
        if (recalltypeSimM.contains("2")) {
          recalltype2Str = recalltypeSimM("2").toArray.sortWith(_._2>_._2).map(tup => tup._1 + ":" + tup._2.formatted("%.4f")).mkString(",")
        }

        source + "\t" + recalltype1Str + "\t" + commonSimsStr + "\t" + recalltype2Str
      }
      //.filter(line => !line.equals("null"))

    result.saveAsTextFile(output)
  }

}
