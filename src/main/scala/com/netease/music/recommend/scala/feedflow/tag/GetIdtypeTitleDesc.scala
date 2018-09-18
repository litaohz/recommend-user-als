package com.netease.music.recommend.scala.feedflow.tag

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object GetIdtypeTitleDesc {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("videoMeta", true, "input directory")
    options.addOption("mvMeta", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoMeta = cmd.getOptionValue("videoMeta")
    val mvMeta = cmd.getOptionValue("mvMeta")
    val output = cmd.getOptionValue("output")

    import spark.implicits._
    val videoMetaRdd = spark.read.json(videoMeta)
      .filter(('title.isNotNull && 'title=!="") || ('description.isNotNull && 'description=!="" && 'description=!="null"))
      .select(
        concat_ws("-", 'videoId, lit("video")).as("idtype"),
        'title,
        'description
      ).rdd
      .map{row =>
        val idtype = row.getAs[String]("idtype")
        val desc_ab = ArrayBuffer[String]()

        val title = row.getAs[String]("title")
        if (title != null && !title.isEmpty)
          desc_ab.append(title)

        val description = row.getAs[String]("description")
        if (description != null && !description.isEmpty)
          desc_ab.append(description)

        (idtype, title, desc_ab.mkString("。").replaceAll("\t", " "))
      }

    val mvMetaRdd = spark.read.parquet(mvMeta)
      .select('id, 'name, 'Artists, 'MVDesc, 'Subtitle)
      .rdd
      .map{row =>
        val id = row.getAs[Long]("id")
        val idtype = id + "-mv"
        val desc_ab = ArrayBuffer[String]()

        val name = row.getAs[String]("name")
        if (name != null && !name.isEmpty)
          desc_ab.append(name)

        val MVDesc = row.getAs[String]("MVDesc")
        if (MVDesc != null && !MVDesc.isEmpty)
          desc_ab.append(MVDesc)

        val Subtitle = row.getAs[String]("Subtitle")
        if (Subtitle != null && !Subtitle.isEmpty)
          desc_ab.append(Subtitle)

        var des_ret = desc_ab.mkString("。").replaceAll("\t", " ")

        val Artists = row.getAs[String]("Artists")
        if (Artists != null && !Artists.isEmpty)
          Artists.split("\t").foreach{aidname =>
            if (!uselessAidnameS.contains(aidname))
              des_ret += ("\t" + aidname)
          }

        (idtype, name, des_ret)
      }

    val finalRdd = videoMetaRdd.union(mvMetaRdd).filter(tup => (tup._1!=null && !tup._1.isEmpty)).cache

    finalRdd.filter(tup => (tup._2!=null && !tup._2.isEmpty)).map(tup => tup._1 + "\t" + tup._2).saveAsTextFile(output + "/title")
    finalRdd.filter(tup => (tup._3!=null && !tup._3.isEmpty)).map(tup => tup._1 + "\t" + tup._3).saveAsTextFile(output + "/desc")
  }

}
