package com.netease.music.recommend.scala.feedflow.song

import com.netease.music.recommend.scala.feedflow.splitAndGet
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

import scala.collection.mutable


/**
  * Created by hzzhangjunfei1 on 2018/4/4.
  */
object GetUserKwdSongs {

  val usefulTagtypeS = Set[String]("T4", "T5")

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("songTitletag", true, "input directory")
    options.addOption("userKwdPref", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val songTitletag = cmd.getOptionValue("songTitletag")
    val userKwdPref = cmd.getOptionValue("userKwdPref")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val userKwdPrefTable = spark.read.option("sep", "\t")
      .csv(userKwdPref).toDF("userId", "tagType", "prefs")
      .filter($"userId"=!="0")
      .filter($"tagType".isin(usefulTagtypeS.toSeq : _*))
      .withColumn("kwdPref", explode(splitByDelim(",", 50)($"prefs")))
      .withColumn("kwd", splitAndGet(":", 0)($"kwdPref"))
      .withColumn("pref", splitAndGet(":", 1)($"kwdPref"))
      .select("kwd", "userId", "pref")
      .filter($"kwd".isNotNull)
      .groupBy($"kwd", $"userId")
      .agg(sum($"pref".cast(FloatType)).as("prefsum"))
      .groupBy($"kwd")
      .agg(
        collect_set(concat_ws(":", $"userId", $"prefsum")).as("uidPrefS")
      )
      .repartition(2000)

    val songTitletagTable = spark.read.option("sep", "\t")
      .csv(songTitletag).toDF("songId", "songtags")
      .filter($"songtags".isNotNull)
      .withColumn("kwd", explode(splitByDelim("_tab_")($"songtags")))
      .select("kwd", "songId")
      .filter($"kwd".isNotNull)
      .groupBy($"kwd")
      .agg(collect_set($"songId").as("songIdS"))
      .repartition(500)

    //val window = Window.partitionBy($"userId").orderBy($"pref".desc)
    val finaltable = songTitletagTable
      .join(userKwdPrefTable, Seq("kwd"), "inner")
      .rdd
      .flatMap {line =>
        val uidPrefS = line.getAs[mutable.WrappedArray[String]]("uidPrefS")
        val songIdS = line.getAs[mutable.WrappedArray[String]]("songIdS")
        val kwd = line.getAs[String]("kwd").replaceAll("\\s|:|,", "")
        //val songId = line.getAs[String]("songId")
        for (uidPref <- uidPrefS ; songId <- songIdS) yield {
          val uidInfo = uidPref.split(":")
          val userId = uidInfo(0)
          val pref = uidInfo(1)
          (userId, (songId + ":" + kwd + ":" + pref, pref.toFloat))
        }
//        (userId, (songId + ":" + kwd + ":" + pref, pref.toFloat))
      }
      .groupByKey()
      .map { line =>
        val userId = line._1
        val prefSongKwdPrefs = line._2.toArray.sortWith(_._2 > _._2).map(tup => tup._1).mkString(",")
        userId + "\t" + prefSongKwdPrefs
      }

    finaltable.saveAsTextFile(output)
  }
}
