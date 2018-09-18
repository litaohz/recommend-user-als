package com.netease.music.recommend.scala.feedflow.song

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by hzzhangjunfei1 on 2017/8/17.
  */
object GetTagFromSongTitle {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("Music_Song", true, "input directory")
    options.addOption("songRelativeVideoSpark", true, "input directory")
    options.addOption("promotionInfo", true, "input directory")
    options.addOption("mixedCF4Song", true, "input directory")
    options.addOption("mixedMusicCF4Song", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val Music_Song = cmd.getOptionValue("Music_Song")
    val songRelativeVideoSpark = cmd.getOptionValue("songRelativeVideoSpark")
    val promotionInfo = cmd.getOptionValue("promotionInfo")
    val mixedCF4Song = cmd.getOptionValue("mixedCF4Song")
    val mixedMusicCF4Song = cmd.getOptionValue("mixedMusicCF4Song")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val songS = mutable.HashSet[String]()
    spark.sparkContext.textFile(songRelativeVideoSpark)
      .map{line =>
        val info = line.split("\t")
        val songId = info(0)
        songId
      }
      .collect
      .foreach(songId => songS.add(songId))
    spark.sparkContext.textFile(promotionInfo)
      .map{line =>
        val info = line.split("\t")
        val songId = info(0)
        songId
      }
      .collect
      .foreach(songId => songS.add(songId))
    spark.sparkContext.textFile(mixedCF4Song)
      .map{line =>
        val info = line.split("\t|-")
        val songId = info(0)
        songId
      }
      .collect
      .foreach(songId => songS.add(songId))
    spark.sparkContext.textFile(mixedMusicCF4Song)
      .map{line =>
        val info = line.split("\t|-")
        val songId = info(0)
        songId
      }
      .collect
      .foreach(songId => songS.add(songId))
    //val songS_broadcast = spark.sparkContext.broadcast(songS)
    val usefulSongTable = spark.sparkContext.parallelize(songS.toSeq).toDF("songId")



    val finaltable = spark.read.json(Music_Song)
      .withColumn("songtags", getTagsFromNameAliaName($"name", $"AliaName"))
      .filter($"songtags"=!="null" && $"songtags".isNotNull)
      .select(
        $"id".as("songId"),
        $"songtags"
      )
      .join(usefulSongTable, Seq("songId"), "inner")

    finaltable.write.option("sep", "\t").csv(output)
  }

  def getTagsFromNameAliaName = udf((name: String, AliaName: String) => {

    val tagBuffer = ArrayBuffer[String]()
    if (name != null && !name.isEmpty) {
      val normedSongName = name.toLowerCase.replaceAll(" ", "")
      tagBuffer.append(normedSongName)
    }
    /*if (ArtistName != null && !ArtistName.isEmpty) {
      val normedArtistName = ArtistName.toLowerCase.replaceAll(" ", "")
      tagBuffer.append(normedArtistName)
    }*/
    if (AliaName != null && !AliaName.isEmpty) {
      val pattern = "(?<=《).*?(?=》)".r
      val normedSubNameTagS = pattern.findAllMatchIn(AliaName.toLowerCase.replaceAll(" ", ""))
      normedSubNameTagS.foreach { mat =>
        val tagMatched = mat.group(0)
        if (!tagBuffer.contains(tagMatched))
          tagBuffer.append(tagMatched)
      }
    }
    if (tagBuffer.size > 0)
      tagBuffer.mkString("_tab_").replaceAll("\\s", "")
    else
      "null"
  })


}
