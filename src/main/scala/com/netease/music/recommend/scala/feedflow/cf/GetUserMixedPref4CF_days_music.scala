package com.netease.music.recommend.scala.feedflow.cf

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.getExistsPathUnderDirectory
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.netease.music.recommend.scala.feedflow.splitAndGet
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object GetUserMixedPref4CF_days_music {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("mixedOriginalPref", true, "input directory")
    options.addOption("songProfile", true, "input directory")
    options.addOption("artistProfile", true, "input directory")
    options.addOption("videoActiveUser", true, "input directory")

    options.addOption("output", true, "output directory")
    options.addOption("outputIndex", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val mixedOriginalPref = cmd.getOptionValue("mixedOriginalPref")
    val songProfile = cmd.getOptionValue("songProfile")
    val artistProfile = cmd.getOptionValue("artistProfile")
    val videoActiveUser = cmd.getOptionValue("videoActiveUser")

    val output = cmd.getOptionValue("output")
    val outputIndex = cmd.getOptionValue("outputIndex")

    import spark.implicits._
    val songProfileTable = spark.sparkContext.textFile(songProfile)
      .repartition(1000)
      .filter{line =>
        val info = line.split("\t")
        !info(2).equals("null") || !info(3).equals("null") || !info(4).equals("null") || !info(5).equals("null")
      }
      .flatMap{line =>
        // "userId", "activeCnt", "songProfile", "subSong", "searchSongProfile", "localSong", "userNocopyrightFCSong", "prefSongFromVideo"
        val info = line.split("\t")
        var songRecalledM = Map[String, Set[String]]()
        val userId = info(0)
        // profSong
        if (!info(2).equals("null")) {      // 全选
          val profSongs = info(2).split(",")
          profSongs.foreach{item =>
            val info = item.split(":")
            val songId = info(0)
            val pref = info(1).toFloat
            if (pref > 0.03) {
              var recallS = songRecalledM.getOrElse(songId, Set[String]())
              recallS += "profSong"
              songRecalledM += (songId -> recallS)
            }
          }
        }
        // searchSong
        if (!info(4).equals("null")) {      // 全选
          val searchSongs = info(4).split(",")
          val searchSongS =  searchSongs.map(item => item.split(":")(0)).toSet
          searchSongS.foreach{songId =>
            var recallS = songRecalledM.getOrElse(songId, Set[String]())
            recallS += "searchSong"
            songRecalledM += (songId -> recallS)
          }
        }
        // subSong + localSong
        if (!info(3).equals("null") && !info(5).equals("null")) {
          val subSongS = info(3).split(",").toSet
          val localSongS = info(5).split(",").toSet
          val subLSongS = ArrayBuffer[String]()
          val subL_thred = 20
          localSongS.foreach{songId =>
            if (subSongS.contains(songId))
              subLSongS.append(songId)
            if (subLSongS.size>subL_thred) {
              val pos_delete = Random.nextInt(subL_thred)
              subLSongS.remove(pos_delete)
            }
          }
          subLSongS.foreach { songId =>
            var recallS = songRecalledM.getOrElse(songId, Set[String]())
            recallS += "subLSong"
            songRecalledM += (songId -> recallS)
          }
        }
        songRecalledM
          .map(tup => tup._1 + "-" + tup._2.mkString("&"))
          .map(item => (userId, item))
      }.toDF("userId", "itemIdWithInfo")
      .withColumn("itemId", concat_ws("-", splitAndGet("-", 0)($"itemIdWithInfo"), lit("song")))
      .withColumn("pref", lit("5"))
    val artistProfileTable = spark.sparkContext.textFile(artistProfile)
      .repartition(1000)
      .filter{line =>
        val info = line.split("\t")
        !info(2).equals("null") || !info(3).equals("null")
      }
      .flatMap{line =>
        // "userId", "activeCnt", "subArt", "recArt", "subSimArt", "artistPrefFromVideo"
        val info = line.split("\t")
        var artsRecalledM = Map[String, Set[String]]()
        val userId = info(0)
        // subArt
        if (!info(2).equals("null")) {
          val subArts = info(2).split(",")
          var subArtS = Set[String]()
          if (subArts.length <= 20) {
            subArtS = subArts.map(item => item.split(":")(0)).toSet
          } else {
            subArtS = subArts.map(item => {
              val info = item.split(":")
              (info(0), info(1).toLong)
            }).sortWith(_._2 > _._2).slice(0, 20).map(tup => tup._1).toSet
          }
          subArtS.foreach{artId =>
            var recallS = artsRecalledM.getOrElse(artId, Set[String]())
            recallS += "subArt"
            artsRecalledM += (artId -> recallS)
          }
        }
        // recArt
        if (!info(3).equals("null")) {
          val recArts = info(3).split(",")
          var recArtS = Set[String]()
          if (recArts.length <= 20) {
            recArtS = recArts.map(item => item.split(":")(0)).toSet
          } else {
            recArtS = recArts.map(item => {
              val info = item.split(":")
              (info(0), info(1).toFloat)
            }).sortWith(_._2 > _._2).slice(0, 20).map(tup => tup._1).toSet
          }
          recArtS.foreach{artId =>
            var recallS = artsRecalledM.getOrElse(artId, Set[String]())
            recallS += "recArt"
            artsRecalledM += (artId -> recallS)
          }
        }
        artsRecalledM
          .map(tup => tup._1 + "-" + tup._2.mkString("&"))
          .map(item => (userId, item))
      }.toDF("userId", "itemIdWithInfo")
      .withColumn("itemId", concat_ws("-", splitAndGet("-", 0)($"itemIdWithInfo"), lit("artist")))
      .withColumn("pref", lit("5"))
    val videoFromVideoTable = spark.read.option("sep", "\t")
      .csv(mixedOriginalPref)
      .toDF("userId", "itemId", "pref")
      .filter($"itemId".endsWith("-video"))      // 过滤mv
      .withColumn("itemIdWithInfo", lit("0"))
      .select("userId", "itemIdWithInfo", "itemId", "pref")

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val timeInMillis = System.currentTimeMillis
    val millisInDay = 24 * 3600 * 1000l
    val qualifiedLogDateSeq = for (i <- (1 to 14)) yield {
      sdf.format(new Date(timeInMillis - i * millisInDay))
    }
    val existsVideoActiveUserInputPaths = getExistsPathUnderDirectory(videoActiveUser, "page", fs)
      .filter{path =>
        var remain = false
        qualifiedLogDateSeq.foreach(qualifiedLogDate => if (path.contains(qualifiedLogDate)) remain = true)
        remain
      }.mkString(",")
    val videoActiveUserTable = spark.sparkContext.textFile(existsVideoActiveUserInputPaths)
      .repartition(288)
      .map{line =>
        // action  \t  type  \t  os  \t  userId  \t  logTime  \t
        // target  \t  offset
        val info = line.split("\t")
        var userId = info(3).toLong
        if (userId < 0)
          userId *= -1
        userId.toString
      }.toDF("userId")
      .groupBy("userId")
      .agg(count("userId").as("cnt"))
      .select($"userId", lit("0").as("itemIdWithInfo"), lit("0").as("itemId"), lit("0").as("pref"), lit(1).as("isVideoActiveUser"))

    val window = Window.partitionBy("userId").orderBy("itemId")
    val finalDataset = videoFromVideoTable
      .union(songProfileTable)
      .union(artistProfileTable)
      .withColumn("isVideoActiveUser", lit(0))
      .union(videoActiveUserTable)
      .withColumn("active", sum($"isVideoActiveUser").over(window))
      .filter($"active">0 && $"pref"=!="0")
      .select("userId", "itemId", "itemIdWithInfo","pref")
      .cache

    finalDataset
      .repartition(64)
      .write.option("sep", "\t").csv(output + "/originalId")

    val windowSpec = Window.partitionBy("groupColumn").orderBy("groupColumn")
    val index = finalDataset
      .groupBy("itemId")
      .agg(count("itemId").as("cnt"))
      .withColumn("groupColumn", lit(1))
      .withColumn("mixedId", row_number() over(windowSpec))
      .select("itemId", "mixedId")
      .cache
    index.write.option("sep", "\t").csv(outputIndex)

    finalDataset
      .join(index, Seq("itemId"), "left_outer")
      .select("userId", "mixedId", "pref")
      .repartition(32)
      .write.option("sep", "\t").csv(output + "/mixedId")
  }



}
