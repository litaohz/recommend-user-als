package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object MixedIdConverterNew {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("inputPairs", true, "input directory")
    options.addOption("index", true, "input directory")

    options.addOption("output", true, "output directory")
    options.addOption("outputMixed4Artist", true, "output directory")
    options.addOption("outputMixed4Song", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val inputPairs = cmd.getOptionValue("inputPairs")
    val index = cmd.getOptionValue("index")

    val output = cmd.getOptionValue("output")
    val outputMixed4Artist = cmd.getOptionValue("outputMixed4Artist")
    val outputMixed4Song = cmd.getOptionValue("outputMixed4Song")

    import spark.implicits._
    // key  : id
    // value: mixedId
    val indexM = spark.sparkContext.textFile(index)
      .map{line =>
        val info = line.split("\t")
        (info(1), info(0))
      }
      .collect
      .toMap[String, String]

    val broatcast_index = spark.sparkContext.broadcast[Map[String, String]](indexM)


    val finalTable = spark.read.option("sep", "\t")
      .csv(inputPairs)
      .toDF("id0", "id1", "simScore")
      .withColumn("mixedId0", id2MixedId(broatcast_index)($"id0"))
      .withColumn("mixedId1", id2MixedId(broatcast_index)($"id1"))
      .select("mixedId0", "mixedId1", "simScore")
      .cache


    finalTable
      .write.option("sep", "\t").csv(output)

    val mixedCF4ArtistTable = finalTable
      .filter($"mixedId0".endsWith("-artist"))
      .groupBy($"mixedId0")
      .agg(
        collect_set(concat_ws(":", $"mixedId1", $"simScore")).as("relativeMixedIds")
      )
      .rdd
      .map(row => splitMixedIds(row.getString(0), row.getSeq[String](1)))
      .cache
    val artistVideoM = mixedCF4ArtistTable
      .map{tup => (tup._1, tup._2)}
      .filter(tup => !tup._2.equals("null"))
      .collect
      .toMap[String, String]
    val bc_artistVideoM = spark.sparkContext.broadcast(artistVideoM)

    val mixedCF4SongTable = finalTable
      .filter($"mixedId0".endsWith("-song"))
      .groupBy($"mixedId0")
      .agg(
        collect_set(concat_ws(":", $"mixedId1", $"simScore")).as("relativeMixedIds")
      )
      .rdd
      .map(row => splitMixedIds(row.getString(0), row.getSeq[String](1)))
      .cache

    val songVideoM = mixedCF4SongTable.map{tup => (tup._1, tup._2)}
      .filter(tup => !tup._2.equals("null"))
      .collect
      .toMap[String, String]
    val bc_songVideoM = spark.sparkContext.broadcast(songVideoM)


    val numSimArt_thred = 5
    val numVideosFromSimArt_thred = 10

    mixedCF4ArtistTable
      .map {tup =>
        val cfRelativeVideosS = tup._2.split(",").map(item => item.split("-")(0)).toSet
        /*var simArtistsRelativeVideosM = Map[String, Int]()
        var simArtS = Set[String]()
        // 遍历相似artist
        if (!tup._3.equals("null")) {
          tup._3.split(",").foreach { line =>
            val artist = line.split(":")(0)
//            val simScore4Artist = line.split(":")(1).toFloat
            if (simArtS.size < numSimArt_thred) { // 最多取numSimArt_thred个最相似的artist
              val videos = bc_artistVideoM.value.getOrElse(artist, "null")
              if (!videos.equals("null")) {
                // 遍历相关video
                var videoS = Set[String]()
                for (videoInfo <- videos.split(",")) {
                  val video = videoInfo.split("-")(0)
//                  val videoSimScore = videoInfo.split(":")(1).toFloat
                  if (!cfRelativeVideosS.contains(video) && videoS.size < numVideosFromSimArt_thred) { // 要求videoSimScore>0.05
                    val cnt = simArtistsRelativeVideosM.getOrElse(video, 0) + 1
                    simArtistsRelativeVideosM += (video -> cnt)
                    videoS += video
                    simArtS += artist
                  }
                }
              }
            }
          }
        }
        var simArtVides = simArtistsRelativeVideosM.toList
          .filter(_._2 > 1)
          .sortWith(_._2>_._2)
          .map(tup => tup._1 + ":" + tup._2)
          .mkString(",")
        if (simArtVides.isEmpty) simArtVides = "null"

        var simSongsRelativeVideosM = Map[String, Int]()
        // 遍历相似song
        if (!tup._4.equals("null")) {
          tup._4.split(",").foreach { line =>
            val song = line.split(":")(0)
            val videos = bc_songVideoM.value.getOrElse(song, "null")
            if (!videos.equals("null")) {
              // 遍历相关video
              for (videoInfo <- videos.split(",")) {
                val video = videoInfo.split("-")(0)
                if (!cfRelativeVideosS.contains(video)) {
                  val cnt = simSongsRelativeVideosM.getOrElse(video, 0) + 1
                  simSongsRelativeVideosM += (video -> cnt)
                }
              }
            }
          }
        }
        var simSongVides = simSongsRelativeVideosM.toList
          .filter(_._2 > 1)
          .sortWith(_._2>_._2)
          .map(tup => tup._1 + ":" + tup._2)
          .mkString(",")
        if (simSongVides.isEmpty) simSongVides = "null"*/

        val cfRelativeVideos = tup._2.split(",").map(item => item.replace("-video", "")).mkString(",")
        tup._1 + "\t" + cfRelativeVideos/* + "\t" + tup._3 + "\t" + tup._4 + "\t" + simArtVides + "\t" + simSongVides*/
      }
      .filter(line => !line.split("\t")(1).equals("null"))
      .saveAsTextFile(outputMixed4Artist)

    mixedCF4SongTable
      .map {tup =>
        val cfRelativeVideosS = tup._2.split(",").map(item => item.split("-")(0)).toSet
        /*var simArtistsRelativeVideosM = Map[String, Int]()
        var simArtS = Set[String]()
        // 遍历相似artist
        if (!tup._3.equals("null")) {
          tup._3.split(",").foreach { line =>
            val artist = line.split(":")(0)
//            val simScore4Artist = line.split(":")(1).toFloat
            if (simArtS.size < numSimArt_thred) { // 最多取numSimArt_thred个最相似的artist
              val videos = bc_artistVideoM.value.getOrElse(artist, "null")
              if (!videos.equals("null")) {
                // 遍历相关video
                var videoS = Set[String]()
                for (videoInfo <- videos.split(",")) {
                  val video = videoInfo.split("-")(0)
//                  val videoSimScore = videoInfo.split(":")(1).toFloat
                  if (!cfRelativeVideosS.contains(video) && videoS.size < numVideosFromSimArt_thred) { // 要求videoSimScore>0.05
                    val cnt = simArtistsRelativeVideosM.getOrElse(video, 0) + 1
                    simArtistsRelativeVideosM += (video -> cnt)
                    videoS += video
                    simArtS += artist
                  }
                }
              }
            }
          }
        }
        var simArtVides = simArtistsRelativeVideosM.toList
          .filter(_._2 > 1)
          .sortWith(_._2>_._2)
          .map(tup => tup._1 + ":" + tup._2)
          .mkString(",")
        if (simArtVides.isEmpty) simArtVides = "null"

        var simSongsRelativeVideosM = Map[String, Int]()
        // 遍历相似song
        if (!tup._4.equals("null")) {
          tup._4.split(",").foreach { line =>
            val song = line.split(":")(0)
            val videos = bc_songVideoM.value.getOrElse(song, "null")
            if (!videos.equals("null")) {
              // 遍历相关video
              for (videoInfo <- videos.split(",")) {
                val video = videoInfo.split("-")(0)
                if (!cfRelativeVideosS.contains(video)) {
                  val cnt = simSongsRelativeVideosM.getOrElse(video, 0) + 1
                  simSongsRelativeVideosM += (video -> cnt)
                }
              }
            }
          }
        }
        var simSongVides = simSongsRelativeVideosM.toList
          .filter(_._2 > 1)
          .sortWith(_._2>_._2)
          .map(tup => tup._1 + ":" + tup._2)
          .mkString(",")
        if (simSongVides.isEmpty) simSongVides = "null"*/

        val cfRelativeVideos = tup._2.split(",").map(item => item.replace("-video", "")).mkString(",")
        tup._1 + "\t" + cfRelativeVideos/* + "\t" + tup._3 + "\t" + tup._4 + "\t" + simArtVides + "\t" + simSongVides*/
      }
      .filter(line => !line.split("\t")(1).equals("null"))
      .saveAsTextFile(outputMixed4Song)
  }

  def splitMixedIds(sourceId:String, relativeIds:Iterable[String]):(String, String, String, String)= {

    val videoS = scala.collection.mutable.Set[(String, Float)]()
    val artistS = scala.collection.mutable.Set[(String, Float)]()
    val songS = scala.collection.mutable.Set[(String, Float)]()
    relativeIds.foreach{str =>
      val info = str.split(":")
      val tup = (info(0), info(1).toFloat)
      if (tup._1.endsWith("-video"))
        videoS += tup
      else if (tup._1.endsWith("-artist"))
        artistS += tup
      else if (tup._1.endsWith("-song"))
        songS += tup
    }
    val videoStr = if(videoS.size>0) videoS.toList.sortWith(_._2>_._2).map(tup => tup._1 + ":" + tup._2.formatted("%.4f")).mkString(",") else "null"
    val artistStr = if(artistS.size>0) artistS.toList.sortWith(_._2>_._2).map(tup => tup._1 + ":" + tup._2.formatted("%.4f")).mkString(",") else "null"
    val songStr = if(songS.size>0) songS.toList.sortWith(_._2>_._2).map(tup => tup._1 + ":" + tup._2.formatted("%.4f")).mkString(",") else "null"
    (sourceId, videoStr, artistStr, songStr)
  }

  def id2MixedId(bc_M:Broadcast[Map[String, String]]) = udf((id:String) => {
    bc_M.value(id)
  })
}
