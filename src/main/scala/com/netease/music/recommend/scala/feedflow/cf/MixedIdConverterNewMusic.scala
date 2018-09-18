package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object MixedIdConverterNewMusic {

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
      }.toDF("id", "mixedId")
    /*.collect
    .toMap[String, String]

  val broatcast_index = spark.sparkContext.broadcast[Map[String, String]](indexM)*/


    val finalTable = spark.read.option("sep", "\t")
      .csv(inputPairs)
      .repartition(88)
      .toDF("id0", "id1", "simScore")
      .join(indexM, $"id0"===$"id")
      .withColumnRenamed("mixedId", "mixedId0")
      .drop("id")
      .join(indexM, $"id1"===$"id")
      .withColumnRenamed("mixedId", "mixedId1")
      /*.withColumn("mixedId0", id2MixedId(broatcast_index)($"id0"))
      .withColumn("mixedId1", id2MixedId(broatcast_index)($"id1"))*/
      .select("mixedId0", "mixedId1", "simScore")
    //      .cache


    finalTable
      .write.option("sep", "\t").csv(output)

    val reverseTable = finalTable
      .select($"mixedId1".as("tmpId0"), $"mixedId0".as("tmpId1"), $"simScore")
      .withColumnRenamed("tmpId0", "mixedId0")
      .withColumnRenamed("tmpId1", "mixedId1")
    val fullDataset_stage1 = finalTable
      .union(reverseTable)
    val tooHotItemSet = fullDataset_stage1
//      .filter($"mixedId0".endsWith("-video"))
      .groupBy($"mixedId0")
      .agg(count($"mixedId0").as("cnt"))
      .filter($"cnt" > 200)
      .select($"mixedId0")
      .rdd.map(line => line.getString(0))
      .collect()
      .toSet
    val bc_tooHotItemSet = spark.sparkContext.broadcast(tooHotItemSet)
    val fullDataset_stage2 = fullDataset_stage1
      .withColumn("isTooHot", isTooHot(bc_tooHotItemSet)($"mixedId0"))

   /* // 非过热video的相关过滤
    val notHotInfo = fullDataset_stage2
      .filter(!$"isTooHot")
      .groupBy("mixedId0")
      .agg(
        sum(($"mixedId1".endsWith("-artist")).cast(IntegerType)).as("relativeArtists_withinHotPart")
        ,sum(($"mixedId1".endsWith("-song")).cast(IntegerType)).as("relativeSongs_withinHotPart")
      )
      .cache
    val notHotArtOrSongIdFilterS = notHotInfo
      .filter($"mixedId0".endsWith("-video"))
      .filter($"relativeArtists_withinHotPart">=7 || $"relativeSongs_withinHotPart">=7)
      .map(line => line.getString(0))
      .collect()
      .toSet[String]
    val bc_notHotArtOrSongIdFilterS = spark.sparkContext.broadcast(notHotArtOrSongIdFilterS)*/

    // 热门video的相关过滤
    val window = Window.partitionBy($"mixedId0").orderBy($"simScore")
    val hotInfo = fullDataset_stage2
      .filter($"isTooHot").drop("isTooHot")
      .withColumn("similar_rank", row_number().over(window))
      .filter($"similar_rank"<=38)
      .groupBy("mixedId0")
      .agg(
        collect_set($"mixedId1").as("filteredIds")
        ,sum(($"mixedId1".endsWith("-artist")).cast(IntegerType)).as("relativeArtists_withinHotPart")
//        ,sum(($"mixedId1".endsWith("-song")).cast(IntegerType)).as("relativeSongs_withinHotPart")
      )
      .cache
    val hotPartFilterM = hotInfo
      .map(line => (line.getString(0), line.getSeq[String](1)))
      .collect()
      .toMap[String, Seq[String]]
    val hotArtOrSongIdFilterS = hotInfo
      .filter($"mixedId0".endsWith("-video"))
      .filter($"relativeArtists_withinHotPart">=9)
      .map(line => line.getString(0))
      .collect()
      .toSet[String]
    val bc_hotPartFilterM = spark.sparkContext.broadcast(hotPartFilterM)
    val bc_hotArtOrSongIdFilterS = spark.sparkContext.broadcast(hotArtOrSongIdFilterS)
    val fullDataset = fullDataset_stage2
      // 过滤热门videos的最不相似item
      .withColumn("isFilter", filterByHotRule(bc_hotPartFilterM)($"mixedId0", $"mixedId1"))
      // 过滤关联过多artist/songd的热门/非热门videos
      .withColumn("isFilterByHotArtSongRule", filterByArtSongRule(bc_hotArtOrSongIdFilterS)($"mixedId1"))
      .filter(!$"isFilter" && !$"isFilterByHotArtSongRule")
      .select("mixedId0", "mixedId1", "simScore")

    val mixedCF4ArtistTable = fullDataset
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

    val mixedCF4SongTable = fullDataset
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
        var simArtistsRelativeVideosM = Map[String, Int]()
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
        if (simSongVides.isEmpty) simSongVides = "null"

        val cfRelativeVideos = tup._2.split(",").map(item => item.replace("-video", "")).mkString(",")
        tup._1 + "\t" + cfRelativeVideos + "\t" + tup._3 + "\t" + tup._4 + "\t" + simArtVides + "\t" + simSongVides
      }
      .saveAsTextFile(outputMixed4Artist)

    mixedCF4SongTable
      .map {tup =>
        val cfRelativeVideosS = tup._2.split(",").map(item => item.split("-")(0)).toSet
        var simArtistsRelativeVideosM = Map[String, Int]()
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
        if (simSongVides.isEmpty) simSongVides = "null"

        val cfRelativeVideos = tup._2.split(",").map(item => item.replace("-video", "")).mkString(",")
        tup._1 + "\t" + cfRelativeVideos + "\t" + tup._3 + "\t" + tup._4 + "\t" + simArtVides + "\t" + simSongVides
      }
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
