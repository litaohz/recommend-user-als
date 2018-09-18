package com.netease.music.recommend.scala.feedflow.song

import com.netease.music.recommend.scala.feedflow.cf.usefulCFRecalltypeS
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by hzzhangjunfei1 on 2018/07/06.
  */

// recalltype:
object GetSongRArtist {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    //options.addOption("downgrade", true, "input directory")
    //options.addOption("videoInfoPoolInput", true, "input directory")
    options.addOption("songCF", true, "input directory")
    options.addOption("label", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    //val downgrade = cmd.getOptionValue("downgrade")
    //val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val songCFInput = cmd.getOptionValue("songCF")
    val label = cmd.getOptionValue("label")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    /*val vidDowngradeM = spark.read.parquet(downgrade)
      .rdd
      .map(row => (row.getAs[Long](0), row.getAs[Double](1)))
      .collect()
      .toMap[Long, Double]
    val vidDowngradeM_broad = spark.sparkContext.broadcast(vidDowngradeM)*/

    /*// 获取song相关联的动态
    val songRelativeEventRDD = spark.read.parquet(videoInfoPoolInput)
      .filter($"bgmIds"=!="0")
      .select("videoId", "vType", "creatorId", "artistIds", "videoBgmIds"
        ,"bgmIds", "userTagIds" ,"auditTagIds", "category", "isControversial"
        ,"rawPrediction", "isMusicVideo")
      .withColumn("songIdForRec", explode(splitByDelim("_tab_")($"bgmIds")))
      .rdd*/
    // label
    val labelTable = spark.read.option("sep", ",").csv(label)
      .toDF("songId", "artistId")
      .groupBy("songId")
      .agg(
        collect_set($"artistId").as("labeledArtistIdSet")
      )
    // merged_cf_sim_song_artist
    val songCFTable = spark.sparkContext.textFile(songCFInput)
      .filter{line =>
        val info = line.split("\t")
        info.length >= 2 && !info(1).equals("null")
      }
      .map{line =>
        val info = line.split("\t")
        val songid = info(0).split("-")(0)
        val artists = info(1).split(",")
          .map { item =>
            val aInfo = item.split(":|-")
            val artistId = aInfo(0).toLong
            val vType = aInfo(1)
            val simScore = aInfo(2)
            val recalltypes = aInfo(3)
            /*var remain = false
            if (vType.equals("video") &&
              (recalltypes.contains("&") || usefulCFRecalltypeS.contains(recalltypes))  // 只保留有效的cf recalltypes
            )
              remain = true
            (artistId+":"+recalltypes, remain)*/
            artistId+":"+recalltypes+":"+simScore
          }
          /*.filter(tup => tup._2)
          .map(tup => tup._1)*/
        (songid, artists)
      }.toDF("songId", "artistCFAInfoS")

    val mergedTable = songCFTable
      .join(labelTable, Seq("songId"), "outer")
      .join(songCFTable, Seq("songId"), "outer")
      .map{line =>
        val songId = line.getAs[String]("songId")


        //val vidDowngradeM = vidDowngradeM_broad.value
        val addedS = scala.collection.mutable.Set[Long]()
        val artistsRet = new scala.collection.mutable.ArrayBuffer[String]()

        val artistCFAInfoS = line.getAs[Seq[String]]("artistCFAInfoS")
        if (artistCFAInfoS != null) {
          val cfRet = new scala.collection.mutable.ArrayBuffer[(Long, Int, Float)]()
          artistCFAInfoS.foreach { aidRecalltypesSimscore =>
            val aInfo = aidRecalltypesSimscore.split(":")
            val artistId = aInfo(0).toLong
            if (addedS.add(artistId)) {
              val recallCnt = aInfo(1).split("&").length
              val simScore = aInfo(2).toFloat
              cfRet.append((artistId, recallCnt, simScore))
            }
          }
          if (cfRet.size > 0) {
            //val cfSize = cfRet.length
            //val endIndex = if (cfSize > 20) 20 else cfSize
            artistsRet.appendAll(cfRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":artist:mcf")/*.slice(0, endIndex)*/)
          }
        }
        val labeledArtistIdSet = line.getAs[Seq[String]]("labeledArtistIdSet")
        if (labeledArtistIdSet != null && labeledArtistIdSet.size > 0) {
          //artistsRet.appendAll(labeledArtistIdSet.map(id => id + ":artist:label"))
          labeledArtistIdSet.foreach{id =>
            val artistId = id.toLong
            if (addedS.add(artistId))
              artistsRet.append(artistId+":artist:label")
          }
        }

        var artistsStr = "null"
        if (artistsRet.length > 0)
          artistsStr = artistsRet.mkString(",")
        (songId, artistsStr)
      }.toDF("songId", "artists")
      .filter($"artists"=!="null")
    //.cache
    mergedTable.write.option("sep", "\t").csv(output)

  }
}
