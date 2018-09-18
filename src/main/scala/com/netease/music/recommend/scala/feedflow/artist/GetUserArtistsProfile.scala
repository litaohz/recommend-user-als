package com.netease.music.recommend.scala.feedflow.artist

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by hzzhangjunfei1 on 2017/8/17.
  */
object GetUserArtistsProfile {

  case class UserId(userId:Long)
  case class ArtistId(artistId:Long)
  case class ControversialArtistId(controversialArtistId:Long)
  case class UserRecArtistInfo(userId:Long, recArtistInfo:String)
  case class UserSubArtistInfo(userId:Long, subArtistInfo:String)
  case class UserSubSimilarArtistInfo(userId:Long, subSimilarArtistInfo:String)
  case class UserPrefArtistFromVideoInfo(userId:Long, prefArtistFromVideoInfo:String)
  case class UserSubArtistAndSubtime(userId:Long, subArtistId:Long, subTime:Long)
  case class SimilarArtistAndScore(originArtistId:Long, simArtistId:Long, simScore:Float)


  def collectUserSubArtists(artistsFromArtistRelativeVideoSet:mutable.Set[Long])(userId:Long, artists:Iterable[(Long, Long)]):UserSubArtistInfo = {

    val set = mutable.Set[String]()
    for (artist <- artists) {

      if (artistsFromArtistRelativeVideoSet.contains(artist._1)) {
        set += (artist._1 + ":" + artist._2)
      }
    }
    UserSubArtistInfo(userId, set.mkString(","))
  }

  def collectUserSimilarArtists(userId:Long, artists:Iterable[(Long, Float)]):UserSubSimilarArtistInfo ={

    val mergeedArtists = artists.groupBy(_._1).map(tup => (tup._1, tup._2.map(_._2).reduce(_ + _)))
//    artists.map(_._2).reduce(_ + _)
    val sortedArtist = mergeedArtists.toArray.sortWith(_._2 > _._2)
    var count = 0
    val resArrBuffer = ArrayBuffer[String]()
    for (artist <- sortedArtist) {
      if (count <= 9) {
        resArrBuffer += (artist._1 + ":" + artist._2)
        count += 1
      }
    }
    UserSubSimilarArtistInfo(userId, resArrBuffer.mkString(","))
  }

  def hasResult = udf((result: String) => {

    if (result.length > 0 && !result.equals("null"))
      true
    else
      false
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("artistRelativeVideosInput", true, "input directory")
    options.addOption("controverSialArtistsInput", true, "input directory")
    options.addOption("artistRecInput", true, "input directory")
    options.addOption("artistSubInput", true, "input directory")
    options.addOption("artistSimilarInput", true, "input directory")
    options.addOption("artistPrefFromVideo", true, "input directory")
    options.addOption("outputPath", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val artistRelativeVideosInput = cmd.getOptionValue("artistRelativeVideosInput")
    val controverSialArtistsInput = cmd.getOptionValue("controverSialArtistsInput")
    val artistRecInput = cmd.getOptionValue("artistRecInput")
    val artistSubInput = cmd.getOptionValue("artistSubInput")
    val artistSimilarInput = cmd.getOptionValue("artistSimilarInput")
    val artistPrefFromVideoInput = cmd.getOptionValue("artistPrefFromVideo")
    val outputPath = cmd.getOptionValue("outputPath")

    import spark.implicits._
    // 获取能关联出动态的艺人Set
    val artistsFromArtistRelativeVideoSet = mutable.Set[Long]()
    spark.sparkContext.textFile(artistRelativeVideosInput)
      .map(line => {
        val info = line.split("\t")
        val artistId = info(0).toLong
        ArtistId(artistId)
      })
      .collect
      .foreach(artist => {
        artistsFromArtistRelativeVideoSet += artist.artistId
      })


    // 获取争议艺人Set
    val controverSialArtistSet = mutable.Set[Long]()
    spark.sparkContext.textFile(controverSialArtistsInput)
      .map(line => {
        val info = line.split("\t")
        val artistId = info(0).toLong
        ControversialArtistId(artistId)
      })
      .collect
      .foreach(controversialArtist => {
        controverSialArtistSet += controversialArtist.controversialArtistId
      })
    // 广播变量
    val broadcastArtistsFromArtistRelativeVideoSet = spark.sparkContext.broadcast(artistsFromArtistRelativeVideoSet)
    val broadcastControverSialArtistSet = spark.sparkContext.broadcast(controverSialArtistSet)

    // 获取推荐艺人信息
    val artistRecTable = spark.sparkContext.textFile(artistRecInput)
      .map(line => {
        val info = line.split("\t")
        val userId = info(0).toLong
        val artists = (parse(info(1)) \ "artists").children

        val hasRelativeVideosArtistSet = broadcastArtistsFromArtistRelativeVideoSet.value
        val controverSialArtistSet = broadcastControverSialArtistSet.value
        var i = 0
        val set = mutable.Set[String]()
        implicit val formats = DefaultFormats
        for (artist <- artists) {
          if (i <= 10) {
            val artistId = (artist \ "id").extractOrElse[String]("0")
            if (hasRelativeVideosArtistSet.contains(artistId.toLong) && !controverSialArtistSet.contains(artistId.toLong)) {
              val score = (artist \ "score").extractOrElse[String]("-1").toFloat
              if (score >= 0.58) {
                set += (artistId + ":" + score)
                i += 1
              }
            }
          }
        }
        UserRecArtistInfo(userId, set.mkString(","))
      })
      .toDF
      .filter(length($"recArtistInfo") > 0)

    // 获取订阅艺人信息
    // 1. 获取用户订阅艺人信息表
    val artistSubOriginTable = spark.sparkContext.textFile(artistSubInput)
      .repartition(88)
      .map(line => {
        implicit val formats = DefaultFormats
        val json = parse(line)
        val userId = (json \ "UserId").extractOrElse[Long](0)
        val artistId = (json \ "ArtistId").extractOrElse[Long](0)
        val subTime = (json \ "SubTime").extractOrElse[Long](0)
        UserSubArtistAndSubtime(userId, artistId, subTime)
      })
      .toDF
      .cache
    // 2. 整合用户订阅艺人信息（长表->宽表）
    val artistSubTable = artistSubOriginTable
      .rdd
      .map(row => {(row.getLong(0), (row.getLong(1), row.getLong(2)))})
      .groupByKey
      .map({ case (key, value) => collectUserSubArtists(broadcastArtistsFromArtistRelativeVideoSet.value)(key, value)})
      .toDF
      .filter(length($"subArtistInfo") > 0)

    // 获取订阅艺人的相似艺人信息
    // 1. 获取相似艺人映射表
    val artistSimilarOriginTable = spark.sparkContext.textFile(artistSimilarInput)
      .flatMap(line => {
        val hasRelativeVideosArtistSet = broadcastArtistsFromArtistRelativeVideoSet.value
        val controverSialArtistSet = broadcastControverSialArtistSet.value

        val info = line.split("\t")
        val originArtistId = info(0).toLong
        val set = mutable.Set[String]()
        var count = 0
        for (artist <- info(1).split(",")) {
          if (count <= 5) {
            val artistId = artist.split(":")(0).toLong
            if (hasRelativeVideosArtistSet.contains(artistId.toLong) && !controverSialArtistSet.contains(artistId.toLong)) {
              val simScore = artist.split(":")(1).toFloat
              if (simScore >= 0.1) {
                set += (artistId + ":" + simScore)
                count += 1
              }
            }
          }
        }
        set.map(line => {
          val info = line.split(":")
          val simArtistId = info(0).toLong
          val simScore = info(1).toFloat
          SimilarArtistAndScore(originArtistId, simArtistId, simScore)
        })
      })
      .toDF
    // 2. 获取用户订阅艺人的相似艺人信息（长表->宽表）
    val artistSubSimilarTable = artistSubOriginTable
      .join(artistSimilarOriginTable, $"subArtistId" === $"originArtistId", "inner")
      .select("userId", "simArtistId", "simScore")
      .rdd
      .map(line => {(line.getLong(0), (line.getLong(1), line.getFloat(2)))})
      .groupByKey
      .map({case (key, value) => collectUserSimilarArtists(key, value)})
      .toDF
      .filter(length($"subSimilarArtistInfo") > 0)

    val artistPrefFromVideoTable = spark.sparkContext.textFile(artistPrefFromVideoInput)
      .map {line =>
        val info = line.split("\t")
        val userId = info(0).toLong
        val prefBuffer = new StringBuilder()
        for (prefInfoStr <- info(1).split(",")) {
          val prefInfo = prefInfoStr.split(":")
          prefBuffer.append(prefInfo(0) + ":" + prefInfo(1) + ":" + prefInfo(2).split("-").length + ",")
        }
        UserPrefArtistFromVideoInfo(userId, prefBuffer.deleteCharAt(prefBuffer.length-1).toString)
      }.toDF

    val userRelativeArtistsTable = artistSubTable
      .join(artistRecTable, Seq("userId"), "outer")
      .join(artistSubSimilarTable, Seq("userId"), "outer")
      .join(artistPrefFromVideoTable, Seq("userId"), "outer")
      .na.fill(0, Seq("userCnt"))
      .na.fill("null", Seq("subArtistInfo", "recArtistInfo", "subSimilarArtistInfo", "prefArtistFromVideoInfo"))
      .filter(hasResult($"subArtistInfo") || hasResult($"recArtistInfo") || hasResult($"subSimilarArtistInfo") || hasResult($"prefArtistFromVideoInfo"))

    userRelativeArtistsTable.write.option("sep", "\t").csv(outputPath)
  }
}
