package com.netease.music.recommend.scala.feedflow.reason

import com.netease.music.recommend.scala.feedflow.dotVectors
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetReasonFeedbackInfo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("reasonPromotion", true, "input digrectory")
    options.addOption("outputArtist", true, "output directory")
    options.addOption("outputSong", true, "output directory")
    options.addOption("outputVideo", true, "output directory")
    options.addOption("outputUser", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val reasonPromotion = cmd.getOptionValue("reasonPromotion")

    val outputArtist = cmd.getOptionValue("outputArtist")
    val outputSong = cmd.getOptionValue("outputSong")
    val outputVideo = cmd.getOptionValue("outputVideo")
    val outputUser = cmd.getOptionValue("outputUser")

    import spark.implicits._

    val promotionInfoTable = spark.read.parquet(reasonPromotion)
      .filter($"playPromotionBeta"<0.9 && $"playTimePromotion"<0.9  && $"pairImpressCount">=10)
      .select(
        "videoId", "vType", "reasonId", "reasonType", "playPromotionBeta"
        ,"playTimePromotion", "playendPromotion"
      )

    val assembler = new VectorAssembler()
      .setInputCols(Array("playPromotionBeta", "playTimePromotion", "playendPromotion"))
      .setOutputCol("assembledFeatures")
    val scaler = new MinMaxScaler()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("features")
    val promotionInfoTable_stage2 = assembler.transform(promotionInfoTable)
    val scalerModel = scaler.fit(promotionInfoTable_stage2)
    val promotionInfoFinal = scalerModel.transform(promotionInfoTable_stage2)
      .withColumn("promotionScore", dotVectors(Array(0.5, 0.5, 0.1))($"features"))

    // artist
    val artistPromotionTable = promotionInfoFinal
      .filter($"reasonType"==="artist")
      .withColumn("videoInfo", concat_video(":")($"videoId", $"promotionScore"))
      .groupBy($"reasonId")
      .agg(collect_set($"videoInfo"))
      .rdd
      .map(line => {
        val artistId = line.getLong(0)
        val videoInfos = line.getSeq[String](1)
        val ret = videoInfos.map(line => {
          val info = line.split(":")
          (info(0), info(1).toDouble)
        }).sortWith(_._2 < _._2)
          .map(line => line._1 + ":" + line._2)
        artistId + "\t" + ret.mkString(",")
      })
      .coalesce(10)
    artistPromotionTable.saveAsTextFile(outputArtist)

    // song
    val songPromotionTable = promotionInfoFinal
      .filter($"reasonType"==="song")
      .withColumn("videoInfo", concat_video(":")($"videoId", $"promotionScore"))
      .groupBy($"reasonId")
      .agg(collect_set($"videoInfo"))
      .rdd
      .map(line => {
        val songId = line.getLong(0)
        val videoInfos = line.getSeq[String](1)
        val ret = videoInfos.map(line => {
          val info = line.split(":")
          (info(0), info(1).toDouble)
        }).sortWith(_._2 < _._2)
          .map(line => line._1 + ":" + line._2)
        songId + "\t" + ret.mkString(",")
      })
      .coalesce(10)
    songPromotionTable.saveAsTextFile(outputSong)

    // video
    val videoPromotionTable = promotionInfoFinal
      .filter($"reasonType"==="video")
      .withColumn("videoInfo", concat_video(":")($"videoId", $"promotionScore"))
      .groupBy($"reasonId")
      .agg(collect_set($"videoInfo"))
      .rdd
      .map(line => {
        val videoId = line.getLong(0)
        val videoInfos = line.getSeq[String](1)
        val ret = videoInfos.map(line => {
          val info = line.split(":")
          (info(0), info(1).toDouble)
        }).sortWith(_._2 < _._2)
          .map(line => line._1 + ":" + line._2)
        videoId + "\t" + ret.mkString(",")
      })
      .coalesce(10)
    videoPromotionTable.saveAsTextFile(outputVideo)

    // user
    val userPromotionTable = promotionInfoFinal
      .filter($"reasonType"==="user")
      .withColumn("videoInfo", concat_video(":")($"videoId", $"promotionScore"))
      .groupBy($"reasonId")
      .agg(collect_set($"videoInfo"))
      .rdd
      .map(line => {
        val videoId = line.getLong(0)
        val videoInfos = line.getSeq[String](1)
        val ret = videoInfos.map(line => {
          val info = line.split(":")
          (info(0), info(1).toDouble)
        }).sortWith(_._2 < _._2)
          .map(line => line._1 + ":" + line._2)
        videoId + "\t" + ret.mkString(",")
      })
      .coalesce(10)
    userPromotionTable.saveAsTextFile(outputUser)
  }

}
