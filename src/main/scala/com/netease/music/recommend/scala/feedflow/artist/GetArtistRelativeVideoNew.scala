package com.netease.music.recommend.scala.feedflow.artist

import com.netease.music.recommend.scala.feedflow.RealtimeAlgConstant
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hzzhangjunfei1 on 2017/8/17.
  */
object GetArtistRelativeVideoNew {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("videoInfoPoolInput", true, "input directory")
    options.addOption("videoKeywordInput", true, "input directory")
    options.addOption("artistName", true, "input directory")
    options.addOption("output", true, "output directory")
    options.addOption("outputOnline", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val videoInfoPoolInput = cmd.getOptionValue("videoInfoPoolInput")
    val videoKeywordInput = cmd.getOptionValue("videoKeywordInput")
    val artistName = cmd.getOptionValue("artistName")
    val output = cmd.getOptionValue("output")
    val outputOnline = cmd.getOptionValue("outputOnline")

    import spark.implicits._

    // artistName
    val artistNameTable = spark.sparkContext.textFile(artistName)
      .map{line =>
        val info = line.split("\t")
        val artistId = info(0)
        val artistName = info(1)
        var score = 0l
        if (info.length >= 4 && !info(3).isEmpty)
          score = info(3).toLong
        else
          println("artistName info:info.length:" + info.length + ",line:" + line)
        (artistId, artistName, score)
      }.toDF("artistId", "artistName", "score")
      .filter($"score">5000)
      .drop("score")

    // keyword
    val videoKWInfoTable = spark.sparkContext.textFile(videoKeywordInput)
      .flatMap{line =>
        val info = line.split("\t")
        val videoId = info(0).toLong
        val vType = info(1)
        var keyS = Set[String]()
        for (item <- info(2).split(",")) yield {
          keyS += item.split("_")(0)
        }
        for (keywrod <- keyS) yield {
          (videoId, vType, keywrod)
        }
      }.toDF("videoId", "vType", "keyword")
      .join(artistNameTable, $"keyword"===$"artistName", "inner")
      .withColumnRenamed("artistId", "artistIdFromKeyword")
      .groupBy("videoId", "vType")
      .agg(
        collect_set("artistIdFromKeyword").as("artistIdFromKeywordSet")
      )
      .select(
        $"videoId"
        ,$"vType"
        ,concat_ws("_tab_", $"artistIdFromKeywordSet").as("artistIdsFromKW")
      )

    val videoInfoPoolTable = spark.read.parquet(videoInfoPoolInput)
      .join(videoKWInfoTable, Seq("videoId", "vType"), "left_outer")
      .na.fill("0", Seq("artistIdsFromKW"))

    // 获取artist相关联的动态
    val artistRelativeEventTable0 = videoInfoPoolTable
      .filter($"artistIds"=!="0")
      .withColumn("type", lit(0))
      .withColumn("videoInfo", concat_ws(":", $"videoId", $"vType", $"creatorId", $"artistIds", $"videoBgmIds"
                                            , $"bgmIds", $"userTagIds" ,$"auditTagIds", $"category", $"isControversial"
                                            , $"rawPrediction", $"isMusicVideo", $"type"))
      .withColumn("videoInfo4Online", concat_ws(":", $"videoId", $"vType", lit("artist"), $"rawPrediction", $"type"))
      .withColumn("artistIdForRec", explode(splitByDelim("_tab_")($"artistIds")))
      .withColumn("idAndType", concat_ws(":", $"videoId", $"vType"))
      .select("artistIdForRec", "type", "idAndType", "videoInfo", "videoInfo4Online", "rawPrediction")

    val artistRelativeEventTable1 = videoInfoPoolTable
      .filter($"artistIdsFromKW"=!="0")
      .withColumn("type", lit(1))
      .withColumn("videoInfo", concat_ws(":", $"videoId", $"vType", $"creatorId", $"artistIdsFromKW", $"videoBgmIds"
                                            , $"bgmIds", $"userTagIds", $"auditTagIds", $"category", $"isControversial"
                                            , $"rawPrediction", $"isMusicVideo", $"type"))
      .withColumn("videoInfo4Online", concat_ws(":", $"videoId", $"vType", lit("artist"), $"rawPrediction", $"type"))
      .withColumn("artistIdForRec", explode(splitByDelim("_tab_")($"artistIdsFromKW")))
      .withColumn("idAndType", concat_ws(":", $"videoId", $"vType"))
      .select("artistIdForRec", "type", "idAndType", "videoInfo", "videoInfo4Online", "rawPrediction")

    val artistRelativeEventTable = artistRelativeEventTable0
      .union(artistRelativeEventTable1)
      .rdd
      .map {line =>
        ((line.getString(0), line.getString(2)), (line.getInt(1), line.getString(3), line.getString(4), line.getDouble(5)))
      }
      .groupByKey
      .map({case (key, values) => reduceVideoByType(key, values)})
      .toDF("artistId", "type", "videoInfo", "videoInfo4Online", "rawPrediction")


    val offlineDataRDD = artistRelativeEventTable
      .rdd
      .map(line => {
        (line.getString(0), line.getString(2))
      })
      .groupByKey
      .map({case (key, values) => collectVideoByKey(key, values)})
    offlineDataRDD.saveAsTextFile(output)

    // 获取艺人相关视频 4 实时推荐
    val onlinelineDataRDD = artistRelativeEventTable
      .rdd
      .map{line =>
        (line.getString(0), OnlineVideoInfoNew(line.getString(3), line.getDouble(4)))
      }
      .groupByKey
      .map({case (key, values) => collectOnlineVideoByKeyNew(key, values)})
    onlinelineDataRDD.saveAsTextFile(outputOnline)

  }


}
