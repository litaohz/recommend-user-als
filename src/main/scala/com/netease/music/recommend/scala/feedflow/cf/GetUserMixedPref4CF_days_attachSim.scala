package com.netease.music.recommend.scala.feedflow.cf

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.getExistsPathUnderDirectory
import com.netease.music.recommend.scala.feedflow.userProfile.GetUserVideoPref_days.isNotLike
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GetUserMixedPref4CF_days_attachSim {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("userVideoPref", true, "input directory")
    options.addOption("simVideo_dup", true, "simVideo dup")
    options.addOption("days", true, "days")
    options.addOption("output", true, "output directory")
    options.addOption("outputIndex", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userVideoPrefInput = cmd.getOptionValue("userVideoPref")
    val simVideo_dup = cmd.getOptionValue("simVideo_dup")
    val days = cmd.getOptionValue("days").toInt
    val output = cmd.getOptionValue("output")
    val outputIndex = cmd.getOptionValue("outputIndex")

    import spark.implicits._

    val simVideo_dupTableM = spark.sparkContext.textFile(simVideo_dup)
      .flatMap{line =>
        val info = line.split("\t").map(video => video + "-video")
        val videos = info.toSet
        for(video <- info) yield {
          val simVideos = (videos - video).mkString(",")
          (video, simVideos)
        }
      }.collect
      .toMap[String, String]
    val broad_simVideo_dupTableM = spark.sparkContext.broadcast(simVideo_dupTableM)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val timeInMillis = System.currentTimeMillis
    val millisInDay = 24 * 3600 * 1000l
    val qualifiedLogDateSeq = for (i <- (1 to days)) yield {
      sdf.format(new Date(timeInMillis - i * millisInDay))
    }
    val existsUserVideoPrefInputPaths = getExistsPathUnderDirectory(userVideoPrefInput, fs)
      .filter{path =>
        var remain = false
        qualifiedLogDateSeq.foreach(qualifiedLogDate => if (path.contains(qualifiedLogDate)) remain = true)
        remain
      }
    logger.info("valid date:" + qualifiedLogDateSeq.mkString(","))

    val videoPref = spark.read.parquet(existsUserVideoPrefInputPaths.toSeq : _*)
      .repartition(1000)
//      .filter(setContain("recommendvideo")($"impressPageSet"))    // 过滤非视频流下行为
//      .filter($"videoPref">=0)      // 过滤负反馈
//      .filter(filter4CFByAlg($"algSet"))    // 只保留个性化推荐产生的用户行为

    val videoFromVideoTable = videoPref
      .groupBy($"videoId", $"vType", $"actionUserId")
      .agg(
        sum($"viewTime").as("totalViewTime"),
        sum($"zanCnt").as("totalZanCnt"),
        sum($"commentCnt").as("totalCommentCnt"),
        sum($"subscribeCnt").as("totalSubscribeCnt"),
        sum($"shareClickCnt").as("totalShareClickCnt"),
        collect_set("videoPref").as("prefSet")
      )
      .withColumn("isNotLike", isNotLike($"prefSet"))
      .filter(!$"isNotLike")    // 负反馈过滤
      .filter($"totalViewTime">30
        || ($"totalZanCnt">0 && $"totalCommentCnt">0)
        || $"totalSubscribeCnt">0
        || $"totalShareClickCnt">0
      )
      .select(
        $"actionUserId".as("userId"),
        concat_ws("-", $"videoId", $"vType").as("itemId")/*,
        lit(5).as("pref")*/
      )
      .rdd
      .flatMap{line =>
        val userId = line.getAs[String](0)
        val idtype = line.getAs[String](1)
        val simSet = scala.collection.mutable.HashSet[(String, String, Int)]()
        simSet.add((userId, idtype, 5))
        if (broad_simVideo_dupTableM.value.contains(idtype)) {
          for (sim <- broad_simVideo_dupTableM.value(idtype).split(",")) {
            simSet.add((userId, sim, 5))
          }
        }
        simSet
      }.toDF("userId", "itemId", "pref")

    val songFromVideoTable = videoPref
      .filter($"songIds"=!="0")
      .withColumn("songId", explode(split($"songIds", "_tab_")))
      .groupBy($"songId", $"actionUserId")
      .agg(
        sum($"viewTime").as("totalViewTime"),
        sum($"zanCnt").as("totalZanCnt"),
        sum($"commentCnt").as("totalCommentCnt"),
        sum($"subscribeCnt").as("totalSubscribeCnt"),
        sum($"shareClickCnt").as("totalShareClickCnt")
      )
      .filter($"totalViewTime" > 300 || $"totalViewTime">60 && ($"totalZanCnt">2 && $"totalCommentCnt">1 || $"totalSubscribeCnt">1 || $"totalShareClickCnt">1))
      .select(
        $"actionUserId".as("userId"),
        concat_ws("-", $"songId", lit("song")).as("itemId"),
        lit(5).as("pref")
      )

    val artistFromVideoTable = videoPref
      .filter($"artistIds"=!="0")
      .withColumn("artistId", explode(split($"artistIds", "_tab_")))
      .groupBy($"artistId", $"actionUserId")
      .agg(
        sum($"viewTime").as("totalViewTime"),
        sum($"zanCnt").as("totalZanCnt"),
        sum($"commentCnt").as("totalCommentCnt"),
        sum($"subscribeCnt").as("totalSubscribeCnt"),
        sum($"shareClickCnt").as("totalShareClickCnt")
      )
      .filter($"totalViewTime" > 300 || $"totalViewTime">60 && ($"totalZanCnt">2 && $"totalCommentCnt">1 || $"totalSubscribeCnt">1 || $"totalShareClickCnt">1))
      .select(
        $"actionUserId".as("userId"),
        concat_ws("-", $"artistId", lit("artist")).as("itemId"),
        lit(5).as("pref")
      )


    val finalDataset = videoFromVideoTable
      .union(songFromVideoTable)
      .union(artistFromVideoTable)
      .cache

//    finalDataset.count

    finalDataset
      .repartition(16)
//      .coalesce(16)
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
      .coalesce(16)
      .write.option("sep", "\t").csv(output + "/mixedId")

  }



}
