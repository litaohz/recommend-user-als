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

object GetUserMixedPref4CF_days_playFromSearch {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("userPref", true, "input directory")
    options.addOption("songProfile", true, "input directory")
    options.addOption("artistProfile", true, "input directory")
    options.addOption("days", true, "days")

    options.addOption("output", true, "output directory")
    options.addOption("outputIndex", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userPrefInput = cmd.getOptionValue("userPref")
    val songProfile = cmd.getOptionValue("songProfile")
    val artistProfile = cmd.getOptionValue("artistProfile")
    val days = cmd.getOptionValue("days").toInt

    val output = cmd.getOptionValue("output")
    val outputIndex = cmd.getOptionValue("outputIndex")


    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val timeInMillis = System.currentTimeMillis
    val millisInDay = 24 * 3600 * 1000l
    val qualifiedLogDateSeq = for (i <- (1 to days)) yield {
      sdf.format(new Date(timeInMillis - i * millisInDay))
    }
    val existsUserPrefInputPaths = getExistsPathUnderDirectory(userPrefInput, fs)
      .filter{path =>
        var remain = false
        qualifiedLogDateSeq.foreach(qualifiedLogDate => if (path.contains(qualifiedLogDate)) remain = true)
        remain
      }
    logger.info("valid date:" + qualifiedLogDateSeq.mkString(","))

    import spark.implicits._
    val userPref = spark.read.parquet(existsUserPrefInputPaths.toSeq : _*)
      .repartition(1000)
//      .filter(setContain("recommendvideo")($"impressPageSet"))    // 过滤非视频流下行为
//      .filter($"videoPref">=0)      // 过滤负反馈
//      .filter(filter4CFByAlg($"algSet"))    // 只保留个性化推荐产生的用户行为

    val videoFromVideoTable = userPref
      .groupBy($"videoId", $"vType", $"actionUserId")
      .agg(
        sum($"viewTime").as("totalViewTime"),
        collect_set($"actionTypeList").as("actionTypeListS"),
        collect_set($"playSourceSet").as("playSourceSetS"),
        collect_set("videoPref").as("prefSet")
      )
      .withColumn("isNotLike", isNotLike($"prefSet"))
      .filter(!$"isNotLike")    // 负反馈过滤
      .filter(!setSetContain("notlike")($"actionTypeListS") && !setSetContain("report")($"actionTypeListS"))
      .filter(setSetContain("search")($"playSourceSetS"))
      .filter($"totalViewTime">60 ||
        (setSetContain("zan")($"actionTypeListS") && !setSetContain("unzan")($"actionTypeListS")) ||
        (setSetContain("subscribe")($"actionTypeListS") && !setSetContain("unsubscribe")($"actionTypeListS")) ||
        (setSetContain("share")($"actionTypeListS") || setSetContain("share_top")($"actionTypeListS"))// TODO: 增加转发到各个app（微信之类）
      )
      .select(
        $"actionUserId".as("userId"),
        concat_ws("-", $"videoId", $"vType").as("itemId"),
        lit(5).as("pref")
      )

    val songProfileTable = spark.sparkContext.textFile(songProfile)
      .flatMap {line =>
        val info = line.split("\t")
        val userId = info(0)
        info(1).split(",").map{item => (userId, item.split(":")(0) + "-song", 5)}.toSet
      }.toDF("userId", "itemId", "pref")

    val artistProfileTable = spark.sparkContext.textFile(artistProfile)
      .flatMap {line =>
        val info = line.split("\t")
        val userId = info(0)
        info(1).split(",").map{item => (userId, item.split(":")(0) + "-artist", 5)}.toSet
      }.toDF("userId", "itemId", "pref")


    val finalDataset = videoFromVideoTable
      .union(songProfileTable)
      .union(artistProfileTable)
      .cache

    finalDataset
      .repartition(88)
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
      .repartition(88)
      .write.option("sep", "\t").csv(output + "/mixedId")

  }



}
