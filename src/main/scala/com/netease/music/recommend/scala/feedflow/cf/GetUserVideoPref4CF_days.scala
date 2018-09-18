package com.netease.music.recommend.scala.feedflow.cf

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.getExistsPathUnderDirectory
import com.netease.music.recommend.scala.feedflow.userProfile.GetUserVideoPref_days.isNotLike
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object GetUserVideoPref4CF_days {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("userVideoPref", true, "input directory")
    options.addOption("days", true, "days")
    options.addOption("Music_MVMeta4Video", true, "input directory")
    options.addOption("videoResolution", true, "input directory")
    options.addOption("output", true, "output directory")
    //options.addOption("outputIndex", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userVideoPrefInput = cmd.getOptionValue("userVideoPref")
    val days = cmd.getOptionValue("days").toInt
    val Music_MVMeta4Video = cmd.getOptionValue("Music_MVMeta4Video")
    val videoResolutionInput = cmd.getOptionValue("videoResolution")
    val output = cmd.getOptionValue("output")
    //val outputIndex = cmd.getOptionValue("outputIndex")


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

    import spark.implicits._
    val videoPref = spark.read.parquet(existsUserVideoPrefInputPaths.toSeq : _*)
      .repartition(1000)
//      .filter(setContain("recommendvideo")($"impressPageSet"))    // 过滤非视频流下行为
//      .filter($"videoPref">=0)      // 过滤负反馈
//      .filter(filter4CFByAlg($"algSet"))    // 只保留个性化推荐产生的用户行为

    val mvDurationTable = spark.read.parquet(Music_MVMeta4Video)
      .select(
//        concat_ws("-", $"ID", lit("mv")),
        $"ID".as("videoId"),
        lit("mv").as("vType"),
        ($"Duration"/1000).as("duration")
      )
    val videoDurationTable = spark.sparkContext.textFile(videoResolutionInput)
      .map{line =>
        val info = line.split("\01")
        val videoId = info(1).toLong
        val duration = info(11).toDouble / 1000
        (videoId, "video", duration)
      }.toDF("videoId", "vType", "duration")
    val durationTable = mvDurationTable.union(videoDurationTable)
      .groupBy($"videoId", $"vType")
      .agg(
        max($"duration").as("maxDuration")
      )


    val videoFromVideoTable = videoPref
      .groupBy($"videoId", $"vType", $"actionUserId")
      .agg(
        sum($"viewTime").as("totalViewTime"),
        collect_set($"playTypes").as("playTypesS"),
        collect_set($"actionTypeList").as("actionTypeListS"),
        collect_set($"playSourceSet").as("playSourceSetS"),
        collect_set("videoPref").as("prefSet"),
        max($"lastLogtime").as("lastLogtime")
      )
      .withColumn("isNotLike", isNotLike($"prefSet"))
      .filter(!$"isNotLike")    // 负反馈过滤
      .filter(!setSetContain("notlike")($"actionTypeListS") && !setSetContain("report")($"actionTypeListS"))
      .filter(setSetContain("search")($"playSourceSetS"))
      .join(durationTable, Seq("videoId", "vType"), "left_outer").na.fill(0.0d, Seq("maxDuration"))
      .withColumn("playratio", getPlayratio($"totalViewTime", $"maxDuration"))
      .filter($"totalViewTime">20 ||
        $"playratio">0.8 ||
        setSetContain("playend")($"playTypesS") ||
        (setSetContain("zan")($"actionTypeListS") && !setSetContain("unzan")($"actionTypeListS")) ||
        (setSetContain("subscribe")($"actionTypeListS") && !setSetContain("unsubscribe")($"actionTypeListS")) ||
        (setSetContain("share")($"actionTypeListS") || setSetContain("share_top")($"actionTypeListS"))// TODO: 增加转发到各个app（微信之类）
      )
      .select(
        $"actionUserId".as("userId"),
        concat_ws("-", $"videoId", $"vType").as("idtype"),
        //concat_ws("-", $"videoId", $"vType", $"lastLogtime").as("idTypeTime"),
        $"lastLogtime"
      )
      .rdd
      .map(row => (row.getString(0), (row.getString(1), row.getLong(2))))
      .groupByKey
      .map{case(key, values) =>
        val userId = key
        val videos_sorted = values
          .toArray.sortWith(_._2 < _._2).map(tup => tup._1).mkString(",")
        userId + "\t" + videos_sorted
      }
    videoFromVideoTable
      .repartition(16) // 合并小文件
      .saveAsTextFile(output)

  }


  def getPlayratio = udf((viewTime:Long, duration:Double) => {
    if (duration > 0)
      viewTime / duration
    else
      0.0d
  })

}
