package com.netease.music.recommend.scala.feedflow.userProfile

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.cf.GetUserVideoPref4CF_days.getPlayratio
import com.netease.music.recommend.scala.feedflow.cf.setSetContain
import com.netease.music.recommend.scala.feedflow.utils.ABTest
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{collect_set, _}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable

object GetUserVideoPref_days {

  case class VideoPrefInfoWithRank(prefInfo:String, rank:Int)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass)

    val options = new Options
    options.addOption("userVideoPref", true, "input directory")
    options.addOption("days", true, "days")
    options.addOption("Music_MVMeta4Video", true, "input directory")
    options.addOption("videoResolution", true, "input directory")
    options.addOption("output", true, "output directory")
    options.addOption("output4Recall", true, "output directory")
//    options.addOption("abtestConfig", true, "abtestConfig file")
//    options.addOption("abtestExpName", true, "abtest Exp Name")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userVideoPrefInput = cmd.getOptionValue("userVideoPref")
    val days = cmd.getOptionValue("days").toInt
    val Music_MVMeta4Video = cmd.getOptionValue("Music_MVMeta4Video")
    val videoResolutionInput = cmd.getOptionValue("videoResolution")
    val outputPath = cmd.getOptionValue("output")
    val output4Recall = cmd.getOptionValue("output4Recall")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val existsUserVideoPrefInputPaths = getExistsPathUnderDirectory(userVideoPrefInput, fs)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val timeInMillis = System.currentTimeMillis
    val millisInDay = 24 * 3600 * 1000l
    val qualifiedLogDateSeq = for (i <- (1 to days)) yield {
      sdf.format(new Date(timeInMillis - i * millisInDay))
    }
    logger.info("valid date:" + qualifiedLogDateSeq.mkString(","))
    val existsQualifiedPrefPaths = existsUserVideoPrefInputPaths.filter{path =>
      var remain = false
      qualifiedLogDateSeq.foreach{qualifiedDate =>
        if (path.contains(qualifiedDate))
          remain = true
      }
      remain
    }


//    val configText = spark.read.textFile(cmd.getOptionValue("abtestConfig")).collect()
//    val abtestExpName = cmd.getOptionValue("abtestExpName")
//    for (cf <- configText) {
//      if (cf.contains(abtestExpName)) {
//        println(cf)
//      }
//    }

    import spark.implicits._

    val window = Window.partitionBy($"actionUserId").orderBy($"videoPrefDays".desc)
    val userVideoPrefTable = spark.read.parquet(existsQualifiedPrefPaths.toSeq : _*)
      .filter($"logDate".isin(qualifiedLogDateSeq : _*))

    println("zhp info:" + userVideoPrefTable.printSchema())

    val finalDataRDD4Pref = userVideoPrefTable
//      .filter($"isNextplay"===0 ||
//        ($"isNextplay"===1/* && abtestGroupName(configText, abtestExpName)($"actionUserId")=!="t1"*/)
//      )
      .withColumn("playend", setContain("playend")($"playTypes"))
      .groupBy("videoId", "vType", "actionUserId")
      .agg(
        sum("videoPref").as("videoPrefDays")
        ,sum("viewTime").as("viewTimeTotal")
        ,(sum("zanCnt") > 0).as("isZan")
        ,(sum("commentCnt") > 0).as("isComment")
        ,(sum("subscribeCnt") > 0).as("isSubscribe")
        ,(sum("shareClickCnt") > 0).as("isShareClick")
        ,(sum($"playend".cast(IntegerType)) > 0).as("isPlayend")
        ,collect_set("videoPref").as("prefSet")
      )
      .withColumn("videoPrefInfo", getVideoPrefInfo($"videoId", $"vType", $"videoPrefDays"))
      .withColumn("isNotLike", isNotLike($"prefSet"))
      .filter(!$"isNotLike")
      .drop("prefSet", "isNotLike")
      .filter($"videoPrefDays">0)
      .select($"actionUserId", $"videoPrefInfo", row_number().over(window).as("prefRank"))
      .rdd
      .map {line =>
        (line.getString(0), VideoPrefInfoWithRank(line.getString(1), line.getInt(2)))
      }
      .groupByKey.map ({
        case (key, value) => collectPrefs(key, value)
      })
      .filter {line =>
        if (line.equals("null"))
          false
        else
          true
      }
    finalDataRDD4Pref.coalesce(16, true).saveAsTextFile(outputPath)



    val mvDurationTable = spark.read.parquet(Music_MVMeta4Video)
      .select(
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

    val finalDataRDD4Recall = userVideoPrefTable
//      .filter($"isNextplay"===0 ||
//        ($"isNextplay"===1/* && abtestGroupName(configText, abtestExpName)($"actionUserId")=!="t2"*/)
//      )
      .withColumn("playend", setContain("playend")($"playTypes"))
      .groupBy("videoId", "vType", "actionUserId")
      .agg(
        sum("videoPref").as("videoPrefDays")
        ,sum("viewTime").as("viewTimeTotal")
        ,(sum("zanCnt") > 0).as("isZan")
        ,(sum("commentCnt") > 0).as("isComment")
        ,(sum("subscribeCnt") > 0).as("isSubscribe")
        ,(sum("shareClickCnt") > 0).as("isShareClick")
        ,(sum($"playend".cast(IntegerType)) > 0).as("isPlayend")
        ,collect_set($"actionTypeList").as("actionTypeListS")
        ,collect_set($"playSourceSet").as("playSourceSetS")
        ,collect_set("videoPref").as("prefSet")
      )
      .withColumn("videoPrefInfo", getVideoPrefInfo($"videoId", $"vType", $"videoPrefDays"))
      .withColumn("isNotLike", isNotLike($"prefSet"))
      .filter(!$"isNotLike")
      .drop("prefSet", "isNotLike")
      .filter($"videoPrefDays">0)
      .join(durationTable, Seq("videoId", "vType"), "left_outer").na.fill(0.0d, Seq("maxDuration"))
      .withColumn("playratio", getPlayratio($"viewTimeTotal", $"maxDuration"))
      .filter(
        ($"isZan" && $"isComment")
          || $"isShareClick"
          || $"isSubscribe"
          || $"isPlayend"
          || $"viewTimeTotal" >= 60
          || /*(abtestGroupName(configText, abtestExpName)($"actionUserId")=!="t1" &&*/ (
          $"playratio">0.8 ||
//            (setSetContain("zan")($"actionTypeListS") && !setSetContain("unzan")($"actionTypeListS")) ||
            (setSetContain("subscribe")($"actionTypeListS") && !setSetContain("unsubscribe")($"actionTypeListS")) ||
            (setSetContain("share")($"actionTypeListS") || setSetContain("share_top")($"actionTypeListS"))
        )
//          || (abtestGroupName(configText, abtestExpName)($"actionUserId")=!="t2" && $"viewTimeTotal" >= 30)
      )
      .select($"actionUserId", $"videoPrefInfo", row_number().over(window).as("prefRank"))
      .rdd
      .map {line =>
        (line.getString(0), VideoPrefInfoWithRank(line.getString(1), line.getInt(2)))
      }
      .groupByKey.map ({
        case (key, value) => collectPrefs(key, value)
      })
      .filter {line =>
        if (line.equals("null"))
          false
        else
          true
      }
    finalDataRDD4Recall.coalesce(16, true).saveAsTextFile(output4Recall)

  }

  def collectPrefs(userId:String, prefTuples:Iterable[VideoPrefInfoWithRank]):String = {
    val videoPrefLinkedSet = mutable.LinkedHashSet[String]()
    var prefThread = 3.0
    if (prefTuples.size > 80)
      prefThread = 10.0
    else if (prefTuples.size > 30)
      prefThread = 6.0
    else if (prefTuples.size < 5)
      prefThread = 2.0
    prefTuples.toArray.sortWith(_.rank < _.rank)
      .foreach {prefInfoWithRank =>
        val prefDays = prefInfoWithRank.prefInfo.split(":")(2).toDouble
        if (prefDays >= prefThread)
          videoPrefLinkedSet.add(prefInfoWithRank.prefInfo)
      }
//    if (userId.equals("44001") || userId.equals("135571358")) {
//      println("zhpjunfei:\n" + userId + "," + prefThread + "," + videoPrefLinkedSet.size + "," + prefTuples.size + "--------------" + videoPrefLinkedSet.toString)
//    }

    if (videoPrefLinkedSet.size > 0)
      userId + "\t" + videoPrefLinkedSet.mkString(",")
    else
      "null"
  }

  def getExistsPathUnderDirectory(inputDir:String, fs:FileSystem):collection.mutable.Set[String] = {
    val set = collection.mutable.Set[String]()
    for (status <- fs.listStatus(new Path(inputDir))) {
      val input = status.getPath.getParent + "/" + status.getPath.getName + "/parquet"
      set.add(input)
    }
    set
  }

  def getVideoPrefInfo = udf((videoId:Long, vType:String, videoPrefDays:Double) => {
    videoId + ":" + vType + ":" + videoPrefDays.formatted("%.4f")
  })

  def isNotLike = udf((prefSet:Seq[Double]) => {
    var notLike = false
    prefSet.foreach{pref =>
      if (pref < 0)
        notLike = true
    }
    notLike
  })

  def setContain(str:String) = udf((set:Seq[String]) => {
    if (set == null || set.isEmpty)
      false
    else
      set.contains(str)
  })

  def setContainContain(str:String) = udf((set:Seq[String]) => {
    if (set == null || set.isEmpty)
      false
    else {
      var ret = false
      set.foreach{item =>
        if(item.contains(str))
          ret = true
      }
      ret
    }
  })

  def setSize = udf((set:Seq[String]) => {
    if (set == null || set.isEmpty)
      0
    else
      set.size
  })

  def abtestGroupName(configText: Array[String], abtestExpName: String) =udf((uid:String) => {
    val musicABTest = new ABTest(configText)
    musicABTest.abtestGroup(uid.toLong, abtestExpName)
  })
}
