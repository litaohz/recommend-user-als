package com.netease.music.recommend.scala.feedflow.videoProfile

//import com.netease.music.recommend.scala.feedflow.videoProfile.GetVideoClickrate.getClickrate
import com.netease.music.recommend.scala.feedflow.videoProfile.GetVideoHotScore.getExistsPath
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetVideoActionInfo4ClickrateAlgFilter {

  case class Video(videoId:Long, vType:String)
  case class Impress(videoId:Long, vType:String, actionUserId:String, page:String, os:String, alg:String)
  case class PlayAction(videoId:Long, vType:String, actionUserId:String, time:Long, source:String, isNewversion:String, os:String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
//    options.addOption("videoPoolNdays", true, "log input directory")
    options.addOption("impress", true, "log input directory")
    options.addOption("playend", true, "log input directory")
    options.addOption("dataDate", true, "dataDate")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

//    val videoPoolNdaysInput = cmd.getOptionValue("videoPoolNdays")
    val impressInput = cmd.getOptionValue("impress")
    val playendInput = cmd.getOptionValue("playend")
    val dataDate = cmd.getOptionValue("dataDate")
    val outputPath = cmd.getOptionValue("output")

    import spark.implicits._

//    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//    val existsVideoPoolInputPaths = getExistsPath(videoPoolNdaysInput, fs)
//    logger.warn("existing videoPoolInput path:\t" + existsVideoPoolInputPaths.mkString(","))
//    val videoPoolNdaysTable = spark.read.parquet(existsVideoPoolInputPaths.toSeq : _*)
//      .groupBy($"videoId", $"vType")
//      .agg(count($"videoId").as("cnt"))
//      .select($"videoId", $"vType")

    val usefulPage = Seq("recommendvideo", "mvplay", "videoplay")
    val usefulVType = Seq("mv", "video")
    val uselessAlgSet = Seq("firstpage_force_rcmd", "hot")
    val impressTable = spark.sparkContext.textFile(impressInput)
      .map {line =>
        val info = line.split("\t")
        val page = info(1)
        val os = info(2)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val vType = info(6)
        val alg = info(8)
//        if (vType.isEmpty && !page.isEmpty)
//          vType = getVType(page)
        Impress(videoId, getVType(vType), actionUserId, page, os, alg)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"page".isin(usefulPage : _*))
      .filter($"vType".isin(usefulVType : _*))
      .filter(!$"alg".isin(uselessAlgSet : _*))
      .filter($"os"==="android")
      .withColumn("clickrateType", getClickratetypeFromPage($"page"))
      .select("videoId", "vType", "clickrateType", "actionUserId", "alg")

    val usefulSource = Seq("mvplay_recommendvideo", "videoplay_recommendvideo", "recommendvideo")
    val playTable = spark.sparkContext.textFile(playendInput)
      .map {line =>
        val info = line.split("\t")
        val os = info(2)
        val actionUserId = info(3)
        val videoId = info(5).toLong
        val vType = info(6)
        val source = info(8)
        val isNewversion = info(11)
        val time = info(15).toLong
        PlayAction(videoId, getVType(vType), actionUserId, time, source, isNewversion, os)
      }.toDF
      .filter($"actionUserId">0)
      .filter($"videoId" > 100 && $"time">0)
      .filter($"source".isin(usefulSource : _*))
      .filter($"vType".isin(usefulVType : _*))
      .filter($"isNewversion"===1)
      .filter($"os"==="android")
      .withColumn("clickrateType", getClickratetypeFromSource($"source"))
      .withColumn("isPlayed", setColumnValue(1)())
      .select("videoId", "vType", "clickrateType", "actionUserId", "isPlayed")

   val joinedTable = impressTable
      .join(playTable, Seq("videoId", "vType", "clickrateType", "actionUserId"), "left_outer")
      .na.fill(0, Seq("isPlayed"))

    val joinedImpressTable = joinedTable
      .groupBy("videoId", "vType", "clickrateType")
      .agg(countDistinct("actionUserId").as("impressCnt"))

    val joinedPlayTable = joinedTable
      .filter($"isPlayed">0)
      .groupBy("videoId", "vType", "clickrateType")
      .agg(countDistinct("actionUserId").as("actionCnt"))

    val videoClickrateTable = joinedImpressTable
      .join(joinedPlayTable, Seq("videoId", "vType", "clickrateType"), "left_outer")
      .na.fill(0, Seq("actionCnt"))
      .withColumn("clickrate1d", getClickrate($"impressCnt", $"actionCnt"))
      .withColumn("date", getDataDate(dataDate)())

    videoClickrateTable.write.parquet(outputPath)
  }

  def getClickratetypeFromPage = udf((page:String) => {
    if (page.equals("mvplay") || page.equals("videoplay"))
      "detail"
    else
      "flow"
  })

  def getClickratetypeFromSource = udf((source:String) => {
    if (source.equals("mvplay_recommendvideo") || source.equals("videoplay_recommendvideo"))
      "detail"
    else
      "flow"
  })

  def getDataDate(date:String) = udf(()=> {
    date
  })

  def getVType(vtype:String):String = {
    if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }

  def setColumnValue(value:Int) = udf(() => {
    value
  })

}
