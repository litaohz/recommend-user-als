package com.netease.music.recommend.scala.feedflow.cf

import java.text.SimpleDateFormat
import java.util.Date

import com.netease.music.recommend.scala.feedflow.userProfile.GetUserVideoPref_days.getExistsPathUnderDirectory
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object GetUserAllPref4CF {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("userPref", true, "log input directory")
    options.addOption("days", true, "days")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userPrefDirectory = cmd.getOptionValue("userPref")
    val days = cmd.getOptionValue("days").toInt
    val outputPath = cmd.getOptionValue("output")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val existsUserPrefInputPaths = getExistsPathUnderDirectory(userPrefDirectory, fs)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val timeInMillis = System.currentTimeMillis
    val millisInDay = 24 * 3600 * 1000l
    val qualifiedLogDateSeq = for (i <- (1 to days)) yield {
      sdf.format(new Date(timeInMillis - i * millisInDay))
    }
    logger.info("valid date:" + qualifiedLogDateSeq.mkString(","))
    val existsQualifiedPrefPaths = existsUserPrefInputPaths.filter{path =>
      var remain = false
      qualifiedLogDateSeq.foreach{qualifiedDate =>
        if (path.contains(qualifiedDate))
          remain = true
      }
      remain
    }
    logger.warn("existing userPrefInput path:\t" + existsUserPrefInputPaths.mkString(","))


    import spark.implicits._
    val userPrefDaysTable = spark.read.parquet(existsQualifiedPrefPaths.toSeq : _*)
      .filter(setContain("recommendpersonal")($"impressPageSet") && setSize($"targetinfoSet")>0)
      .withColumn("isDislike", setContainContain("dislike")($"targetinfoSet"))
      .select(
        $"actionUserId".as("userId"),
        concat_ws("-", $"videoId", $"vType").as("itemId"),
        lit(5).as("pref"),
        $"isDislike"
      )
      .cache

    userPrefDaysTable
      .repartition(16)
      .write.option("sep", "\t").csv(outputPath + "/originalId")

    val windowSpec = Window.partitionBy("groupColumn").orderBy("groupColumn")
    val index = userPrefDaysTable
      .groupBy("itemId")
      .agg(count("itemId").as("cnt"))
      .withColumn("groupColumn", lit(1))
      .withColumn("mixedId", row_number() over(windowSpec))
      .select("itemId", "mixedId")
      .cache
    index.write.option("sep", "\t").csv(outputPath + "/index")

    val userPrefDaysTableWithIndex = userPrefDaysTable
      .join(index, Seq("itemId"), "left_outer")

    userPrefDaysTableWithIndex
      .filter($"isDislike")
      .select("userId", "mixedId", "pref")
      .repartition(16)
      .write.option("sep", "\t").csv(outputPath + "/dislike_mixedId")

    userPrefDaysTableWithIndex
      .filter(!$"isDislike")
      .select("userId", "mixedId", "pref")
      .repartition(16)
      .write.option("sep", "\t").csv(outputPath + "/pref_mixedId")

  }

  def getLogDate(timeInMilis:Long):String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(new Date(timeInMilis))
  }

  def getVideoPref = udf((viewTime:Double, zanCnt:Long, commentCnt:Long, subscribeCnt:Long, shareClickCnt:Long, totalActionCnt:Long, totalInverseActionCnt:Long, playTypes:Seq[String], impressCnt:Long) => {
    //    val viewPref = {
    //      if (viewTime <= 180 && playTypes != null && playTypes.contains("playend"))
    //        16
    //      else
    //        Math.sqrt(viewTime)
    //    }
    if (totalInverseActionCnt > 0)
      -5.0
    else {
      val viewPref = Math.sqrt(viewTime)
      val commentPref = {
        if (commentCnt > 0)
          viewPref * 0.5
        else
          0.0
      }
      viewPref + subscribeCnt * 16 + commentPref + zanCnt * 2 + shareClickCnt * 3 + totalActionCnt * 1 + impressCnt * 0
    }
  })

  def getNegativeFeedback = udf((negativeActionTypes:Seq[String]) => {
    var negativeScore = 0.0f
    negativeActionTypes.foreach{negativeActionType =>
      if (negativeActionType.equals(""))
        negativeScore += 0.0f
      else if (negativeActionType.equals(""))
        negativeScore += 0.0f
      else if (negativeActionType.equals(""))
        negativeScore += 0.0f
      else if (negativeActionType.equals(""))
        negativeScore += 0.0f
    }
  })

  def getVType(vtype:String):String = {
    if (vtype.toLowerCase.contains("video"))
      "video"
    else
      vtype
  }

  def getIdsFromSeq = udf((seq : Seq[String]) => {
    val set = collection.mutable.Set[String]()
    seq.foreach(line =>{
      if (line.contains("_tab_")) {
        for (idStr <- line.split("_tab_")) {
          if (idStr.toLong > 0)
            set.add(idStr)
        }
      } else {
        if (line.toLong > 0)
          set.add(line)
      }
    })
    if (set.size > 0)
      set.mkString("_tab_")
    else
      "0"
  })

  def getLongFromSeq = udf((seq : Seq[Long]) => {
    val map = collection.mutable.Map[Long, Int]()
    seq.foreach(line =>{
      val cnt = map.getOrElse(line, 0)
      map.put(line, cnt)
    })

    var result = 0l
    var maxCnt = 0
    map.map{line =>
      if (line._2 > maxCnt) {
        result = line._1
        maxCnt = line._2
      }
    }
    result
  })

  def getIntFromSeq = udf((seq : Seq[Int]) => {
    val map = collection.mutable.Map[Int, Int]()
    seq.foreach(line =>{
      val cnt = map.getOrElse(line, 0)
      map.put(line, cnt)
    })

    var result = 0
    var maxCnt = 0
    map.map{line =>
      if (line._2 > maxCnt) {
        result = line._1
        maxCnt = line._2
      }
    }
    result
  })

  def getStringFromSeq = udf((seq : Seq[String]) => {
    val map = collection.mutable.Map[String, Int]()
    seq.foreach(line =>{
      val cnt = map.getOrElse(line, 0)
      map.put(line, cnt)
    })

    var result = "null"
    var maxCnt = 0
    map.map{line =>
      if (line._2 > maxCnt) {
        result = line._1
        maxCnt = line._2
      }
    }
    result
  })
}
