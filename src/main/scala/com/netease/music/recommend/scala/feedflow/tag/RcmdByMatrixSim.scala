package com.netease.music.recommend.scala.feedflow.tag

import java.io.{BufferedReader, InputStreamReader}

import breeze.linalg.DenseVector
import com.netease.music.recommend.scala.feedflow.tag.SimItemByAls.{UidRcedVideos, UidRcedVideosFromEvent}
import com.netease.music.recommend.scala.feedflow.utils.ABTest
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.util.Random;

/**
  * word2vec
  */
object RcmdByMatrixSim {

  def getItemFactorM(rows: Array[Row]) = {
    val itemFactorM = mutable.HashMap[String, DenseVector[Double]]()
    for (row <- rows) {
      val idtype = row.getAs[String]("idtype")
      val features = row.getAs[mutable.WrappedArray[Double]]("features").toArray[Double]
      itemFactorM.put(idtype, DenseVector[Double](features))
    }
    itemFactorM
  }
  // 1/chanceN概率遍历itemFactorsMBroadcast中内容
  def sims1(itemFactorsMBroadcast: Broadcast[mutable.HashMap[String, DenseVector[Double]]], factors: DenseVector[Double], rceds: mutable.HashSet[String], random:Random, chanceN:Int) = {
    val scoreNameList = mutable.ArrayBuffer[Tuple2[Double, String]]()
    for ((idtype, candFacotrs) <- itemFactorsMBroadcast.value) {
      if (!rceds.contains(idtype) && random.nextInt(chanceN) == 0) {
        val score = simCosine(candFacotrs, factors)
        scoreNameList.append(Tuple2(score, idtype))
      }
    }
    //scoreNameList.sortBy[Double](t2 => t2._1, false)
    val sortedNameList = scoreNameList.sortBy[Double](-_._1)
    sortedNameList
  }
  def sims2(itemFactorsMBroadcast: Broadcast[mutable.HashMap[String, DenseVector[Double]]], factors: DenseVector[Double], rceds: mutable.HashSet[String], random: Random, chanceN:Int) = {
    val scoreNameList = mutable.ArrayBuffer[Tuple2[Double, String]]()
    for ((idtype, candFacotrs) <- itemFactorsMBroadcast.value) {
      if (!rceds.contains(idtype) && random.nextInt(chanceN) == 0) {
        val score = simDot(candFacotrs, factors)
        scoreNameList.append(Tuple2(score, idtype))
      }
    }
    //scoreNameList.sortBy[Double](t2 => t2._1, false)
    val sortedNameList = scoreNameList.sortBy[Double](-_._1)
    sortedNameList
  }

  def simCosine(candFactors: DenseVector[Double], factors: DenseVector[Double]) = {
    val xy = candFactors.dot(factors)
    val xx = Math.sqrt(candFactors.dot(candFactors))
    val yy = Math.sqrt(factors.dot(factors))
    xy / (xx * yy)
  }
  def simDot(candFactors: DenseVector[Double], factors: DenseVector[Double]) = {
    val xy = candFactors.dot(factors)
    xy
  }

  def getValidVideos(inputDir: String) = {
    val validVideos = new mutable.HashSet[String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir))) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getValidVideosFromFile(bufferedReader, validVideos)
        }
      }
    }
    validVideos
  }

  def getValidVideosFromFile(reader: BufferedReader, validVideos: mutable.HashSet[String]): Unit = {
    var line = reader.readLine()
    while (line != null) {
      //try {
      implicit val formats = DefaultFormats
      val json = parse(line)
      val videoId = (json \ "videoId").extractOrElse[String]("")
      validVideos.add(videoId + "-video")
    }
  }

  def getValidVideosList(strings: Array[String]) = {
    val validVideos = new mutable.HashSet[String]()
    for(str <- strings) {
      implicit val formats = DefaultFormats
      val json = parse(str)
      val videoId = (json \ "videoId").extractOrElse[String]("")
      validVideos.add(videoId + "-video")
    }
    validVideos
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("input_item", true, "input_item")
    options.addOption("input_user", true, "input_user")
    options.addOption("Music_VideoRcmdMeta", true, "input")

    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("user_rced_video_from_event", true, "user_rced_video_from_event")
    options.addOption("ml_resort", true, "ml_resort")

    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)
    val outputPath = cmd.getOptionValue("output")

    val testUserSet = mutable.HashSet[Long](359792224, 303658303, 135571358, 2915298, 85493186, 41660744)
    val testUserSetBroad = sc.broadcast(testUserSet)

    println("1. 读取用户已经推荐数据1...")
    val inputUserRced = cmd.getOptionValue("user_rced_video")
    val userRcedVideosData = spark.read.textFile(inputUserRced).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userIdStr", "rcedVideos")
    println("读取用户已经推荐数据2...")
    val inputUserRcedFromEvent = cmd.getOptionValue("user_rced_video_from_event")
    val userRcedVideosFromEventData = spark.read.textFile(inputUserRcedFromEvent).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userIdStr", "rcedVideosFromEvent")

    val userRecedVideosAll = userRcedVideosData.join(userRcedVideosFromEventData, Seq("userIdStr"), "outer")
      .map(row => {
        val userId = row.getAs[String]("userIdStr")
        val rcedSet = mutable.HashSet[String]()
        val rcedVideos = row.getAs[String]("rcedVideos")
        val rcedVideosFromEvent = row.getAs[String]("rcedVideosFromEvent")
        if (rcedVideos != null) {
          for (v <- rcedVideos.split(",")) {
            rcedSet.add(v)
          }
        }
        if (rcedVideosFromEvent != null) {
          for (v <- rcedVideosFromEvent.split(",")) {
            rcedSet.add(v)
          }
        }
        (userId, rcedSet.mkString(","))
      }).toDF("userIdStr", "rcedVideos")
    userRecedVideosAll.show(2, false)

    println("2. 加载用户召回数据...")
    val userResortData = spark.read.textFile(cmd.getOptionValue("ml_resort")).map(line => {
      val ts = line.split("\t", 2)
      (ts(0), ts(1).split(",").size)
    }).toDF("userIdStr", "recallNum")
    userResortData.show(2, false)

    println("3. 加载有效视频features")
    val itemFactors = spark.read.parquet(cmd.getOptionValue("input_item"))
    println("itemFactors schema:")
    itemFactors.printSchema()
    itemFactors.show(2, false)

    val validVideos = getValidVideosList(spark.read.textFile(cmd.getOptionValue("Music_VideoRcmdMeta")).collect())
    println("validVideos size:" + validVideos.size)
    //val validVideosBroad = sc.broadcast(validVideos)
    val itemFactorsIdtypeM = getItemFactorM(itemFactors.filter($"idtype".endsWith("-video")).collect())
    print ("itemFactorsIdtypeM size:" + itemFactorsIdtypeM.size)
    val itemFactorsMBroadcast = sc.broadcast[mutable.HashMap[String, DenseVector[Double]]](itemFactorsIdtypeM)


    //println("abtest...")
    //val musicABTestBroad = sc.broadcast(musicABTest)
    print("4. 根据user feature 和 video feature推荐...")
    val userFactors = spark.read.parquet(cmd.getOptionValue("input_user"))
    println("userFactors schema:")
    userFactors.printSchema()
    userFactors.show(10, false)
    val config = spark.read.textFile(cmd.getOptionValue("abtest_config")).collect()
    println("config:")
    println(config.toString)
    val musicABTest = new ABTest(config)
    val musicABTestBroad = sc.broadcast(musicABTest)

    val rcmdData = userFactors.join(userResortData, Seq("userIdStr"), "left")
        .join(userRecedVideosAll, Seq("userIdStr"), "left")
        .sample(false, 0.001)
      .map(row => {
      val uid = row.getAs[String]("userIdStr").toLong
      val recallNum = row.getAs[Int]("recallNum")
      val rcedVideos = mutable.HashSet[String]()
      val rcedVideoStr = row.getAs[String]("rcedVideos")
      if (rcedVideoStr != null) {
        for (vtype <- rcedVideoStr.split(",")) {
          rcedVideos.add(vtype + "-video")
        }
      }
      val random = new Random()
      if (recallNum < 100 || musicABTestBroad.value.getCodeGroupNames(uid).contains("videorcmd-mixals-t3") || testUserSetBroad.value.contains(uid)) {
        val uFeats = DenseVector[Double](row.getAs[mutable.WrappedArray[Double]]("features").toArray[Double])
        var simNameScoreList = sims1(itemFactorsMBroadcast, uFeats, rcedVideos, random, 10)
        var topSimNameScoreList = mutable.ArrayBuffer[String]()
        for (i <- 0 until 100) {
          topSimNameScoreList.append(simNameScoreList(i)._2 + ":" + simNameScoreList(i)._1)
        }
        // tag \t simtag:score,simtag:score...
        if (testUserSetBroad.value.contains(uid)) {
          println(uid.toString + "_cosine\t" + topSimNameScoreList.mkString(","))
          simNameScoreList = sims2(itemFactorsMBroadcast, uFeats, rcedVideos, random, 10) // dot
          topSimNameScoreList = mutable.ArrayBuffer[String]()
          for (i <- 0 until 100) {
            topSimNameScoreList.append(simNameScoreList(i)._2 + ":" + simNameScoreList(i)._1)
          }
          println(uid.toString + "_dot\t" + topSimNameScoreList.mkString(","))
        }
        uid.toString + "\t" + topSimNameScoreList.mkString(",")
      } else {
        null
      }
    }).filter(_ != null)

    rcmdData.repartition(10).write.text(outputPath)

  }
}