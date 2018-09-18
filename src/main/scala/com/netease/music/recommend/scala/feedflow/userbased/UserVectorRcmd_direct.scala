package com.netease.music.recommend.scala.feedflow.userbased

import breeze.linalg.DenseVector
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import com.netease.music.recommend.scala.feedflow.userbased.VectorKmeans.cosineSim

import scala.collection.mutable

/**
  * userbased + 热门降权
  * Created by hzlvqiang on 2018/1/22.
  */
object UserVectorRcmd_direct {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("user_vector", true, "user_vector directory")
    options.addOption("item_vector", true, "item_vector directory")
    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("videoPool", true, "videoPool input")
    options.addOption("mvPool", true, "mvPool input")
    options.addOption("abtestConfig", true, "abtestConfig file")
    options.addOption("abtestExpName", true, "abtest Exp Name")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    val user_vector = cmd.getOptionValue("user_vector")
    val item_vector = cmd.getOptionValue("item_vector")
    val user_rced_video = cmd.getOptionValue("user_rced_video")
    val videoPool = cmd.getOptionValue("videoPool")
    val mvPool = cmd.getOptionValue("mvPool")
    val output = cmd.getOptionValue("output")

    val configText = spark.read.textFile(cmd.getOptionValue("abtestConfig")).collect()
    val abtestExpName = cmd.getOptionValue("abtestExpName")
    for (cf <- configText) {
      if (cf.contains(abtestExpName)) {
        println(cf)
      }
    }

    println("加载有效视频/Mv数据")
    val validVidDf = spark.read.parquet(videoPool)
      .select(concat_ws("-", $"videoId", $"vType").as("idtype"))
    val validMvidDf = spark.read.parquet(mvPool)
      .select(concat_ws("-", $"mvId", $"vType").as("idtype"))
    val validIdtypeDf = validVidDf.union(validMvidDf)
    val itemVectorM = spark.read.option("sep", "\t").csv(item_vector)
      .toDF("idtype", "itemVec", "id")
      .join(validIdtypeDf, Seq("idtype"), "inner")
      .withColumn("itemVec", seq2Vec($"itemVec"))
      .rdd
      .collect
      .map(row => (row.getAs[String]("idtype"), DenseVector(row.getAs[org.apache.spark.ml.linalg.DenseVector]("itemVec").values)))
      .toMap
    val itemVectorM_broad = spark.sparkContext.broadcast(itemVectorM)


    println("加载用户已经推荐数据1...")
    val userRcedVideosDf = spark.read.textFile(user_rced_video).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userId", "rcedVideos")

    val userCnt = userRcedVideosDf.count()
    println("userRcedVideosData cnt:" + userCnt)


    val finalRcmdRdd = spark.read.option("sep", "\t").csv(user_vector)
      .toDF("userId", "userVec", "id")
      .filter(abtestGroupName_udf(configText, abtestExpName)($"userId") === "t3" || $"userId".isin(testUserSet.toSeq : _*)) // abtest
      .withColumn("userVec", seq2Vec($"userVec"))
      .join(userRcedVideosDf, Seq("userId"), "left_outer")
      .rdd
      .map{row =>
        val userId = row.getAs[String]("userId")
        val userVec = DenseVector(row.getAs[org.apache.spark.ml.linalg.DenseVector]("userVec").values)

        val rcedVideos = mutable.HashSet[String]()
        val rcedVstr = row.getAs[String]("rcedVideos")
        if (rcedVstr != null) {
          for (rvid <- rcedVstr.split(",")) {
            rcedVideos.add(rvid + "-video")
          }
        }

        val recallS = mutable.HashSet[(String, Double)]()
        itemVectorM_broad.value.foreach{tup =>
          if (!rcedVideos.contains(tup._1))
            recallS.add((tup._1, cosineSim(userVec, tup._2)))
        }
        userId + "\t" + recallS.toArray.sortWith(_._2 > _._2).slice(0, 30).map(tup => tup._1.replaceAll("-", ":")).mkString(",")
      }
    finalRcmdRdd
      .repartition(32) // 合并小文件
      .saveAsTextFile(output)

  }


  def seq2Vec = udf((features:String) => Vectors.dense(features.split(",").map(_.toDouble)))
}
