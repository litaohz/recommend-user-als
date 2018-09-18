package com.netease.music.recommend.scala.feedflow.comment

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

import scala.collection.mutable
import org.apache.spark.sql.functions._

/**
  * 资源评论按照tfidf保留关键token
  */
object TfIdf {

  def getDfM(tokenDfs: Array[Row]) = {
    val sortedTokenDfs = tokenDfs.sortWith(_.getAs[Long]("df") > _.getAs[Long]("df"))
    val tokenDfM = mutable.HashMap[String, Long]()
    // for (i <- sortedTokenDfs.length / 30 until sortedTokenDfs.length * 9 / 10) { // 去掉前后10%
    for (i <- 0 until sortedTokenDfs.length) { // 去掉前后10%
      val tokenDf = sortedTokenDfs.apply(i)
      val token = tokenDf.getAs[String]("token")
      val df = tokenDf.getAs[Long]("df")
      tokenDfM.put(token, df)
    }
    tokenDfM
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("input", true, "input")

    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val docNum = spark.read.textFile(cmd.getOptionValue("input")).count()
    println("docNum:" + docNum)

    val tokenDf = spark.read.textFile(cmd.getOptionValue("input"))
      .flatMap(line => {
        // R_MV_5_10728088	佬:1,大:1,奇:1,曲风清:1,逼:1,好听:1,了:1,牛:1,膜:1,太:1
        val ts = line.split("\t", 2)
        val tokenCntM = mutable.HashMap[String, Integer]()
        if (ts.length > 1) {
          for (tokenCnt <- ts(1).split(",")) {
            val tc = tokenCnt.split(":")
            if (tc.length > 1) {
              tokenCntM.put(tc(0), tc(1).toInt)
            }
          }
        }
        tokenCntM
      }).toDF("token", "tf")
      .groupBy($"token").agg(count($"tf").as("df"))
      .collect()

    val tokenDfM = getDfM(tokenDf) // 去了按df排序后中间80%的token
    val tokenDfMBroad = sc.broadcast(tokenDfM)

    val tokenTfidfData = spark.read.textFile(cmd.getOptionValue("input"))
      .map(line => {
        // R_MV_5_10728088	佬:1,大:1,奇:1,曲风清:1,逼:1,好听:1,了:1,牛:1,膜:1,太:1
        val ts = line.split("\t", 2)
        val tokenTfidfs = mutable.ArrayBuffer[(String, Double)]()
        if (ts.length > 1) {
          for (tokenCnt <- ts(1).split(",")) {
            val tc = tokenCnt.split(":")
            if (tc.length > 1) {
              val df = tokenDfMBroad.value.getOrElse[Long](tc(0), -1)
              if (df > 0) {
                val tfidf = tc(1).toInt * Math.log((docNum + 1.0) / (df + 1.0))
                tokenTfidfs.append((tc(0), tfidf))
              }
            }
          }
        }
        val sortedTOkenTfidfs = tokenTfidfs.sortWith(_._2 > _._2)
        (ts(0), sortedTOkenTfidfs)
      }).toDF("resId", "tokenTfidf")

    tokenTfidfData.write.parquet(cmd.getOptionValue("output"))

  }

}
