package com.netease.music.recommend.scala.feedflow.comment

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ColumnName, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 资源评论按照tfidf保留关键token
  */
object TfIdfIndex {

  def getDfM(tokenDfs: Array[Row]) = {
    val sortedTokenDfs = tokenDfs.sortWith(_.getAs[Long]("df") > _.getAs[Long]("df"))
    val tokenDfM = mutable.HashMap[String, Long]()
    // for (i <- sortedTokenDfs.length / 30 until sortedTokenDfs.length * 9 / 10) { // 去掉前后10%
    for (i <- 0 until sortedTokenDfs.length) { // 去掉前后10%
      val tokenDf = sortedTokenDfs.apply(i)
      val token = tokenDf.getAs[String]("token")
      if (token.length > 1) {
        val df = tokenDf.getAs[Long]("df")
        tokenDfM.put(token, df)
      }
    }
    tokenDfM
  }

  def matchLen = udf((token: String) => {
    if (token.length > 1) {
      true
    } else {
      false
    }
  })


  def getTokenIndexM(rows: Array[Row]) = {
    val tokenIndxM = mutable.HashMap[String, Integer]()
    var maxIdx = 0
    for (row <- rows) {
      val token = row.getAs[String]("token")
      val idx = row.getAs[Integer]("tokenIdx")
      tokenIndxM.put(token, idx)
      if (idx > maxIdx) {
        maxIdx = idx
      }
    }
    (tokenIndxM, maxIdx)
  }

  def svmformat = udf((resid: Integer , tokidTfidf: String) => {
    resid.toString + " " + tokidTfidf
  })

  def tokenformat = udf((resid: String , tokidTfidf: String) => {
    resid.toString + "\t" + tokidTfidf
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("input", true, "input")
    options.addOption("maxDf", true, "maxDf")
    options.addOption("minDf", true, "minDf")
    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)
    val maxDf = cmd.getOptionValue("maxDf").toInt
    val minDf = cmd.getOptionValue("minDf").toInt
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
        .withColumn("matchLen", matchLen($"token"))
      .filter($"matchLen" === true)
      .filter($"df" < maxDf)
      .filter($"df" > minDf)

    val tokenIndex = tokenDf.select($"token").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("token"), line._2.toInt + 1)
    }).toDF("token", "tokenIdx").cache()
      .filter($"tokenIdx" > 0)
    tokenIndex.write.parquet(cmd.getOptionValue("output") + "/tokenIdMapping")

    val (tokenIndxM, maxTokenIdx) = getTokenIndexM(tokenIndex.collect())
    println("maxTokenIdx vs. tokenIdx size:" + maxTokenIdx + " - " + tokenIndxM.size)
    val tokenIndxMBroad = sc.broadcast(tokenIndxM)
    println("tokenIndxM.head")
    println(tokenIndxM.head)

    val tokenDfM = getDfM(tokenDf.collect())
    println("tokenDfM head")
    println(tokenDfM.head)
    val tokenDfMBroad = sc.broadcast(tokenDfM)

    println("join token id")
    val resTokenTfidfData = spark.read.textFile(cmd.getOptionValue("input"))
      .map(line => {
        // R_MV_5_10728088	佬:1,大:1,奇:1,曲风清:1,逼:1,好听:1,了:1,牛:1,膜:1,太:1
        val ts = line.split("\t", 2)
        val tokenTfidfs = mutable.ArrayBuffer[(String, Double)]()
        if (ts.length > 1) {
          val toks = ts(1).split(",")
          if (toks.length > 10) {
            for (tokenCnt <- toks) {
              val tc = tokenCnt.split(":")
              if (tc.length > 1 && tc(0).length > 1) {
                val df = tokenDfMBroad.value.getOrElse[Long](tc(0), -1)
                if (df > 0) {
                  val tfidf = tc(1).toInt * Math.log((docNum + 1.0) / (df + 1.0))
                  tokenTfidfs.append((tc(0), tfidf))
                }
              }
            }
          }
        }
        var outKey = ""
        var outValue = ""
        var outValueTokenRaw = ""
        var outValuePickTokenRaw = ""
        if (tokenTfidfs.length > 5 && ts(0).startsWith("R")) {
          val sortedTokenTfidfs = tokenTfidfs.sortWith(_._2 > _._2)
          val pickTokenIdx = mutable.ArrayBuffer[String]()
          val pikeToken = mutable.ArrayBuffer[String]()
          val topNs = mutable.ArrayBuffer[(Integer, Integer)]()
          for (tokenTfidf <- sortedTokenTfidfs if (pickTokenIdx.size < sortedTokenTfidfs.length*2/3 && pickTokenIdx.size < 200)) {
            val tokenIdx = tokenIndxMBroad.value.apply(tokenTfidf._1)
            val tfidf = tokenTfidf._2
            topNs.append((tokenIdx, tfidf.toInt))
            pikeToken.append(tokenTfidf._1)
          }
          for ((tokenIdx, tfidf) <- topNs.sortWith(_._1 < _._1)) {  // svmforamt需升序
            pickTokenIdx.append(tokenIdx + ":" + tfidf.toInt)
          }

          outKey = ts(0)
          outValue = pickTokenIdx.mkString(" ")
          outValueTokenRaw = sortedTokenTfidfs.mkString(",")
          outValuePickTokenRaw = pikeToken.mkString(",")
        } else {
          outKey = null
        }
        (outKey, outValue, outValueTokenRaw, outValuePickTokenRaw)
      }).toDF("resource", "tokidTfidf", "tokenRaw", "pickTokenRaw")
      .filter($"resource".isNotNull)

    println("tokenTfidfData.head")
    println(resTokenTfidfData.head())

    println("resource id mapping")
    val resIdxData = resTokenTfidfData.select("resource").rdd.distinct().zipWithUniqueId()
      .map(line =>{
      (line._1.getAs[String]("resource"), line._2.toInt + 1 + maxTokenIdx) // resourceId在tokenid基础上增加
      }).toDF("resource", "resId").cache()
    resIdxData.write.parquet(cmd.getOptionValue("output") + "/resourceIdMapping")

    println("join id")
    val residTokidData = resTokenTfidfData.join(resIdxData, Seq("resource"), "left")
    residTokidData.write.parquet(cmd.getOptionValue("output") + "/rawData")

    residTokidData.filter($"resource".startsWith("R_VI_62_")).withColumn("svmformat", svmformat($"resId", $"tokidTfidf"))
      .select("svmformat")
      .repartition(1).write.text(cmd.getOptionValue("output") + "/svmformat")

    residTokidData.withColumn("tokenformat", tokenformat($"resource", $"pickTokenRaw"))
      .select("tokenformat")
      .repartition(1).write.text(cmd.getOptionValue("output") + "/tokenformat")

  }

}
