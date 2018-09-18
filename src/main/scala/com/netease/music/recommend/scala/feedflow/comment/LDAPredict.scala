package com.netease.music.recommend.scala.feedflow.comment

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.mllib.clustering.DistributedLDAModel
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 资源评论按照tfidf保留关键token
  */
object LDAPredict {


  def getIdxtokenM(rows: Array[Row]) = {
    val idxTokenM = mutable.HashMap[Long, String]()
    for (row <- rows) { //"token", "tokenIdx"
      val token = row.getAs[String]("token")
      val tokenIdx = row.getAs[Long]("tokenIdx")
      idxTokenM.put(tokenIdx, token)
    }
    idxTokenM
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("model", true, "input")
    options.addOption("token_idx", true, "token_idx")
    options.addOption("output", true, "output")
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    // val model = DistributedLDAModel.load(sc, cmd.getOptionValue("model"))

    val sameModel = DistributedLDAModel.load(sc, cmd.getOptionValue("model"))
    // "token", "tokenIdx"
    val tokenIdxMapping = spark.read.parquet(cmd.getOptionValue("token_idx"))
    val idxTokenM = getIdxtokenM(tokenIdxMapping.collect())
    println("idxTokenM size:" + idxTokenM.size)
    val idxTokenMBroad = sc.broadcast(idxTokenM)

    val topics = sameModel.describeTopics(20)

    for (topic <- topics) {
      val tokenScore = mutable.ArrayBuffer[String]()
      for (i <- 0 until topic._1.size) {
        val token = idxTokenMBroad.value.apply(topic._1(i).toLong)
        val score = topic._2(i)
        tokenScore.append(token + ":" + score)
      }
      println("topic -> " + tokenScore.mkString(","))
    }


  }

}
