package com.netease.music.recommend.scala.feedflow.tag

import breeze.linalg.DenseVector
import org.apache.commons.cli.{Options, PosixParser}

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.SparkSession

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel


/**
  * Created by hzlvqiang on 2018/3/15.
  */
object ALSTest {



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("input_model", true, "input")
    options.addOption("userid", true, "userid")
    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)


    // val model = MatrixFactorizationModel.load(sc, "/user/ndir/music_recommend/feed_video/tag/als_mixedId/model")
    val input = cmd.getOptionValue("input_model")
    println("input:" + input)


    val model = MatrixFactorizationModel.load(sc, input)

    val uid = cmd.getOptionValue("userid").toInt
    println("uid:" + uid)
    val res = model.recommendProducts(uid, 100)
    println("rcmd for uid")
    res.foreach(println)

    println("xxxxx details")
    for (r <- res) {
      val scores = sim(model.productFeatures.lookup(r.product).head, model.userFeatures.lookup(uid).head)
      println(r + "ratinng=" + r.rating + "," + scores)
    }
  }

  def sim(productFeat: Array[Double], userFeat: Array[Double]) = {
    val pVector = DenseVector(productFeat)
    val uVector = DenseVector(userFeat)
    val s1 = pVector.dot(uVector)
    val s2 = pVector.dot(pVector)
    val s3 = uVector.dot(uVector)
    (s1, s1 / (Math.sqrt(s2)*Math.sqrt(s3)))
  }


}
