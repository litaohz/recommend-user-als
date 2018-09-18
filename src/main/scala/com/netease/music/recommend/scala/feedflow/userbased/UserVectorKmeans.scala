package com.netease.music.recommend.scala.feedflow.userbased

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.KMeans

/**
  * Created by hzlvqiang on 2018/1/22.
  */
object UserVectorKmeans {

  def seq2Vec = udf((features:Seq[Float]) => Vectors.dense(features.map(_.toDouble).toArray[Double]))

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("input", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._
    val input = cmd.getOptionValue("input")
    val userVectorData = spark.read.parquet(input)//.toDF("id", "features", "userIdStr")
      .withColumn("features", seq2Vec($"features"))

    val kmeans = new KMeans().setK(10000).setSeed(10000L)

    println("Train....")
    val kMeansModel = kmeans.fit(userVectorData) // prediction
    //val kMeansModel = KMeans.train(userVectorData, 100, 50)

    //val kMeansModelBroad = sc.broadcast(kMeansModel)
    val predResult = kMeansModel.transform(userVectorData)
    predResult.show(100, false)
    println("输出...")
    predResult.write.parquet(cmd.getOptionValue("output"))

  }

}
