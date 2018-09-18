package com.netease.music.recommend.scala.feedflow.tag

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hzlvqiang on 2018/1/22.
  */
object VideoVectorBisecting {

  def seq2Vec = udf((features:Seq[String]) => Vectors.dense(features.map(_.toDouble).toArray[Double]))

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("input", true, "input directory")
    options.addOption("k_num", true, "input")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._
    val input = cmd.getOptionValue("input")
    val kNum = cmd.getOptionValue("k_num").toLong
    val userVectorData = spark.read.textFile(input)
          .map(line => {
            val ts = line.split("\t", 2)
            val idtype = ts(0)
            val vectors = ts(1).split(",")
            (idtype, vectors)
          }).toDF("idtype", "features")
      .withColumn("features", seq2Vec($"features"))

    val kmeans = new BisectingKMeans().setK(kNum.toInt).setSeed(kNum)

    println("Train....")
    val kMeansModel = kmeans.fit(userVectorData)
    //val kMeansModel = KMeans.train(userVectorData, 100, 50)

    val cost = kMeansModel.computeCost(userVectorData)
    println(s"Within Set Sum of Squared Errors = $cost")
    // Shows the result.
    println("Cluster Centers: ")
    val centers = kMeansModel.clusterCenters
    centers.foreach(println)

    //val kMeansModelBroad = sc.broadcast(kMeansModel)
    val predResult = kMeansModel.transform(userVectorData)
    predResult.show(100, false)
    println("输出...")
    predResult.write.parquet(cmd.getOptionValue("output"))

  }

}
