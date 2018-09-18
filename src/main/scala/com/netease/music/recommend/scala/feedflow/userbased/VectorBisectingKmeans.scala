package com.netease.music.recommend.scala.feedflow.userbased

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hzzhangjunfei1 on 2018/8/8.
  */
object VectorBisectingKmeans {

  def seq2Vec = udf((features:String) => Vectors.dense(features.split(",").map(_.toDouble)))

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("vectorInput", true, "input directory")
    options.addOption("clusterNum", true, "cluster num")
    options.addOption("maxIterations", true, "maxIterations num")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val vectorInput = cmd.getOptionValue("vectorInput")
    val cluster_num = cmd.getOptionValue("clusterNum").toInt
    val maxIterations = cmd.getOptionValue("maxIterations").toInt
    val output = cmd.getOptionValue("output")

    import spark.implicits._
    val userVectorData = spark.read.option("sep", "\t").csv(vectorInput)
      .toDF("idtype", "features", "id")
      .withColumn("features", seq2Vec($"features"))

    val bisectingKmeans = new BisectingKMeans()
      .setK(cluster_num)
      .setMaxIter(maxIterations)
      .setSeed(10000L)

    val kMeansModel = bisectingKmeans.fit(userVectorData) // prediction
    kMeansModel.save(output + "/model/")
//    val kMeansSummary = kMeansModel.summary
//    kMeansSummary
//    kMeansSummary.cluster

    //val kMeansModelBroad = sc.broadcast(kMeansModel)
    val predResult = kMeansModel.transform(userVectorData)
    predResult.show(100, false)
    println("输出...")
    predResult.write.parquet(output + "/pred_result/")

  }

}
