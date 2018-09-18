package com.netease.music.recommend.scala.feedflow.userbased

import breeze.linalg.DenseVector
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hzzhangjunfei1 on 2018/8/8.
  */
object VectorLSH {

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
//    options.addOption("clusterNum", true, "cluster num")
//    options.addOption("maxIterations", true, "maxIterations num")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val vectorInput = cmd.getOptionValue("vectorInput")
//    val cluster_num = cmd.getOptionValue("clusterNum").toInt
//    val maxIterations = cmd.getOptionValue("maxIterations").toInt
    val output = cmd.getOptionValue("output")

    import spark.implicits._
    val userVectorData = spark.read.option("sep", "\t").csv(vectorInput)
      .toDF("idStr", "features", "id")
      .withColumn("features", seq2Vec($"features"))

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(5000)
      .setNumHashTables(2000)
      .setInputCol("features")
      .setOutputCol("hashes")

    val brpModel = brp.fit(userVectorData)

    val resultDf = brpModel.approxSimilarityJoin(userVectorData, userVectorData, 1.5, "EuclideanDistance")
    resultDf.write.parquet(output)
  }

  def cosineSim(x: DenseVector[Double], y: DenseVector[Double]) = {
    val xy = x.dot(y)
    val xx = Math.sqrt(x.dot(x))
    val yy = Math.sqrt(y.dot(y))
    xy / (xx * yy)
  }

  def adjust_clusterId = udf((clusterId: Int, clusterSize: Long) =>{
    if (clusterSize < 50)
      -1
    else
      clusterId
  })
}
