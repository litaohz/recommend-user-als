package com.netease.music.recommend.scala.feedflow.userbased

import breeze.linalg.DenseVector
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.clustering.KMeans

import scala.collection.mutable

/**
  * Created by hzzhangjunfei1 on 2018/8/8.
  */
object VectorKmeans {

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
      .toDF("idStr", "features", "id")
      .withColumn("features", seq2Vec($"features"))

    val kmeans = new KMeans()
      .setK(cluster_num)
      .setMaxIter(maxIterations)
      .setSeed(10000L)

    val kMeansModel = kmeans.fit(userVectorData) // prediction
    kMeansModel.save(output + "/model/")

    val predResult = kMeansModel.transform(userVectorData)
    println("cost info:" + kMeansModel.computeCost(userVectorData))

    val clusterCentersDF = kMeansModel.clusterCenters.zipWithIndex.toSeq.toDF("vector", "clusterId")
    val clusterSizeDF = kMeansModel.summary.clusterSizes.zipWithIndex.toSeq.toDF("clusterSize", "clusterId")
    val clusterDF = clusterCentersDF.join(clusterSizeDF, Seq("clusterId"), "outer")
    //clusterDF.write.parquet(output + "/cluster/")

    val finalDf = predResult
      .withColumnRenamed("prediction", "clusterId")
      .join(clusterDF, Seq("clusterId"))
      .withColumn("cid_final", adjust_clusterId($"clusterId", $"clusterSize"))
    finalDf
      .write.parquet(output + "/pred_result/")

//    finalDf
//      .rdd.map{row =>
//      (row.getAs[Int]("cid_final"),
//        (row.getAs[Int]("clusterId"),
//          row.getAs[Long]("clusterSize"),
//          row.getAs[String]("id"),
//          DenseVector[Double](row.getAs[org.apache.spark.ml.linalg.DenseVector]("features")),
//          row.getAs[Int]("clusterId")
//        )
//      )
//    }
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
