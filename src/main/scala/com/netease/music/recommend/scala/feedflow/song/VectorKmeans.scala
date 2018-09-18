package com.netease.music.recommend.scala.feedflow.song

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import scala.collection.mutable

/**
  * Created by hzlvqiang on 2018/1/22.
  */
object VectorKmeans {


  def denVec = udf((features:mutable.WrappedArray[Double]) => {
    val doubleArray = mutable.ArrayBuffer[Double]()
    for (f <- features) {
      doubleArray.append(f.toDouble)
    }
    Vectors.dense(doubleArray.toArray[Double])
  })
  def normVector = udf((features:mutable.WrappedArray[Double]) => {
    val doubleArray = mutable.ArrayBuffer[Double]()
    var sumsqrt = 0.0
    for (f <- features) {
      sumsqrt += Math.pow(f, 2)
    }
    sumsqrt = Math.sqrt(sumsqrt)
    for (f <- features) {
      doubleArray.append(f / sumsqrt)
    }

    Vectors.dense(doubleArray.toArray[Double])

  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("item_features", true, "item_features")
    options.addOption("cluster_num", true, "cluster_num")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._
    // itemFeature
    val itemFeatures = spark.read.textFile(cmd.getOptionValue("item_features")) // id float1,float2
      .map(line => {
        val ts = line.split("\t")
        val item = ts(0)
        val vecs = mutable.ArrayBuffer[Double]()
        for (v <- ts(1).split(",")) {
          vecs.append(v.toDouble)
        }
        (item, vecs)
      }).toDF("idtype", "featuresVec")

    val itemIdFeatures = itemFeatures.select("idtype").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("idtype"), line._2)
    }).toDF("idtype", "id")

    val itemFeaturesNorm = itemFeatures.withColumn("features", normVector($"featuresVec"))
      .join(itemIdFeatures, Seq("idtype"), "left")
      .select("id", "idtype", "features")

    println("itemFeaturesNorm size:" + itemFeaturesNorm.count())
    itemFeaturesNorm.show(2, false)

    val kmeans = new KMeans().setK(10000).setSeed(100).setMaxIter(20) // 当前有效视频数
    val kMeansModel = kmeans.fit(itemFeaturesNorm)
    val predResult = kMeansModel.transform(itemFeaturesNorm) // prediction
    println("predResult schema...")
    predResult.printSchema()
    predResult.show(10, false)
    predResult.write.parquet(cmd.getOptionValue("output"))

  }

}
