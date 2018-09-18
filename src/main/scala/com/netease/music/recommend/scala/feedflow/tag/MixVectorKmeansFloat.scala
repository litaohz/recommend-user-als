package com.netease.music.recommend.scala.feedflow.tag

import com.netease.music.recommend.scala.feedflow.tag.MixedSimVideoUnionML.getValidVideosList
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by hzlvqiang on 2018/1/22.
  */
object MixVectorKmeansFloat {


  def denVec = udf((features:mutable.WrappedArray[Float]) => {
    val doubleArray = mutable.ArrayBuffer[Double]()
    for (f <- features) {
      doubleArray.append(f.toDouble)
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
    options.addOption("Music_VideoRcmdMeta", true, "Music_VideoRcmdMeta")
    // options.addOption("user_faatures", true, "user_features")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)


    val validVideos = getValidVideosList(spark.read.textFile(cmd.getOptionValue("Music_VideoRcmdMeta")).collect())
    println("validVideos size:" + validVideos.size)
    val validVideosBroad = sc.broadcast(validVideos)
    print ("itemFactorsIdtypeM size:" + validVideosBroad.value.size)


    import spark.implicits._
    // itemFeature
    val itemFeatures = spark.read.parquet(cmd.getOptionValue("item_features")) // id, idtype, features
        .map(row => {
          val idtype = row.getAs[String]("idtype")
          val id = row.getAs[Int]("id")
          val features = row.getAs[mutable.WrappedArray[Float]]("features")
          var valid = false
          if (validVideosBroad.value.contains(idtype)) {
            valid = true
          }
          (id, idtype, features, valid)
        }).toDF("id", "idtype", "features", "valid")
        .filter($"valid" === true)
        .withColumn("featuresVec", denVec($"features"))
        .select("id", "idtype", "featuresVec")
    // 归一化
    val normalizer = new Normalizer()
      .setInputCol("featuresVec")
      .setOutputCol("features")
      .setP(1.0)

    val itemFeaturesNorm = normalizer.transform(itemFeatures)
    val kmeans = new KMeans().setK(800).setSeed(1231) // 800 * 800 = 当前有效视频数
    val kMeansModel = kmeans.fit(itemFeaturesNorm)
    val predResult = kMeansModel.transform(itemFeaturesNorm) // prediction
    println("predResult schema...")
    predResult.printSchema() //
    predResult.show(10, false)
    predResult.write.parquet(cmd.getOptionValue("output"))

  }

}
