package com.netease.music.recommend.scala.feedflow.tag

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable;

/**
  * word2vec
  */
object TagALSVideo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("input", true, "input")
    options.addOption("output", true, "output")
    options.addOption("videoTags", true, "videoTags")
    // options.addOption("modelOutput", true, "output directory")
    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val inputPath = cmd.getOptionValue("input")
    val outputPath = cmd.getOptionValue("output")
    println("int maxValue: " + Integer.MAX_VALUE)
    val userItemRatingData = spark.sparkContext.textFile(inputPath).map(line => {
      val ts = line.split("\t", 3)
      var uL = ts(0).toLong // TODO
      if (uL < 0) {
        uL = -uL
      }
      (uL.toString, ts(1), ts(2).toDouble)
    }).toDF("userIdStr", "itemIdType", "rating")

    val userIdMapping = userItemRatingData.select($"userIdStr").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("userIdStr"), line._2)
    }).toDF("userIdStr", "id")

    val videoIdxM = mutable.HashMap[String, Int]()
    var cnt = 0
    System.out.println("SAMPLE")
    for (idtype <- userItemRatingData.select($"itemIdType").distinct().collect()) {
      videoIdxM.put(idtype.getAs[String]("itemIdType"), videoIdxM.size.toInt)
      cnt += 1
      if (cnt < 10) {
        println(idtype.getAs[String]("itemIdType") + "->" + videoIdxM.size.toInt)
      }
    }
    println(videoIdxM.head)
    val videoIdxMBroad = sc.broadcast(videoIdxM)

    val userItemRating = userItemRatingData
      .join(userIdMapping, Seq("userIdStr"), "left")
      .map(row => {
        val userIdInt = row.getAs[Long]("id").toInt
        (userIdInt, videoIdxMBroad.value(row.getAs[String]("itemIdType")), row.getAs[Double]("rating"))
    }).toDF("userId", "itemId", "rating").cache()

    println("Sample data")
    // userItemRating.show(10, true)
    userItemRating.show(10, false)

    val als = new ALS()
      .setMaxIter(5)
      .setNumUserBlocks(200)
      .setNumItemBlocks(50)
      .setRegParam(0.01)
      .setImplicitPrefs(true)
      .setRank(128)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")
      val model = als.fit(userItemRating)

    als.save(outputPath + "/model")

    val itemNameIdxMapping = videoIdxM.toSeq.toDF("idtype", "itemIdx")
    val itemFactors = model.itemFactors.join(itemNameIdxMapping, $"id"===$"itemIdx", "left")
    itemFactors.write.parquet(outputPath + "/itemFactors")

    model.userFactors.join(userIdMapping, Seq("id"), "left").write.parquet(outputPath + "/userFactors")




    //itemFactors.write.text(outputPath + "/itemFactors_txt")
    //model.itemFactors.write.text(outputPath + "/userFactors_txt")

  }
}