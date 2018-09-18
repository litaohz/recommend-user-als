package com.netease.music.recommend.scala.feedflow.tag

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable;

/**
  * word2vec
  */
object TagALSMixed {

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
    }).toDF("userIdStr", "itemId", "rating")

    val userIdMapping = userItemRatingData.select($"userIdStr").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("userIdStr"), line._2)
    }).toDF("userIdStr", "id").cache

    val userItemRating = userItemRatingData
      .join(userIdMapping, Seq("userIdStr"), "left")
      .map(row => {
        val userIdInt = row.getAs[Long]("id").toInt
        (userIdInt, row.getAs[String]("itemId").toInt, row.getAs[Double]("rating"))
    }).toDF("userId", "itemId", "rating").cache()

    println("Sample data")
    // userItemRating.show(10, true)
    userItemRating.show(10, false)

    val als = new ALS()
      .setMaxIter(5)
      .setNumUserBlocks(500)
      .setNumItemBlocks(500)
      .setRegParam(0.01)
      .setImplicitPrefs(true)
      .setRank(128)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")
      val model = als.fit(userItemRating)

    als.save(outputPath + "/model")

    model.itemFactors.write.parquet(outputPath + "/itemFactors")

    model.userFactors.join(userIdMapping, Seq("id"), "left").write.parquet(outputPath + "/userFactors")


    //itemFactors.write.text(outputPath + "/itemFactors_txt")
    //model.itemFactors.write.text(outputPath + "/userFactors_txt")

  }
}