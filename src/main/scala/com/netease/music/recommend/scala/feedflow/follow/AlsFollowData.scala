package com.netease.music.recommend.scala.feedflow.follow

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{collect_list, _}

import scala.collection.mutable

import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

/**
  * Created by hzlvqiang on 2018/4/4.
  */
object AlsFollowData {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("input", true, "input")
    options.addOption("input_userIdMapping", true, "input")
    options.addOption("input_itemMapping", true, "input")
    options.addOption("output", true, "output")

    val parser = new PosixParser()
    val cmd = parser.parse(options, args)
    import spark.implicits._

    val userItemRatingData = spark.read.parquet(cmd.getOptionValue("input"))

    val Array(training, test) = userItemRatingData.randomSplit(Array(0.95, 0.05))
    println("training...")

    val als = new ALS()
      .setMaxIter(30)
      .setNumUserBlocks(1000)
      .setNumItemBlocks(500)
      .setRegParam(0.01)
      .setImplicitPrefs(true)
      .setRank(100)
      .setUserCol("userId")
      .setItemCol("friendId")
      .setRatingCol("rating")
    // 防止stachoverflow
    sc.setCheckpointDir(cmd.getOptionValue("output") + "checkpoint")
    als.setCheckpointInterval(5)
    val model = als.fit(training)
    als.save(cmd.getOptionValue("output") + "/model")

    val itemMapping = spark.read.parquet(cmd.getOptionValue("input_itemMapping"))
    val userIdMapping = spark.read.parquet(cmd.getOptionValue("input_userIdMapping"))
    val outputPath = cmd.getOptionValue("output")

    println("itemFactor sample")
    val productFeatures = model.itemFactors.toDF("id", "features")
    println(productFeatures.show(10, false))
    val pFeatsC = productFeatures.join(itemMapping.select($"friendIdStr", $"friendId".as("id")), Seq("id"), "left")
    pFeatsC.write.parquet(outputPath + "/itemFactors")
    println("userFactor sample")
    val userFeatures = model.userFactors.toDF("id", "features")
    println(userFeatures.show(10, false))
    val uFeatsC = userFeatures.join(userIdMapping.select($"userIdStr", $"userId".as("id")), Seq("id"), "left")
    uFeatsC.write.parquet(outputPath + "/userFactors")

    // evaluation
    val predictions = model.transform(test)
    println("sample...")
    predictions.show(10, false)

    val MSE = predictions.map({ row =>
      val rating = row.getAs[Double]("rating")
      val prediction = row.getAs[Float]("prediction")
      val err = (rating - prediction)
      err * err
    }).filter(!_.isNaN).rdd.mean()
    println(s"Mean Squared Error = $MSE")

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")


  }
}
