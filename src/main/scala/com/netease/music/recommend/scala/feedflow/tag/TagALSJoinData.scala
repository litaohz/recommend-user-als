package com.netease.music.recommend.scala.feedflow.tag

import breeze.linalg.norm
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.{ColumnName, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import scala.collection.mutable;

/**
  * word2vec
  */
object TagALSJoinData {

  def getSuffix = udf((idtype: String) => {
    val it = idtype.split("-")
    if (it.length >= 2) {
      it(1)
    } else {
      ""
    }
  })

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("input", true, "input")
    //options.addOption("input_userIdMapping", true, "input")
    //options.addOption("input_itemMapping", true, "input")
    options.addOption("itemCntX", true, "input")
    //options.addOption("target_suffix", true, "suffix")
    options.addOption("output", true, "output")
    // options.addOption("modelOutput", true, "output directory")
    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val inputPath = cmd.getOptionValue("input")
    val outputPath = cmd.getOptionValue("output")
    println("int maxValue: " + Integer.MAX_VALUE)
    /*
    val userItemRatingData = spark.sparkContext.textFile(inputPath).flatMap(line => {
      val ts = line.split("\t", 2)
      var uL = ts(0).toLong // TODO
      if (uL < 0) {
        uL = -uL
      }
      val uid = uL.toString
      val result = mutable.ArrayBuffer[Tuple3[String, String, Double]]()
      for (item <- ts(1).split(",")) {
        result.append((uid, item, 5.0))
        if (uL == 359792224) {
          println("item for 359792224:" + item)
        }
      }
      result
    }).toDF("userIdStr", "idtype", "rating")
    */
    //val targetSuffix = cmd.getOptionValue("target_suffix").split(",").toSeq
    //println("targetSuffix:" + targetSuffix)
    val idtypeCntData = spark.read.parquet(cmd.getOptionValue("itemCntX"))
    val userItemRatingData = spark.read.parquet(inputPath)
      .join(idtypeCntData, Seq("idtype"), "left")
      .filter($"userCnt" > 100)
      .withColumn("suffix", getSuffix($"idtype"))
      //.filter($"suffix".isin(targetSuffix: _*))
      // .filter($"userCnt" > 100 && !$"idtype".endsWith("video") || $"idtype".endsWith("video") || $"idtype".endsWith("mv") || $"idtype".endsWith("art"))
      .withColumnRenamed("rating", "rawRating")
      .withColumn("rating", normRating($"rawRating")) // 转为1
      .select("userIdStr", "userId", "itemId", "rating", "idtype")

    println("sample...")
    userItemRatingData.show(100, false)

    val Array(training, test) = userItemRatingData.randomSplit(Array(0.95, 0.05))

    val als = new ALS()
      .setMaxIter(30) // .setMaxIter(5)
      .setNumUserBlocks(1000)
      .setNumItemBlocks(1000)
      .setRegParam(0.01)
      .setImplicitPrefs(true)
      .setRank(100)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")

    sc.setCheckpointDir(outputPath + "checkpoint")
    als.setCheckpointInterval(5)

    val model = als.fit(training)

    als.save(outputPath + "/model")

    // val itemMapping = spark.read.parquet(cmd.getOptionValue("input_itemMapping"))
    // val userIdMapping = spark.read.parquet(cmd.getOptionValue("input_userIdMapping"))

    println("itemFactor sample")
    val productFeatures = model.itemFactors.toDF("id", "features")
    println(productFeatures.show(10, false))
    val pFeatsC = productFeatures.join(userItemRatingData.select($"idtype", $"itemId".as("id")), Seq("id"), "left")
    pFeatsC.write.parquet(outputPath + "/itemFactors")
    println("userFactor sample")
    val userFeatures = model.userFactors.toDF("id", "features")
    println(userFeatures.show(10, false))
    val uFeatsC = userFeatures.join(userItemRatingData.select($"userIdStr", $"userId".as("id")), Seq("id"), "left")
    uFeatsC.write.parquet(outputPath + "/userFactors")

    // pFeatsC.map(row => {row.getAs[String]("idtype") + "\t" + row.getAs[mutable.WrappedArray[Double]]("features").mkString(",")}).write.text(outputPath + "/itemFactors.txt")
    // uFeatsC.map(row => {row.getAs[String]("userIdStr") + "\t" + row.getAs[mutable.WrappedArray[Double]]("features").mkString(",")}).write.text(outputPath + "/userFactors.txt")

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
    // 30 Mean Squared Error = 0.8266650641902483
    println(s"Mean Squared Error = $MSE")

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
  }

  import org.apache.spark.sql.functions._
  def normRating = udf((rating:Double) => {
    1.0
  })
}