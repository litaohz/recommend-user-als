package com.netease.music.recommend.scala.feedflow.tag

import com.netease.music.recommend.scala.feedflow.tag.ALSTest.sim
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable;

/**
  * word2vec
  */
object TagALSMixedUnionML {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("input", true, "input")
    options.addOption("vid_uid_pos", true, "vid_uid_pos")
    options.addOption("output", true, "output")
    // options.addOption("modelOutput", true, "output directory")
    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val inputPath = cmd.getOptionValue("input")
    val outputPath = cmd.getOptionValue("output")
    println("int maxValue: " + Integer.MAX_VALUE)

    // 混合uid-itemidx输出数据
    val userItemRatingData = spark.sparkContext.textFile(inputPath).map(line => {
      // uid idtype detail wt
      val ts = line.split("\t")
      var uL = ts(0).toLong // TODO
      if (uL < 0) {
        uL = -uL
      }
      (uL.toString, ts(1), ts(3).toDouble)
    }).toDF("userIdStr", "idtype", "rating")
    println("userItemRatingData cnt:" + userItemRatingData.count())

    // 视频用户正向数据
    /*
    val userPosVideosPrefData = spark.read.textFile(cmd.getOptionValue("vid_uid_pos")).map(line => {
      val ts = line.split("\t")
      val idtype = ts(0) + "-video"
      val uid = ts(1)
      (uid, idtype, 10.0)
    }).toDF("userIdStr", "idtype", "rating")
    println("userPosVideosPrefData cnt:" + userPosVideosPrefData.count)

    // 取音乐偏好正向用户  + 正向用户本身并集
    val userItemRatingActiveDataUnion = userItemRatingData.union(userPosVideosPrefData)
    println("userItemRatingActiveDataUnion cnt:" + userItemRatingActiveDataUnion.count())
    */
    val userItemRatingActiveDataUnion = userItemRatingData

    val userIdMapping = userItemRatingActiveDataUnion.select($"userIdStr").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("userIdStr"), line._2)
    }).toDF("userIdStr", "uid").cache()

    val itemMapping = userItemRatingActiveDataUnion.select($"idtype").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("idtype"), line._2)
    }).toDF("idtype", "itemId").cache()

    val userItemRating = userItemRatingActiveDataUnion
      .join(userIdMapping, Seq("userIdStr"), "left")
      .join(itemMapping, Seq("idtype"), "left")
      .map(row => {
        val userIdInt = row.getAs[Long]("uid").toInt
        val itemId = row.getAs[Long]("itemId").toInt
        (userIdInt, itemId, row.getAs[Double]("rating"))
    }).toDF("userId", "itemId", "rating").cache()

    println("Sample data")

    // userItemRating.show(10, true)
    val Array(training, test) = userItemRating.randomSplit(Array(0.95, 0.05))
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

    val model = als.fit(training)

    als.save(outputPath + "/model")
    // model.itemFactors.write.parquet(outputPath + "/itemFactors")

    // model.userFactors.join(userIdMapping, Seq("id"), "left").write.parquet(outputPath + "/userFactors")

    println("itemFactor sample")
    model.itemFactors.show(10, false)
    val pFeatsC = model.itemFactors.join(itemMapping.select($"idtype", $"itemId".as("id")), Seq("id"), "left")
    pFeatsC.write.parquet(outputPath + "/itemFactors")
    println("userFactor sample")
    model.userFactors.show(10, false)
    val uFeatsC = model.userFactors.join(userIdMapping.select($"userIdStr", $"uid".as("id")), Seq("id"), "left")
    uFeatsC.write.parquet(outputPath + "/userFactors")

    pFeatsC.map(row => {row.getAs[String]("idtype") + "\t" + row.getAs[mutable.WrappedArray[Double]]("features").mkString(",")}).write.text(outputPath + "/itemFactors.txt")
    uFeatsC.map(row => {row.getAs[String]("userIdStr") + "\t" + row.getAs[mutable.WrappedArray[Double]]("features").mkString(",")}).write.text(outputPath + "/userFactors.txt")


    // 评估
    val predictions = model.transform(test)
    println("sample...")
    predictions.show(10, false)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    //itemFactors.write.text(outputPath + "/itemFactors_txt")
    //model.itemFactors.write.text(outputPath + "/userFactors_txt")

  }
}