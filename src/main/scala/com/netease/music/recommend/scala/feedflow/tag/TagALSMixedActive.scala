package com.netease.music.recommend.scala.feedflow.tag

import com.netease.music.recommend.scala.feedflow.tag.ALSTest.sim
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable;

/**
  * word2vec
  */
object TagALSMixedActive {

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
    val userPosVideosPrefData = spark.read.textFile(cmd.getOptionValue("vid_uid_pos")).map(line => {
      val ts = line.split("\t")
      val idtype = ts(0) + "-video"
      val uid = ts(1)
      (uid, idtype, 10.0)
    }).toDF("userIdStr", "idtype", "rating")
    println("userPosVideosPrefData cnt:" + userPosVideosPrefData.count)
    val userPosVideosPrefDataDisinct = userPosVideosPrefData.groupBy($"userIdStr")
      .agg(count("idtype").as("cnt"))

    // 音乐偏好 join 正向用户
    val userItemRatingActiveData = userItemRatingData.join(userPosVideosPrefDataDisinct, Seq("userIdStr"), "inner").select("userIdStr", "idtype", "rating")
    println("userItemRatingActiveData cnt:" + userItemRatingActiveData.count())
    userItemRatingActiveData.show(10, false)
    println("userItemRatingActiveData cn")

    // 取音乐偏好正向用户  + 正向用户本身并集
    val userItemRatingActiveDataUnion = userItemRatingActiveData.union(userPosVideosPrefData)
    println("userItemRatingActiveDataUnion cnt:" + userItemRatingActiveDataUnion.count())


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
        Rating(userIdInt, itemId, row.getAs[Double]("rating"))
    }).rdd.cache

    println("Sample data")

    // userItemRating.show(10, true)

    /*
    userItemRating.show(10, false)

    val als = new ALS()
        .setIterations(5)
      .setNumUserBlocks(200)
      .setNumItemBlocks(50)
      .setRegParam(0.01)
      .setImplicitPrefs(true)
      .setRank(128)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")*/
    val Array(training, test) = userItemRating.randomSplit(Array(0.95, 0.05))
    val model = ALS.train(training, 128, 5, 0.01)


    model.save(sc, outputPath + "/model")
    // als.save(outputPath + "/als")

    println("itemFactor sample")
    val productFeatures = model.productFeatures.toDF("id", "features")
    println(productFeatures.show(10, false))
    val pFeatsC = productFeatures.join(itemMapping.select($"idtype", $"itemId".as("id")), Seq("id"), "left")
    pFeatsC.write.parquet(outputPath + "/itemFactors")
    println("userFactor sample")
    val userFeatures = model.userFeatures.toDF("id", "features")
    println(userFeatures.show(10, false))
    val uFeatsC = userFeatures.join(userIdMapping.select($"userIdStr", $"uid".as("id")), Seq("id"), "left")
    uFeatsC.write.parquet(outputPath + "/userFactors")

    pFeatsC.map(row => {row.getAs[String]("idtype") + "\t" + row.getAs[mutable.WrappedArray[Double]]("features").mkString(",")}).write.text(outputPath + "/itemFactors.txt")
    uFeatsC.map(row => {row.getAs[String]("userIdStr") + "\t" + row.getAs[mutable.WrappedArray[Double]]("features").mkString(",")}).write.text(outputPath + "/userFactors.txt")

    val uid = userIdMapping.filter($"userIdStr" === "359792224").select($"uid").collect().head.getAs[Long]("uid").toInt

    println("uid:" + uid)
    val res = model.recommendProducts(uid, 100)
    println("rcmd for uid")
    res.foreach(println)

    for (r <- res) {
      val scores = sim(model.productFeatures.lookup(r.product).head, model.userFeatures.lookup(uid).head)
      println(r + "rating=" + r.rating + "," + scores)
    }


    // Evaluate the model on rating data
    val usersProducts = test.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val ratesAndPreds = test.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println(s"Mean Squared Error = $MSE")
    //itemFactors.write.text(outputPath + "/itemFactors_txt")
    //model.itemFactors.write.text(outputPath + "/userFactors_txt")

  }
}