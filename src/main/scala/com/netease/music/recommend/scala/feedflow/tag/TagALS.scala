package com.netease.music.recommend.scala.feedflow.tag

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable;

/**
  * word2vec
  */
object TagALS {

  def getTagIdxM(strings: Array[String]) = {
    val tagM = new mutable.HashMap[String, Long]()
    for (line <- strings) {
      // id type tag_TT,tag_XX,... \t
      val ts = line.split("\t")
      val idtype = ts(0) + "-" + ts(1)
      if (!tagM.contains(idtype)) {
        tagM.put(idtype, tagM.size.toLong)
      }

      val tags = ts(2).split(",")
      for (tag <- tags) {
        val wtype = tag.split("_")
        if (!wtype(1).equals("CC") && !wtype(1).equals("WD") && !wtype(1).equals("OT") && !wtype(1).equals("KW")) {
          val word = wtype(0)
          if (!tagM.contains(word)) {
            tagM.put(word, tagM.size.toLong)
          }
        }

      }
    }
    tagM
  }
  def getIdxTagM(tagIdxM : mutable.HashMap[String, Long]) = {
    val idxTagM = new mutable.HashMap[Long, String]()
    for (entry <- tagIdxM) {
      idxTagM.put(entry._2, entry._1)
    }
    idxTagM
  }
  case class UidTags(userId:Long, tags:String)

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
    val videoTags = cmd.getOptionValue("videoTags")

    val tagIdxM = getTagIdxM(sc.textFile(videoTags).collect())
    println("tagIdxM size:" + tagIdxM.size)
    val idxTagM = getIdxTagM(tagIdxM)
    for (i <- 0 until 10) {
     println(i + "=>" + idxTagM.get(i))
    }

    val tagIdxMBroad : Broadcast[mutable.HashMap[String, Long]] = sc.broadcast[mutable.HashMap[String, Long]](tagIdxM)

    val usreItmeRating = spark.sparkContext.textFile(inputPath).map(line => {
      val ts = line.split("\t", 2)
      (ts(0).toLong, ts(1))
    }).flatMap({case(uid, tags) => {
      val res = mutable.ArrayBuffer[Tuple3[Long, Long, Double]]()
      val tagScores = tags.split(",")
      val tagIdxMLocalM : mutable.HashMap[String, Long] = tagIdxMBroad.value
      var maxSize = 100
      if (tagScores.length < maxSize * 2) {
        maxSize = tagScores.length / 2
      }
      for (i <- 0 until maxSize) {
        val tagScore = tagScores(i)
        val ts = tagScore.split(":")
        if (ts.length >= 2) {
          val tagIdx = tagIdxMLocalM.get(ts(0)) // TODO
          if (tagIdx != None) {
            res.append((uid.toInt, tagIdx.get.toInt, ts(1).toDouble))
          }
        }
      }
      res
    }}).toDF("userId", "itemId", "rating").cache()
    println("Sample data")
    usreItmeRating.show(10, true)
    usreItmeRating.show(10, false)

    val als = new ALS()
      .setMaxIter(5)
        .setNumUserBlocks(200)
        .setNumItemBlocks(50)
      .setRegParam(0.01)
      .setImplicitPrefs(true)
      .setRank(100)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")
      val model = als.fit(usreItmeRating)

    als.save(outputPath + "/model")

    model.itemFactors.write.parquet(outputPath + "/userFactors")


    val idxTagMapping = idxTagM.toSeq.toDF("itemIdx", "itemName")
    val itemFactors = model.itemFactors.join(idxTagMapping, $"id"===$"itemIdx", "left")
    itemFactors.write.parquet(outputPath + "/itemFactors")

    //itemFactors.write.text(outputPath + "/itemFactors_txt")
    //model.itemFactors.write.text(outputPath + "/userFactors_txt")

  }
}