package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by hzzhangjunfei1 on 2017/12/6.
  */
object AlsSimJob {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
//    options.addOption("alsModelInput", true, "input directory")
    options.addOption("sourceType", true, "all | video_classify | mv")
    options.addOption("featureVectorInput", true, "input directory")
    options.addOption("simType", true, "user | item")
    options.addOption("simOutput", true, "simOutput")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

//    val alsModelInput = cmd.getOptionValue("alsModelInput")
    val sourceType = cmd.getOptionValue("sourceType")
    val featureVectorInput = cmd.getOptionValue("featureVectorInput")
    val simType = cmd.getOptionValue("simType")
    val simOutput = cmd.getOptionValue("simOutput")

    import spark.implicits._

//    val alsModel = ALSModel.load(alsModelInput)
    val factors = spark.read.parquet(featureVectorInput)

    var simTypeError = false
    var upperLimit = 100
    if (simType.equals("user")) {
      upperLimit = 10
    } else if (simType.equals("item")) {
      upperLimit = 100
    } else
      simTypeError = true

    var similarThred = 0.5
    if (sourceType.equals("all")) {
      similarThred = 0.7
    }
    if (!simTypeError) {
      // 计算相似item
      val qualifiedItemFactors = factors
        .filter(vectorNorm2($"features") > 0)
        .rdd
        .map(line => (line.getLong(0), line.getSeq[Float](1)))

      val simTmp = qualifiedItemFactors.cartesian(qualifiedItemFactors)
        .map(line => ((line._1._1, line._2._1), (line._1._2, line._2._2)))
        .filter(line => (line._1._1 != line._1._2)) // 过滤掉和本身的feature pair
        .map(line => (line._1._1, (line._1._2, cosSimilarity(line._2._1.toArray, line._2._2.toArray))))
        .groupByKey
        .map({ case (key, value) => collectAndSort(key, value, upperLimit, similarThred) })

      simTmp.saveAsTextFile(simOutput)
    }
  }

  def collectAndSort(id: Long, seq: Iterable[(Long, Double)], upperLimit:Int, similarThred:Double):String = {

    id + "\t" +
    seq.toArray.sortWith(_._2 > _._2)
      .filter(_._2 >= similarThred)
      .map(pair => pair._1 + ":" + pair._2)
      .slice(0, upperLimit)
      .mkString(",")
  }
}
