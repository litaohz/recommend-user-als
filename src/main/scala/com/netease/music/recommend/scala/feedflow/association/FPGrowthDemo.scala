package com.netease.music.recommend.scala.feedflow.association

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FPGrowthDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("userMixedPref", true, "input directory")
    options.addOption("minPrefsPerUser", true, "minPrefsPerUser")
    options.addOption("minSupport", true, "minSupport")
    options.addOption("numPartition", true, "numPartition")
    options.addOption("minConfidence", true, "minConfidence")

    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val minPrefsPerUser = cmd.getOptionValue("minPrefsPerUser").toInt
    val minSupport = cmd.getOptionValue("minSupport").toDouble
    val numPartition = cmd.getOptionValue("numPartition").toInt
    val minConfidence = cmd.getOptionValue("minConfidence").toDouble
    val userMixedPref = cmd.getOptionValue("userMixedPref")

    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val videoTransactionRDD = spark.read.option("sep", "\t")
      .csv(userMixedPref)
      .filter(line => line.getString(1).split("-")(1).equals("video"))
      .map { line =>
        val userId = line.getString(0)
        val itemInfo = line.getString(1).split("-")
        val videoId = itemInfo(0)
        (userId, videoId)
      }
      .rdd
      .groupByKey()
      .map(tup => tup._2.toArray)
      .filter(array => array.size > minPrefsPerUser)
    videoTransactionRDD.saveAsTextFile(output + "/videoTransactions")

    val fpGrowth = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)

    val fpgModel = fpGrowth.run(videoTransactionRDD)

    fpgModel.save(spark.sparkContext, output + "/fpgModel")

    val ruleRDD = fpgModel.generateAssociationRules(minConfidence)
      .map {rule =>
        (rule.antecedent.mkString(","), rule.consequent.mkString(","), rule.confidence)
      }
      .groupBy(tup => tup._1)
      .map(tup => tup._1 + "\t" + tup._2.toArray.sortWith(_._3 > _._3).map(item => item._2 + ":" + item._3).mkString(","))

    ruleRDD.saveAsTextFile(output + "/rules")
  }
}
