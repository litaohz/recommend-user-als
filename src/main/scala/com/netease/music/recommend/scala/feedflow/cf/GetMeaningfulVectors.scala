package com.netease.music.recommend.scala.feedflow.cf

import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession

object GetMeaningfulVectors {

  case class KmeansClass(id:Double, features:Vector)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("itemFactors", true, "input directory")
    options.addOption("videoPool", true, "videoPool")
    options.addOption("bigDigitNum", true, "bigDigitNum")
    options.addOption("outputLogVectrs", true, "output directory")
    options.addOption("idType", true, "user or video")
    options.addOption("outputPoolVectors", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val itemFactorsInput = cmd.getOptionValue("itemFactors")
    val videoPoolInput = cmd.getOptionValue("videoPool")
    val bigDigitNum = cmd.getOptionValue("bigDigitNum").toInt
    val outputLogVectrs = cmd.getOptionValue("outputLogVectrs")
    val idType = cmd.getOptionValue("idType")
    val outputPoolVectors = cmd.getOptionValue("outputPoolVectors")

    import spark.implicits._

    val itemFactorsTable =
      if (idType.equals("video")) {
        spark.read.parquet(itemFactorsInput)
          .select($"itemId", $"features", hasBigDimVector(bigDigitNum)($"features").as("hasBigDim"))
      } else {
        spark.read.parquet(itemFactorsInput)
          .select($"userId", $"features", hasBigDimVector(bigDigitNum)($"features").as("hasBigDim"))
      }



    itemFactorsTable
      .rdd
      .map {line =>
        line.getLong(0) + "\t" + line.getSeq[Float](1).mkString(",") + "\t" + line.getInt(2)
      }
      .saveAsTextFile(outputLogVectrs)

    if (idType.equals("video")) {
      val videoPoolTable = spark.read.parquet(videoPoolInput)
        .select("videoId", "eventId")
      logger.warn("videoPoolTable size:" + videoPoolTable.count)
      itemFactorsTable
        .join(videoPoolTable, $"itemId" === $"eventId", "inner")
        .select($"videoId", $"features", $"hasBigDim")
        .rdd
        .map { line =>
          line.getLong(0) + "\t" + line.getSeq[Float](1).mkString(",") + "\t" + line.getInt(2)
        }
        .saveAsTextFile(outputPoolVectors)
    }

  }

}
