package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, RowMatrix}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.spark.sql.expressions.Window

object FindRelativeItemsByKmeans {

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
    //    options.addOption("videoPool", true, "input directory")
    options.addOption("output", true, "output directory")
    options.addOption("outputKmeans", true, "output directory")
    options.addOption("outputVideoPoolVec", true, "output directory")
    options.addOption("outputVec", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val itemFactorsInput = cmd.getOptionValue("itemFactors")
    //    val videoPoolInput = cmd.getOptionValue("videoPool")
    val output = cmd.getOptionValue("output")
    val outputKmeans = cmd.getOptionValue("outputKmeans")
    val outputVideoPoolVec = cmd.getOptionValue("outputVideoPoolVec")
    val outputVec = cmd.getOptionValue("outputVec")

    import spark.implicits._

    val itemFactorsTable = spark.read.parquet(itemFactorsInput)
      .select($"itemId", $"features", hasBigDimVector(2)($"features").as("hasBigDim"))
      .cache

    itemFactorsTable
      .rdd
      .map {line =>
        line.getLong(0) + "\t" + line.getSeq[Float](1).mkString(",") + "\t" + line.getInt(2)
      }
      .saveAsTextFile(outputVec)

    //    val videoPoolTable = spark.read.parquet(videoPoolInput)
    //      .select("videoId")

    itemFactorsTable
      //      .join(videoPoolTable, $"itemId"===$"videoId", "inner")
      .rdd
      .map {line =>
        line.getLong(0) + "\t" + line.getSeq[Float](1).mkString(",") + "\t" + line.getInt(2)
      }
      .saveAsTextFile(outputVideoPoolVec)

    //    val itemFactorsVectorTable = itemFactorsTable
    //      .rdd
    //      .map{line =>
    //        KmeansClass(line.getLong(0).toDouble, Vectors.dense(line.getSeq[Float](1).map(_.toDouble).toArray))
    //      }.toDF
    //
    //    val kmeans = new KMeans()
    //      .setK(1000)
    //      .setFeaturesCol("features")
    //      .setSeed(1L)
    //    val model = kmeans.fit(itemFactorsVectorTable)
    //
    //    val predictionWindow = Window.partitionBy("prediction").orderBy($"prediction")
    //    val predTable = model.transform(itemFactorsVectorTable)
    //      .withColumn("itemId", double2Long($"id"))
    //      .select($"itemId", $"prediction", $"features", row_number().over(predictionWindow).as("cluster_rn"))
    //    predTable.write.parquet(outputKmeans)
    //
    //    val result = predTable
    ////      .withColumn("itemId", double2Long($"id"))
    //      .groupBy("prediction")
    //      .agg(count("itemId").as("groupCnt"), collect_set("itemId").as("idSet"))
    //      .orderBy($"groupCnt".desc)
    //      .withColumn("ids", set2String($"idSet"))
    //    result
    //      .select("prediction", "ids", "groupCnt")
    //      .write.option("sep", "\t").csv(output)
  }

}
