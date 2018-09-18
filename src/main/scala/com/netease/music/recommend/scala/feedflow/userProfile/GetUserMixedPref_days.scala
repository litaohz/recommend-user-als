package com.netease.music.recommend.scala.feedflow.userProfile

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GetUserMixedPref_days {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("userVideoPref", true, "input directory")
    options.addOption("userSongPref", true, "input directory")
    options.addOption("userArtPref", true, "input directory")

    options.addOption("output", true, "output directory")
    options.addOption("outputIndex", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val userVideoPref = cmd.getOptionValue("userVideoPref")
    val userSongPref = cmd.getOptionValue("userSongPref")
    val userArtPref = cmd.getOptionValue("userArtPref")

    val output = cmd.getOptionValue("output")
    val outputIndex = cmd.getOptionValue("outputIndex")

    import spark.implicits._
    val videoPref = spark.sparkContext.textFile(userVideoPref)
      .flatMap{line =>
        val info = line.split("\t")
        val userId = info(0).toLong
        for (prefInfoStr <- info(1).split(",")) yield {
          val prefInfo = prefInfoStr.split(":")
          val item = prefInfo(0) + "-video"
          val pref = prefInfo(2).toFloat.formatted("%.4f")
          (userId, item, pref)
        }
      }
      .toDF("userId", "itemId", "pref")
    val songPref = spark.sparkContext.textFile(userSongPref)
      .flatMap{line =>
        val info = line.split("\t")
        val userId = info(0).toLong
        for (prefInfoStr <- info(1).split(",")) yield {
          val prefInfo = prefInfoStr.split(":")
          val item = prefInfo(0) + "-song"
          val pref = prefInfo(1).toFloat.formatted("%.4f")
          (userId, item, pref)
        }
      }
      .toDF("userId", "itemId", "pref")
    val artPref = spark.sparkContext.textFile(userArtPref)
      .flatMap{line =>
        val info = line.split("\t")
        val userId = info(0).toLong
        for (prefInfoStr <- info(1).split(",")) yield {
          val prefInfo = prefInfoStr.split(":")
          val item = prefInfo(0) + "-art"
          val pref = prefInfo(1).toFloat.formatted("%.4f")
          (userId, item, pref)
        }
      }
      .toDF("userId", "itemId", "pref")

    val finalDataset = videoPref
      .union(songPref)
      .union(artPref)
      .groupBy("userId", "itemId")
      .agg(sum($"pref").as("totalPref"))
      .coalesce(16)
    finalDataset.write.option("sep", "\t").csv(output + "/originalId")

    val windowSpec = Window.partitionBy("groupColumn").orderBy("groupColumn")
    val index = finalDataset
      .groupBy("itemId")
      .agg(count("itemId").as("cnt"))
      .withColumn("groupColumn", lit(1))
      .withColumn("mixedId", row_number() over(windowSpec))
      .select("itemId", "mixedId")
      .coalesce(1)
    index.write.option("sep", "\t").csv(outputIndex)

    finalDataset
      .join(index, Seq("itemId"), "left_outer")
      .select("userId", "mixedId", "totalPref")
      .coalesce(16)
      .write.option("sep", "\t").csv(output + "/mixedId")

  }

  def transposeRowMatrix(mat: RowMatrix): RowMatrix = {
    val transposedRowsRDD = mat.rows.zipWithIndex
      .map{case(row, rowIndex) => rowToTransposedTriplet(row, rowIndex)}
      .flatMap(x => x)
      .groupByKey
      .sortByKey().map(_._2)
      .map(buildRow)
    new RowMatrix(transposedRowsRDD)
  }

  def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
    val indexedRow = row.toArray.zipWithIndex
    indexedRow.map{case(value, colIndex) => (colIndex.toLong, (rowIndex, value))}
  }

  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
    val resArr = new Array[Double](rowWithIndexes.size)
    rowWithIndexes.foreach{case (index, value) => resArr(index.toInt) = value }
    Vectors.dense(resArr)
  }

}
