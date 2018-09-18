package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import com.netease.music.recommend.scala.feedflow.utils.userFunctions._

object ALSRecall {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
//    options.addOption("alsModel", true, "input directory")
    options.addOption("itemFactors", true, "input directory")
//    options.addOption("itemWhitelist", true, "output directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

//    val alsModelInput = cmd.getOptionValue("alsModel")
    val itemFactorsInput = cmd.getOptionValue("itemFactors")
//    val itemWhitelistInput = cmd.getOptionValue("itemWhitelist")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val itemFactorsVectorRdd = spark.read.parquet(itemFactorsInput)
//      .withColumn("featureVector", getFeatureVector($"features"))
      .rdd
      .map { line =>
        IndexedRow(line.getInt(0).toLong, Vectors.dense(line.getSeq[Float](1).map(_.toDouble).toArray))
      }
//    val itemFactorsMat = new RowMatrix(itemFactorsVectorRdd)
//    val indexedItemMat = new IndexedRowMatrix(itemFactorsVectorRdd)
//    val itemFactorsMat = indexedItemMat.rows.toDF.limit(100)
//      .map{line =>
//        Vectors.dense(line.getSeq[Double](1).toArray)
//      }
//    val itemFactorsMatTransposed = transposeRowMatrix(itemFactorsMat)
//    val sim = itemFactorsMatTransposed.columnSimilarities
//      .entries
//      .map{case(entry) => entry.i + "\t" + entry.j + "\t" + entry.value}
//      .saveAsTextFile(output)


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
