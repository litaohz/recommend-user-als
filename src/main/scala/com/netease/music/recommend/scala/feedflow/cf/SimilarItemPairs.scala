package com.netease.music.recommend.scala.feedflow.cf

import com.netease.music.recommend.scala.feedflow.utils.userFunctions._
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SimilarItemPairs {

  case class itemSimilarityPair(row:Long, column:Long, similarity:Double)
  case class SimilarItemInfo(id:Long, similarity:Double)
  case class ClusterInfo(id:Long, isAccurate:Boolean)

  def collectSimilarItems(rowId:Long, columnIds:Iterable[SimilarItemInfo]):String = {
    val simItemList = columnIds.toArray
      .sortWith(_.similarity > _.similarity)
      .map {line =>
        line.id + ":" + line.similarity
      }
    rowId + "\t" + simItemList.mkString(",")
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("itemFactorsKmeans", true, "input directory")
    options.addOption("kmeansPredInfo", true, "input directory")
    options.addOption("outputSimPair", true, "output directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val itemFactorsKmeansInput = cmd.getOptionValue("itemFactorsKmeans")
    val kmeansPredInfoInput = cmd.getOptionValue("kmeansPredInfo")
    val outputSimPair = cmd.getOptionValue("outputSimPair")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val kmeansPredInfoTable = spark.read.option("sep", "\t")
      .csv(kmeansPredInfoInput)
      .toDF("prediction", "itemIdSet", "groupCnt")
      .select("prediction", "groupCnt")
    val clusterInfoArray = kmeansPredInfoTable
      .filter($"groupCnt">1 && $"groupCnt"<10000)
      .map{line =>
        val clusterId = line.getString(0).toLong
        val groupCnt = line.getString(1).toInt
        ClusterInfo(clusterId, groupCnt>30)
      }.collect

    val itemFactorsKmeansTable = spark.read.parquet(itemFactorsKmeansInput)
      .cache

    for (clusterInfo <- clusterInfoArray) {
      logger.warn(clusterInfo.id + ":" + clusterInfo.isAccurate + " is being processed...")

      val predictionValue = clusterInfo.id
      val itemsToCalculatedRdd = itemFactorsKmeansTable
        .select("features")
        .rdd
        .map {
          line =>
            val denseVector = line.getAs[org.apache.spark.ml.linalg.SparseVector]("features").toDense
            org.apache.spark.mllib.linalg.Vectors.fromML(denseVector)
        }
      val featureMatrixTransposed = transposeRowMatrix(new RowMatrix(itemsToCalculatedRdd))
      val similarityPairsCordinateMatrix = featureMatrixTransposed.columnSimilarities(0.1)

      val itemIndex = itemFactorsKmeansTable
        .select("itemId", "cluster_rn")
        .filter($"prediction"===predictionValue)

      val similarPairsTable = similarityPairsCordinateMatrix
        .entries
        .map {
          case MatrixEntry(row, col, similarity) => itemSimilarityPair(row.toLong, col.toLong, similarity.toDouble)
        }
        .toDF
        .join(itemIndex, $"row"===itemIndex("cluster_rn"), "left_outer")
        .withColumn("rowItemId", $"itemId")
        .drop("itemId", "cluster_rn")
        .join(itemIndex, $"column"===itemIndex("cluster_rn"), "left_outer")
        .withColumn("columnItemId", $"itemId")
        .drop("itemId", "cluster_rn")
      similarPairsTable.write.parquet(outputSimPair + "/" + predictionValue)

      val simResult = similarPairsTable
        .filter($"row"=!=0)
        .filter($"similarity">0)
        .select("rowItemId", "columnItemId", "similarity")
        .rdd
        .flatMap { line =>
          val rowId = line.getLong(0)
          val columnId = line.getLong(1)
          val similarity = line.getDouble(2)
          Seq(rowId+","+columnId+","+similarity, columnId+","+rowId+","+similarity)
        }
        .map { line =>
          val info = line.split(",")
          val rowId = info(0).toLong
          val columnId = info(1).toLong
          val similarity = info(2).toDouble
          (rowId, SimilarItemInfo(columnId, similarity))
        }
        .groupByKey
        .map{case(key, value) => collectSimilarItems(key, value)}
      simResult.saveAsTextFile(output + "/" + predictionValue)
    }

//    val predictionValue = 30
//    val itemFactorsKmeansTable = spark.read.parquet(itemFactorsKmeansInput)
//    val itemsToCalculatedRdd = itemFactorsKmeansTable
////      .select(expandVector($"features").as("features"))
//      .select("features")
//      .filter($"prediction"===predictionValue)
//      .rdd
//      .map {
//        line =>
//          val denseVector = line.getAs[org.apache.spark.ml.linalg.SparseVector]("features").toDense
//          org.apache.spark.mllib.linalg.Vectors.fromML(denseVector)
//      }
//    val featureMatrixTransposed = transposeRowMatrix(new RowMatrix(itemsToCalculatedRdd))
//    val similarityPairsCordinateMatrix = featureMatrixTransposed.columnSimilarities(0.1)
////    similarityPairsCordinateMatrix.entries.saveAsTextFile(output)
//
//    val itemIndex = itemFactorsKmeansTable
//      .select("itemId", "cluster_rn")
//      .filter($"prediction"===predictionValue)
//
//    val similarPairsTable = similarityPairsCordinateMatrix
//      .entries
//      .map {
//        case MatrixEntry(row, col, similarity) => itemSimilarityPair(row.toLong, col.toLong, similarity.toDouble)
//      }
//      .toDF
//      .join(itemIndex, $"row"===itemIndex("cluster_rn"), "left_outer")
//      .withColumn("rowItemId", $"itemId")
//      .drop("itemId", "cluster_rn")
//      .join(itemIndex, $"column"===itemIndex("cluster_rn"), "left_outer")
//      .withColumn("columnItemId", $"itemId")
//      .drop("itemId", "cluster_rn")
//    similarPairsTable.write.parquet(outputSimPair)
//
//    val simResult = similarPairsTable
//      .filter($"row"=!=0)
//      .filter($"similarity">0)
//      .select("rowItemId", "columnItemId", "similarity")
//      .rdd
//      .flatMap { line =>
//        val rowId = line.getLong(0)
//        val columnId = line.getLong(1)
//        val similarity = line.getDouble(2)
//        Seq(rowId+","+columnId+","+similarity, columnId+","+rowId+","+similarity)
//      }
//      .map { line =>
//        val info = line.split(",")
//        val rowId = info(0).toLong
//        val columnId = info(1).toLong
//        val similarity = info(2).toDouble
//        (rowId, SimilarItemInfo(columnId, similarity))
//      }
//      .groupByKey
//      .map{case(key, value) => collectSimilarItems(key, value)}
//    simResult.saveAsTextFile(output)
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
