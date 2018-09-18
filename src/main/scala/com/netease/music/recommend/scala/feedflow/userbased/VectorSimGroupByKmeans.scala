package com.netease.music.recommend.scala.feedflow.userbased

import breeze.linalg.DenseVector
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by hzzhangjunfei1 on 2018/8/8.
  */
object VectorSimGroupByKmeans {

  def seq2Vec = udf((features:String) => Vectors.dense(features.split(",").map(_.toDouble)))

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("kmeansPredictions", true, "input directory")
    options.addOption("clusterSizeExpected", true, "clusterSize expected")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val kmeansPredictions = cmd.getOptionValue("kmeansPredictions")
    val clusterSizeExpected = cmd.getOptionValue("clusterSizeExpected").toInt
    val output = cmd.getOptionValue("output")

    import spark.implicits._
    val rand = new Random(1214L)
    val simsRdd = spark.read.parquet(kmeansPredictions)
      .withColumn("cid_adjust", getCidstrByClustersize(clusterSizeExpected, rand)($"cid_final", $"clusterSize"))
      .rdd.map { row =>
      (row.getAs[Int]("cid_adjust"),
        (row.getAs[String]("idStr"),
          DenseVector[Double](row.getAs[org.apache.spark.ml.linalg.DenseVector]("features").values)
        )
      )
    }.groupByKey
      .flatMap { case (key, values) =>
        val simM = mutable.HashMap[(String, String), Double]()
        values.foreach { tup1 =>
          values.foreach { tup2 =>
            if (!tup1._1.equals(tup2._1)
              && !simM.contains((tup1._1, tup2._1))
              && !simM.contains(tup2._1, tup1._1))
              simM.put((tup1._1, tup2._1), cosineSim(tup1._2, tup2._2))
          }
        }
        simM.map(entry => (entry._1._1, entry._1._2, entry._2)).toSet
      }/*.toDF("id1", "id2", "simScore")*/
//    val simsDf_reverse = simsDf.select("id2", "id1", "simScore")
//      .toDF("id1", "id2", "simScore")
//    val simsDf_final = simsDf.union(simsDf_reverse)
    val finalRdd = simsRdd
      .flatMap { row =>
        Set(
          (row._1, (row._2, row._3)),
          (row._2, (row._1, row._3))
        )
      }.groupByKey
      .map{case (source, sims) =>
          val sims_str = sims.toArray.sortWith(_._2 > _._2)
            .map(tup => tup._1 + ":" + tup._2.formatted("%.4f"))
            .slice(0, 30)
            .mkString(",")
          source + "\t" + sims_str
      }

    finalRdd
      .repartition(32) // 合并小文件
      .saveAsTextFile(output)
  }

  def cosineSim(x: DenseVector[Double], y: DenseVector[Double]) = {
    val xy = x.dot(y)
    val xx = Math.sqrt(x.dot(x))
    val yy = Math.sqrt(y.dot(y))
    xy / (xx * yy)
  }

  def adjust_clusterId = udf((clusterId: Int, clusterSize: Long) =>{
    if (clusterSize < 50)
      -1
    else
      clusterId
  })

  def getCidstrByClustersize(clusterSizeExpected: Int, rand : Random) = udf((cid_final :Int, clusterSize: Long)  => {

    if (clusterSize > 10000) {

      val randMode = Math.ceil(clusterSize.toDouble / clusterSizeExpected).toInt
      cid_final + "_" + rand.nextInt(randMode)

    } else {
      cid_final.toString
    }
  })
}
