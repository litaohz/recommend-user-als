package com.netease.music.recommend.scala.feedflow.tag

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable;

/**
  * word2vec
  */
object TagALSSimVideo {

  def getItemFactorM(rows: Array[Row]) = {
    val itemFactorM = mutable.HashMap[String, Array[Float]]()
    for (row <- rows) {
      val name = row.getAs[String]("idtype")
      val features = row.getAs[mutable.WrappedArray[Float]]("features").toArray[Float]
      itemFactorM.put(name, features)
    }
    itemFactorM
  }

  def sim(itemFactorsMBroadcast: Broadcast[mutable.HashMap[String, Array[Float]]], factors: Array[Float]) = {
    val scoreNameList = mutable.ArrayBuffer[Tuple2[Float, String]]()
    for ((candname, candFacotrs) <- itemFactorsMBroadcast.value) {
      val score = simScore(candFacotrs, factors)
      scoreNameList.append(Tuple2(score, candname))
    }
    //scoreNameList.sortBy[Float](t2 => t2._1, false)
    val sortedNameList = scoreNameList.sortBy[Float](-_._1)
    sortedNameList
  }

  def simScore(candFactors: Array[Float], factors: Array[Float]) = {
    var product =  0.0F
    var sqrt1 = 0.0F
    var sqrt2 = 0.0F
    for (i <- 0 until candFactors.length) {
      product += candFactors(i) * factors(i)
      sqrt1 += candFactors(i) * candFactors(i)
      sqrt2 += factors(i) * factors(i)
    }

    product / (math.sqrt(sqrt1.toDouble) * math.sqrt(sqrt2.toDouble)).toFloat
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()
    options.addOption("input_item", true, "input")
    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val inputItemPath = cmd.getOptionValue("input_item")
    val outputPath = cmd.getOptionValue("output")

    val itemFactors = spark.read.parquet(inputItemPath)

    val itemFactorsM = getItemFactorM(itemFactors.collect())
    print ("itemFactorM size:" + itemFactorsM.size)
    val itemFactorsMBroadcast = sc.broadcast[mutable.HashMap[String, Array[Float]]](itemFactorsM)

    val simTagData = itemFactors.map(row => {
      val name = row.getAs[String]("idtype")
      val factors = row.getAs[mutable.WrappedArray[Float]]("features").toArray[Float]
      val simNameScoreList = sim(itemFactorsMBroadcast, factors)
      val topSimNameScoreList = mutable.ArrayBuffer[String]()
      for (i <- 0 until 100) {
        topSimNameScoreList.append(simNameScoreList(i)._2 + ":" + simNameScoreList(i)._1)
      }
      // tag \t simtag:score,simtag:score...
      name + "\t" + topSimNameScoreList.mkString(",")
    })

    simTagData.write.text(outputPath)

  }
}