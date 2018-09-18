package com.netease.music.recommend.scala.feedflow.cf

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

/**
  * Created by hzzhangjunfei1 on 2017/12/6.
  */
object AlsSimJobEud {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val logger = Logger.getLogger(getClass.getName)

    val options = new Options
    options.addOption("sourceType", true, "all | video_classify | mv")
    options.addOption("featureVectorInput", true, "input directory")
    options.addOption("simType", true, "user | item")
    options.addOption("simOutput", true, "simOutput")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val sourceType = cmd.getOptionValue("sourceType")
    val featureVectorInput = cmd.getOptionValue("featureVectorInput")
    val simType = cmd.getOptionValue("simType")
    val simOutput = cmd.getOptionValue("simOutput")

    import spark.implicits._

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
//      val qualifiedItemFactors = factors
//        .filter(vectorNorm2($"features") > 0)
//        .rdd
//        .map(line => (line.getLong(0), line.getSeq[Float](1)))
//
//      val simTmp = qualifiedItemFactors.cartesian(qualifiedItemFactors)
//        .map(line => ((line._1._1, line._2._1), (line._1._2, line._2._2)))
//        .filter(line => (line._1._1 != line._1._2)) // 过滤掉和本身的feature pair
//        .map(line => (line._1._1, (line._1._2, cosSimilarity(line._2._1.toArray, line._2._2.toArray))))
//        .groupByKey
//        .map({ case (key, value) => collectAndSort(key, value, upperLimit, similarThred) })

//      val qualifiedFactors = factors
      val factorsM = getItemFactorM(factors.collect)
      logger.info("factorsM size:" + factorsM.size)
      val broadcastFactorsM = spark.sparkContext.broadcast[mutable.HashMap[Long, Array[Float]]](factorsM)


      val simData = factors
        .map(row => {
          val id = row.getAs[Long]("id")
          val factors = row.getAs[mutable.WrappedArray[Float]]("features").toArray[Float]
          val simScoreList = sim(broadcastFactorsM, factors)
          val topSimIdScoreList = mutable.ArrayBuffer[String]()
          for (i <- 0 until 100) {
            topSimIdScoreList.append(simScoreList(i)._2 + ":" + simScoreList(i)._1)
          }
          id + "\t" + topSimIdScoreList.mkString(",")
        })

      simData.write.text(simOutput)
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

  def getItemFactorM(rows: Array[Row]) = {
    val itemFactorM = mutable.HashMap[Long, Array[Float]]()
    for (row <- rows) {
      val name = row.getAs[Long]("id")
      val features = row.getAs[mutable.WrappedArray[Float]]("features").toArray[Float]
      itemFactorM.put(name, features)
    }
    itemFactorM
  }

  def sim(itemFactorsMBroadcast: Broadcast[mutable.HashMap[Long, Array[Float]]], factors: Array[Float]) = {
    val scoreNameList = mutable.ArrayBuffer[Tuple2[Float, Long]]()
    for ((candId, candFacotrs) <- itemFactorsMBroadcast.value) {
      val score = simScore(candFacotrs, factors)
      scoreNameList.append(Tuple2(score, candId))
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
}
