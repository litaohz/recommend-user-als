package com.netease.music.recommend.scala.feedflow.userbased

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Itembased_test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("ratingsInput", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val ratingsInput = cmd.getOptionValue("ratingsInput")
    val output = cmd.getOptionValue("output")

    val oriRatings = spark.sparkContext.textFile(ratingsInput).flatMap{line =>
      val info = line.split("\t")
      val user = info(0).toLong
      for (tup <- info(1).split(",")) yield {
        val prefInfo = tup.split(":")
        val item = prefInfo(0).toLong
        val pref = prefInfo(2).toDouble
        ((user, item), pref)
      }
    }.groupByKey
      .map (data => {
        val user = data._1._1
        val item = data._1._2
        val pref = (data._2.sum * 100).toInt
        (user, item, pref)
      })

//    val oriRatings = spark.sparkContext.textFile(ratingsInput).map { line =>
//      val info = line.split("\t")
//      /*(info(0).toLong, info(1).toLong, (info(2).toDouble * 100).toInt)
//    }.groupBy(tup => (tup._1, tup._2))
//      .map(data => (data._1._1, data._1._2, data._2.size))
//      .filter(tup => tup._3>1)*/
//        ((info(0).toLong, info(1).toLong), info(2).toDouble)
//      }.groupByKey
//      .map (data => {
//        val user = data._1._1
//        val item = data._1._2
//        val pref = (data._2.sum * 100).toInt
//        (user, item, pref)
//      })
    val ratings = oriRatings
      .groupBy(k => k._1)
      .flatMap{tuple =>
        (tuple._2.toList.sortWith((x,y)=>x._3>y._3)).take(200)
      }

    val item2manyUser = ratings.groupBy(tup=>tup._2)
    val numRatersPerItem = item2manyUser.map(grouped => (grouped._1, grouped._2.size))

    // (user, item, rating, numRaters)
    val ratingsWithSize = item2manyUser.join(numRatersPerItem).
      flatMap(joined =>  joined._2._1.map(f => (f._1, f._2, f._3, joined._2._2))).
      filter(tup => tup._4 > 1)

    val ratings2 = ratingsWithSize.keyBy(tup => tup._1)
    val ratingPairs = ratings2.join(ratings2).filter(f => f._2._1._2<f._2._2._2)

    // userX 同时对item1,item2进行了评分
    val tempVectorCalcs = ratingPairs.map(data => {
      // (item1, item2)
      val key = (data._2._1._2, data._2._2._2)
      val stats =
        (data._2._1._3 * data._2._2._3,
        data._2._1._3,
        data._2._2._3,
        math.pow(data._2._1._3, 2),
        math.pow(data._2._2._3, 2),
        data._2._1._4,
        data._2._2._4)
      (key, stats)
    })
    val vectorCalcs = tempVectorCalcs.groupByKey().map(data => {
      // (item1, item2)
      val key = data._1
      val vals = data._2
      val size = vals.size
      val dotProduct = vals.map(f => f._1).sum
      val ratingSum = vals.map(f => f._2).sum
      val rating2Sum = vals.map(f => f._3).sum
      val ratingSq = vals.map(f => f._4).sum
      val rating2Sq = vals.map(f => f._5).sum
      val numRaters = vals.map(f => f._6).sum
      val numRaters2 = vals.map(f => f._7).sum
      (key, (size, dotProduct, ratingSum, rating2Sum, ratingSq, rating2Sq, numRaters, numRaters2))
    })

    val inverseVectorCalcs = vectorCalcs.map(x=>((x._1._2, x._1._1), (x._2._1, x._2._2, x._2._4, x._2._3, x._2._6, x._2._5, x._2._8, x._2._7)))
    val vectorCalcsTotal = vectorCalcs ++ inverseVectorCalcs

    val tempSimilarities = vectorCalcsTotal.map(fields=> {
      val key = fields._1
      val (size ,dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
      val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))*size/(numRaters*math.log10(numRaters2+10))
      (key._1, (key._2, cosSim))
    })
    val similarities = tempSimilarities.groupByKey().map(x=> {
      x._1 + "\t" + x._2.toList.sortWith((a, b) => a._2>b._2).take(100).map(tup=>tup._1 + ":" + tup._2).mkString(",")
    })

    similarities.saveAsTextFile(output)
  }



  def correlation(size:Double, dotProduct:Double, ratingSum:Double, rating2Sum:Double, ratingNormSeq:Double, rating2NormSq:Double) = {
    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSeq - ratingSum * ratingSum) *
      scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum) + 1
    numerator / denominator
  }
  def regularizedCorrelation(size:Double, dotProduct:Double, ratingSum:Double, rating2Sum:Double, ratingNormSq:Double, rating2NormSq:Double, virtualCount:Double, priorCorrelation:Double) = {
    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1-w) * priorCorrelation
  }
  def cosineSimilarity(dotProduct:Double, ratingNorm:Double, rating2Norm:Double) = {
    dotProduct / (rating2Norm * rating2Norm)
  }
  def jaccardSimilarity(userInCommon:Double, totalUsers1:Double, totalUsers2:Double) = {
    val union = totalUsers1 + totalUsers2 - userInCommon
    userInCommon / union
  }
}
