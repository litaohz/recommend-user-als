package com.netease.music.recommend.scala.feedflow.tag

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable;

/**
  * word2vec
  */
object MixedSimVideo {

  def getItemFactorM(rows: Array[Row]) = {
    val itemFactorM = mutable.HashMap[Int, (Array[Float], String)]()
    for (row <- rows) {
      val id = row.getAs[Int]("id")
      val idtype = row.getAs[String]("idtype")
      val features = row.getAs[mutable.WrappedArray[Float]]("features").toArray[Float]
      itemFactorM.put(id, (features, idtype))
    }
    itemFactorM
  }

  def sim(itemFactorsMBroadcast: Broadcast[mutable.HashMap[Int, (Array[Float], String)]], factors: Array[Float]) = {
    val scoreNameList = mutable.ArrayBuffer[Tuple2[Float, String]]()
    for ((id, candFacotrs) <- itemFactorsMBroadcast.value) {
      val score = simScore(candFacotrs._1, factors)
      val idtype = candFacotrs._2
      scoreNameList.append(Tuple2(score, idtype))
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
    options.addOption("mixed_index", true, "input")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val inputItemPath = cmd.getOptionValue("input_item")
    val outputPath = cmd.getOptionValue("output")
    val mixedIndexPath = cmd.getOptionValue("mixed_index")

    val itemFactors = spark.read.parquet(inputItemPath)
    println("itemFactors schema:")
    itemFactors.printSchema()
    itemFactors.show(10, false)
    val mixedIndex = spark.read.textFile(mixedIndexPath).map(line => {
      val ts = line.split("\t")
      (ts(0), ts(1).toInt)
    }).toDF("idtype", "id")
    println("mixedIndex schema:")
    mixedIndex.printSchema()
    mixedIndex.show(10, false)

    val itemWithType = itemFactors.join(mixedIndex, Seq("id"), "left")
    println("itemWithType schema:")
    itemWithType.printSchema()
    itemWithType.show(10, false)

    val itemFactorsIdtypeM = getItemFactorM(itemWithType.filter($"idtype".endsWith("-video")).collect())
    print ("itemFactorsIdtypeM size:" + itemFactorsIdtypeM.size)
    val itemFactorsMBroadcast = sc.broadcast[mutable.HashMap[Int, (Array[Float], String)]](itemFactorsIdtypeM)

    val simTagData = itemWithType.repartition(2000).map(row => {
      val id = row.getAs[Int]("id")
      val idtype = row.getAs[String]("idtype")
      val factors = row.getAs[mutable.WrappedArray[Float]]("features").toArray[Float]
      val simNameScoreList = sim(itemFactorsMBroadcast, factors)
      val topSimNameScoreList = mutable.ArrayBuffer[String]()
      for (i <- 0 until 100) {
        topSimNameScoreList.append(simNameScoreList(i)._2 + ":" + simNameScoreList(i)._1)
      }
      // tag \t simtag:score,simtag:score...
      idtype + "\t" + topSimNameScoreList.mkString(",")
    })

    simTagData.repartition(10).write.text(outputPath)

  }
}