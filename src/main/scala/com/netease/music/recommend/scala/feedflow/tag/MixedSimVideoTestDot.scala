package com.netease.music.recommend.scala.feedflow.tag

import breeze.linalg.DenseVector
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{ColumnName, Encoders, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable;

/**
  * word2vec
  */
object MixedSimVideoTestDot {

  def getItemFactorM(rows: mutable.WrappedArray[Row]) = {
    val itemFactorM = mutable.HashMap[Int, (DenseVector[Double], String)]()
    for (row <- rows) {
      val id = row.getAs[Int]("id")
      val idtype = row.getAs[String]("idtype")
      val features = row.getAs[Vector[Double]]("normFeatures")
      itemFactorM.put(id, (DenseVector(features.toArray[Double]), idtype))
    }
    itemFactorM
  }

  def sim(itemFactorsMBroadcast: Broadcast[mutable.HashMap[Int, (DenseVector[Double], String)]], factors: DenseVector[Double]) = {
    val scoreNameList = mutable.ArrayBuffer[Tuple2[Tuple2[Double, Double], String]]()
    for ((id, candFacotrs) <- itemFactorsMBroadcast.value) {
      val (score1, scoreDot) = simScore(candFacotrs._1, factors)
      val idtype = candFacotrs._2
      scoreNameList.append(Tuple2((score1, scoreDot), idtype))
    }
    //scoreNameList.sortBy[Double](t2 => t2._1, false)
    val sortedNameList1 = scoreNameList.sortBy[Double](-_._1._1)
    val sortedNameList2 = scoreNameList.sortBy[Double](-_._1._2)
    (sortedNameList1, sortedNameList2)
  }

  def simScore(candFactors: DenseVector[Double], factors: DenseVector[Double]) = {
    var product =  0.0
    var sqrt1 = 0.0
    var sqrt2 = 0.0
    for (i <- 0 until candFactors.length) {
      product += candFactors(i) * factors(i)
      sqrt1 += candFactors(i) * candFactors(i)
      sqrt2 += factors(i) * factors(i)
    }

    (product / (math.sqrt(sqrt1.toDouble) * math.sqrt(sqrt2.toDouble)).toDouble, candFactors.dot(factors))
  }

  def denVec = udf((features:mutable.WrappedArray[Float]) => {
    val doubleArray = mutable.ArrayBuffer[Double]()
    for (f <- features) {
      doubleArray.append(f.toDouble)
    }

    Vectors.dense(doubleArray.toArray[Double])
  })

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

    val itemWithType = itemFactors.withColumn("featuresVec", denVec($"features"))
      .join(mixedIndex, Seq("id"), "left")
      .na.fill("", Seq("idtype"))


    println("itemWithType schema:")
    itemWithType.printSchema()
    itemWithType.show(10, false)

    val normalizer = new Normalizer()
      .setInputCol("featuresVec")
      .setOutputCol("normFeatures")
      .setP(1.0)
    val itemWithTypeNorm = normalizer.transform(itemWithType)
    println("Norm schema...")
    itemWithTypeNorm.printSchema()
    itemWithTypeNorm.show(10, false)

    val itemVideoData = itemWithTypeNorm.filter($"idtype".endsWith("-video"))
    println("sample2")
    itemVideoData.show(10, false)
    val itemDataBuff = itemVideoData.collect()
    println("itemDataBuff size:" + itemDataBuff.size)
    val itemFactorsIdtypeM = getItemFactorM(itemDataBuff)
    print ("itemFactorsIdtypeM size:" + itemFactorsIdtypeM.size)
    val itemFactorsMBroadcast = sc.broadcast[mutable.HashMap[Int, (DenseVector[Double], String)]](itemFactorsIdtypeM)

    val simTagData = itemWithTypeNorm.sample(false, 0.00001).map(row => {
      val id = row.getAs[Int]("id")
      val idtype = row.getAs[String]("idtype")
      val factors = row.getAs[org.apache.spark.ml.linalg.DenseVector]("normFeatures")
      println("normFeatures")
      println(factors)
      val (simNameScoreList1, simNameScoreList2) = sim(itemFactorsMBroadcast, DenseVector(factors.toArray))
      val topSimNameScoreList1 = mutable.ArrayBuffer[String]()
      val topSimNameScoreList2 = mutable.ArrayBuffer[String]()
      for (i <- 0 until 100) {
        if (i < topSimNameScoreList1.size) {
          topSimNameScoreList1.append(simNameScoreList1(i)._2 + ":" + simNameScoreList1(i)._1._1)
        }
        if (i < simNameScoreList2.size) {
          topSimNameScoreList2.append(simNameScoreList2(i)._2 + ":" + simNameScoreList2(i)._1._2)
        }
      }
      // tag \t simtag:score,simtag:score...
      idtype + "\t" + topSimNameScoreList1.mkString(",") + "\t" + topSimNameScoreList2.mkString(",")
    })

    simTagData.repartition(10).write.text(outputPath)

  }
}