package com.netease.music.recommend.scala.feedflow.follow

import java.io.{BufferedReader, InputStreamReader}
import java.util.Date

import breeze.linalg.DenseVector
import com.netease.music.recommend.scala.feedflow.GetVideoPool.getJsonValue
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable

/**
  * word2vec
  */
object MixedSimUserNormDot {

  def denVec = udf((features:mutable.WrappedArray[Float]) => {
    val doubleArray = mutable.ArrayBuffer[Double]()
    for (f <- features) {
      doubleArray.append(f.toDouble)
    }

    Vectors.dense(doubleArray.toArray[Double])
  })

  def vector2vector(features: linalg.DenseVector): _root_.breeze.linalg.DenseVector[Float] = {
    val floatFeatures = mutable.ArrayBuffer[Float]()
    for (f <- features.toArray) {
      floatFeatures.append(f.toFloat)
    }
    DenseVector[Float](floatFeatures.toArray[Float])
  }

  def getUserFactorM(rows: Array[Row], validUsers: mutable.HashSet[String]) = {
    val itemFactorM = mutable.HashMap[String, DenseVector[Float]]()
    for (row <- rows) {
      val id = row.getAs[String]("userIdStr")
      if (validUsers.contains(id)) {
        val features = row.getAs[org.apache.spark.ml.linalg.DenseVector]("normFeatures")
        itemFactorM.put(id, vector2vector(features))
      }
      /*
      val features = row.getAs[mutable.WrappedArray[Double]]("normFeatures")
      val floatFeatures = mutable.ArrayBuffer[Float]()
      for (feat <- features) {
        floatFeatures.append(feat.toFloat)
      }
      itemFactorM.put(idtype, DenseVector[Float](floatFeatures.toArray[Float]))
      */
    }
    itemFactorM
  }

  def simDot(itemFactorsMBroadcast: Broadcast[mutable.HashMap[String, DenseVector[Float]]], factors: DenseVector[Float]) = {
    val scoreNameListDot = mutable.ArrayBuffer[Tuple2[Float, String]]()

    for ((idtype, candFacotrs) <- itemFactorsMBroadcast.value) {
      // val xy = candFacotrs.dot(factors)
      val (scoreDot, scoreDotconsine) = simScoreDotConsine(candFacotrs, factors)
      scoreNameListDot.append(Tuple2(scoreDotconsine, idtype))
    }
    //scoreNameList.sortBy[Float](t2 => t2._1, false)
    val sortedNameListDot = scoreNameListDot.sortBy[Float](-_._1)

    sortedNameListDot
  }

  def sim(itemFactorsMBroadcast: Broadcast[mutable.HashMap[String, DenseVector[Float]]], factors: DenseVector[Float]) = {
    val scoreNameListDot = mutable.ArrayBuffer[Tuple2[Float, String]]()
    val scoreNameListDotCosine = mutable.ArrayBuffer[Tuple2[Float, String]]()
    var date = new Date()
    println("dot start:" + date.getTime)
    for ((idtype, candFacotrs) <- itemFactorsMBroadcast.value) {
      val (scoreDot, scoreDotconsine) = simScoreDotConsine(candFacotrs, factors)
      scoreNameListDot.append(Tuple2(scoreDot, idtype))
      scoreNameListDotCosine.append(Tuple2(scoreDotconsine, idtype))
    }
    date = new Date()
    println("dot end:" + date.getTime)
    //scoreNameList.sortBy[Float](t2 => t2._1, false)
    val sortedNameListDot = scoreNameListDot.sortBy[Float](-_._1)
    val sortedNameListDotCosine = scoreNameListDotCosine.sortBy[Float](-_._1)

    // for 循环排序
    val scoreNameListCosine = mutable.ArrayBuffer[Tuple2[Float, String]]()
    date = new Date()
    println("cosine start:" + date.getTime)
    for ((idtype, candFacotrs) <- itemFactorsMBroadcast.value) {
      val cosine = simScoreConsine(candFacotrs, factors)
      scoreNameListCosine.append(Tuple2(cosine, idtype))
    }
    date = new Date()
    println("cosine end:" + date.getTime)
    val sortedNameListCosine = scoreNameListCosine.sortBy[Float](-_._1)

    (sortedNameListDot, sortedNameListDotCosine, sortedNameListCosine)
  }

  def simScoreDotConsine(candFactors: DenseVector[Float], factors: DenseVector[Float]) = {

    val xy = candFactors.dot(factors)
    val xx = Math.sqrt(candFactors.dot(candFactors))
    val yy = Math.sqrt(factors.dot(factors))
    (xy.toFloat, (xy / (xx * yy)).toFloat)
  }

  def simScoreConsine(candFactors: DenseVector[Float], factors: DenseVector[Float]) = {

    var product =  0.0
    var sqrt1 = 0.0
    var sqrt2 = 0.0
    for (i <- 0 until candFactors.length) {
      product += candFactors(i) * factors(i)
      sqrt1 += candFactors(i) * candFactors(i)
      sqrt2 += factors(i) * factors(i)
    }
    // dot cosine
    (product / (math.sqrt(sqrt1.toFloat) * math.sqrt(sqrt2.toFloat)).toFloat).toFloat

  }


  def isValid(validVids: mutable.HashSet[String]) = udf((idtype: String) => {
    if (validVids.contains(idtype)) {
      true
    } else {
      false
    }
  })

  def double2Float(doubles: mutable.WrappedArray[Double]) = {
    val floats = mutable.ArrayBuffer[Float]()
    for (d <- doubles) {
      floats.append(d.toFloat)
    }
    floats.toArray[Float]
  }

  def getValidUsers(rows: Array[Row]) = {
    val validUsers = mutable.HashSet[String]()
    for (row <- rows) {
      val userid = row.getAs[String](0).split("\t")(0)
      validUsers.add(userid)
      println("userid:" + userid)
    }
    validUsers
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
    options.addOption("whitelist_user", true, "input")
    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val inputItemPath = cmd.getOptionValue("input_item")
    val outputPath = cmd.getOptionValue("output")
    println("加载视频meta，相似视频限定在这些视频内")
    /*
    val whitelistUser = spark.read.json(cmd.getOptionValue("whitelist_user"))
      .withColumn("vid", $"videoId")
      .withColumn("smallFlow", getJsonValue("smallFlow")($"extData"))
      .filter(!$"smallFlow")
      */
    val validUsers = getValidUsers(spark.read.json(cmd.getOptionValue("whitelist_user")).collect())
    println("validUser id num:" + validUsers.size)

    val userFactors = spark.read.parquet(inputItemPath)
    println("itemFactors schema:")
    userFactors.printSchema()
    userFactors.show(10, false)

    val normalizer = new Normalizer()
      .setInputCol("featuresVec")
      .setOutputCol("normFeatures")
      .setP(1.0)
    println("对itemFactor向量进行归一处理")
    val userFactorsNorm = normalizer.transform(userFactors.withColumn("featuresVec", denVec($"features"))).select("userIdStr", "normFeatures")

    println("Norm schema...")
    userFactorsNorm.printSchema()
    userFactorsNorm.show(10, false)

    println("相似视频限定在有效视频内")

    val validUserFactorsNormM = getUserFactorM(userFactorsNorm.filter($"userIdStr".isin(validUsers.toArray[String] : _*)).collect(), validUsers)
    print ("itemFactorsIdtypeM size:" + validUserFactorsNormM.size)
    val itemFactorsMBroadcast = sc.broadcast[mutable.HashMap[String, DenseVector[Float]]](validUserFactorsNormM)


    val simTagData = userFactorsNorm.map(row => {
      val srcId = row.getAs[String]("userIdStr")
      // val factors = DenseVector[Float](double2Float(row.getAs[mutable.WrappedArray[Double]]("normFeatures")))
      val factors = vector2vector(row.getAs[org.apache.spark.ml.linalg.DenseVector]("normFeatures"))
      val simNameScoreListDot = simDot(itemFactorsMBroadcast, factors)
      val topSimNameScoreListDot = mutable.ArrayBuffer[String]()
      for (i <- 0 until 60) {
        topSimNameScoreListDot.append(simNameScoreListDot(i)._2 + ":" + simNameScoreListDot(i)._1)
      }
      // tag \t simtag:score,simtag:score...
      srcId + "\t" + topSimNameScoreListDot.mkString(",")
    })
    simTagData.repartition(10).write.text(outputPath)
  }
}