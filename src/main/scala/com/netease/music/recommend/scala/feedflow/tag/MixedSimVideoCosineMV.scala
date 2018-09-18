package com.netease.music.recommend.scala.feedflow.tag

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
  * 计算歌曲、艺人、视频的相似视频
  */
object MixedSimVideoCosineMV {

  def vector2vector(features: linalg.DenseVector): _root_.breeze.linalg.DenseVector[Float] = {
    val floatFeatures = mutable.ArrayBuffer[Float]()
    for (f <- features.toArray) {
      floatFeatures.append(f.toFloat)
    }
    DenseVector[Float](floatFeatures.toArray[Float])
  }

  def getItemFactorM(rows: Array[Row]) = {
    val itemFactorM = mutable.HashMap[String, DenseVector[Float]]()
    for (row <- rows) {
      val idtype = row.getAs[String]("idtype")
      val features = row.getAs[org.apache.spark.ml.linalg.DenseVector]("normFeatures")
      itemFactorM.put(idtype, vector2vector(features))
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
      val xy = candFacotrs.dot(factors)
      //val (scoreDot, scoreDotconsine) = simScoreDotConsine(candFacotrs, factors)
      scoreNameListDot.append(Tuple2(xy, idtype))
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

  def getValidVideosList(strings: Array[String]) = {
    val validVideos = new mutable.HashSet[String]()
    for(str <- strings) {
      implicit val formats = DefaultFormats
      val json = parse(str)
      val videoId = (json \ "videoId").extractOrElse[String]("")
      validVideos.add(videoId + "-video")
    }
    validVideos
  }

  def getValidVideos(inputDir: String) = {
    val validVideos = new mutable.HashSet[String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir))) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getValidVideosFromFile(bufferedReader, validVideos)
        }
      }
    }
    validVideos
  }

  def getValidVideosFromFile(reader: BufferedReader, validVideos: mutable.HashSet[String]): Unit = {
    var line = reader.readLine()
    while (line != null) {
      //try {
      implicit val formats = DefaultFormats
      val json = parse(line)
      val videoId = (json \ "videoId").extractOrElse[String]("")
      validVideos.add(videoId + "-video")
    }
  }

  def getValidIdtypes(rows: Array[Row]) = {
    val validvids = mutable.HashSet[String]()
    for (row <- rows) {
      validvids.add(row.getAs[Long]("vid").toString + "-video")
    }
    validvids
  }

  def isValid(validVids: mutable.HashSet[String]) = udf((idtype: String) => {
    var isValid = false
    if (validVids.contains(idtype)) {
      isValid = true
    }
    isValid
  })

  def double2Float(doubles: mutable.WrappedArray[Double]) = {
    val floats = mutable.ArrayBuffer[Float]()
    for (d <- doubles) {
      floats.append(d.toFloat)
    }
    floats.toArray[Float]
  }

  def normVector = udf((features:mutable.WrappedArray[Float]) => {
    val doubleArray = mutable.ArrayBuffer[Double]()
    var sumsqrt = 0.0
    for (f <- features) {
      sumsqrt += Math.pow(f, 2)
    }
    sumsqrt = Math.sqrt(sumsqrt)
    for (f <- features) {
      doubleArray.append(f.toDouble / sumsqrt)
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
    options.addOption("Music_VideoRcmdMeta", true, "input")
    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val inputItemPath = cmd.getOptionValue("input_item")
    val outputPath = cmd.getOptionValue("output")
    println("加载视频meta，相似视频限定在这些视频内")
    val videoRcmdMetaTable = spark.read.json(cmd.getOptionValue("Music_VideoRcmdMeta"))
      .withColumn("vid", $"videoId")
      .withColumn("smallFlow", getJsonValue("smallFlow")($"extData"))
      .filter(!$"smallFlow")
    val validVids = getValidIdtypes(videoRcmdMetaTable.collect())
    val validVidsBroad = sc.broadcast(validVids)
    println("validVids size:" + validVids.size)
    println("validVids head:" + validVids.head)

    val itemFactors = spark.read.parquet(inputItemPath)
    println("itemFactors schema:")
    itemFactors.printSchema()
    itemFactors.show(10, false)

    val normalizer = new Normalizer()
      .setInputCol("featuresVec")
      .setOutputCol("normFeatures")
      .setP(1.0)
    println("对itemFactor向量进行归一处理")
    val itemFactorsNorm = itemFactors.dropDuplicates().withColumn("normFeatures", normVector($"features"))
    println("Norm schema...")
    itemFactorsNorm.printSchema()
    itemFactorsNorm.show(10, false)

    println("相似视频限定在有效视频内")
    val validItemFactorsNormX = itemFactorsNorm.filter($"idtype".endsWith("-mv"))
    val validItemFactorsCnt = validItemFactorsNormX.count()
    println("validItemFactorsNormX count:" + validItemFactorsCnt)
    validItemFactorsNormX.show(10, false)
    if (validItemFactorsCnt < 5000000) {
      val validItemFactorsNormM = getItemFactorM(validItemFactorsNormX.collect())
      print("itemFactorsIdtypeM size:" + validItemFactorsNormM.size)
      val itemFactorsMBroadcast = sc.broadcast[mutable.HashMap[String, DenseVector[Float]]](validItemFactorsNormM)


      val simTagData = itemFactorsNorm.map(row => {
        val idtype = row.getAs[String]("idtype")
        // val factors = DenseVector[Float](double2Float(row.getAs[mutable.WrappedArray[Double]]("normFeatures")))
        val factors = vector2vector(row.getAs[org.apache.spark.ml.linalg.DenseVector]("normFeatures"))
        val simNameScoreListDot = simDot(itemFactorsMBroadcast, factors)
        val topSimNameScoreListDot = mutable.ArrayBuffer[String]()
        for (i <- 0 until 60) {
          topSimNameScoreListDot.append(simNameScoreListDot(i)._2 + ":" + simNameScoreListDot(i)._1)
        }
        // tag \t simtag:score,simtag:score...
        idtype + "\t" + topSimNameScoreListDot.mkString(",")
      })
      simTagData.repartition(10).write.text(outputPath)
    }
  }
}