package com.netease.music.recommend.scala.feedflow.tag

import java.io.{BufferedReader, InputStreamReader}
import java.util.Date

import breeze.linalg.DenseVector
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable;

/**
  * word2vec
  */
object MixedSimVideoUnionFloat {

  def getItemFactorM(rows: Array[Row]) = {
    val itemFactorM = mutable.HashMap[String, DenseVector[Float]]()
    for (row <- rows) {
      val idtype = row.getAs[String]("idtype")
      val features = row.getAs[mutable.WrappedArray[Float]]("features").toArray[Float]
      itemFactorM.put(idtype, DenseVector[Float](features))
    }
    itemFactorM
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


    val itemFactors = spark.read.parquet(inputItemPath)
    println("itemFactors schema:")
    itemFactors.printSchema()
    itemFactors.show(10, false)

    /*
    val validVideos = getValidVideosList(spark.read.textFile(cmd.getOptionValue("Music_VideoRcmdMeta")).collect())
    println("validVideos size:" + validVideos.size)
    val validVideosBroad = sc.broadcast(validVideos)*/

    val itemFactorsIdtypeM = getItemFactorM(itemFactors.filter($"idtype".endsWith("-video")).collect())
    print ("itemFactorsIdtypeM size:" + itemFactorsIdtypeM.size)
    val itemFactorsMBroadcast = sc.broadcast[mutable.HashMap[String, DenseVector[Float]]](itemFactorsIdtypeM)
    // TODO
    val simTagData = itemFactors.sample(false, 0.001).map(row => {
      val idtype = row.getAs[String]("idtype")
      val factors = DenseVector[Float](row.getAs[mutable.WrappedArray[Float]]("features").toArray[Float])
      val (simNameScoreListDot,simNameScoreListDotCosine, simNameScoreListCosine)  = sim(itemFactorsMBroadcast, factors)
      val topSimNameScoreListDot = mutable.ArrayBuffer[String]()
      val topSimNameScoreListDotCosine = mutable.ArrayBuffer[String]()
      val topSimNameScoreListCosine = mutable.ArrayBuffer[String]()
      for (i <- 0 until 50) {
        topSimNameScoreListDot.append(simNameScoreListDot(i)._2 + ":" + simNameScoreListDot(i)._1)
        topSimNameScoreListDotCosine.append(simNameScoreListDotCosine(i)._2 + ":" + simNameScoreListDotCosine(i)._1)
        topSimNameScoreListCosine.append(simNameScoreListCosine(i)._2 + ":" + simNameScoreListCosine(i)._1)
      }
      // tag \t simtag:score,simtag:score...
      idtype + "\t" + topSimNameScoreListDot.mkString(",") + "\t" + topSimNameScoreListDotCosine.mkString(",") + "\t" + topSimNameScoreListCosine.mkString(",")
    })

    simTagData.repartition(10).write.text(outputPath)

  }
}