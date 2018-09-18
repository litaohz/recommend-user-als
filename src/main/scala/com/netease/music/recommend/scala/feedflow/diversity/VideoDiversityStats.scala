package com.netease.music.recommend.scala.feedflow.diversity

import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector
import breeze.linalg._
import scala.collection.mutable

/**
  * Created by hzlvqiang on 2018/1/26.
  */
object VideoDiversityStats {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("video_vec", true, "input")
    options.addOption("user_video_impress", true, "input")
    options.addOption("user_video_play", true, "input")
    options.addOption("output", true, "output")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    println("Load video_vec...")
    val videoVecM = mutable.HashMap[String, DenseVector[Double]]()
    getidtypeVecM(new Path(cmd.getOptionValue("video_vec")), videoVecM)
    println("videoVecM size:" + videoVecM.size)
    val videoVecMBroad = sc.broadcast(videoVecM)

    println("Load user video...")
    val userVideoImpressData = spark.read.parquet(cmd.getOptionValue("user_video_impress"))
    val userVideoPlayData = spark.read.parquet(cmd.getOptionValue("user_video_play"))
    val userVideoImpresssData = userVideoImpressData.map(row => {
      // videoId  vType userId  logTime playRank  tags  allTagsSize
      val userId = row.getAs[Long]("userId")
      val videoId = row.getAs[Long]("videoId")
      val vType = row.getAs[String]("vType")
      val idType = videoId.toString + ":" + vType
      val logtime = row.getAs[Long]("logTime")
      (userId, idType + "\t" + logtime.toString)
    }).rdd.groupByKey.map({case(key, values)=>
      //val varray = mutable.ArrayBuffer[String]()
      //varray.appendAll(values)
      (key, values.toSeq)
    }).toDF("userId", "impressSeq")
    val userVideoPlaysData = userVideoPlayData.map(row => {
      // videoId  vType userId  logTime playRank  tags  allTagsSize
      val userId = row.getAs[Long]("userId")
      val videoId = row.getAs[Long]("videoId")
      val vType = row.getAs[String]("vType")
      val idType = videoId.toString + ":" + vType
      val logtime = row.getAs[Long]("logTime")
      (userId, idType + "\t" + logtime.toString)
    }).rdd.groupByKey.map({case(key, values)=>
      // (key, values.toArray[(String, Long)])
      //val varray = mutable.ArrayBuffer[String]()
      //varray.appendAll(values)
      (key, values.toSeq)
    }).toDF("userId", "playSeq")

    val userImpressPalyDist = userVideoImpresssData.join(userVideoPlaysData, Seq("userId"), "left")
      .map(row => {
        val userId = row.getAs[Long]("userId")
        val impressTimeSeqs = row.getAs[Seq[String]]("impressSeq")
        val playTimeSeqs = row.getAs[Seq[String]]("playSeq")
        val impressTimeSeqt = mutable.ArrayBuffer[(String, Long)]()
        for (its <- impressTimeSeqs) {
          val it = its.split("\t")
          impressTimeSeqt.append((it(0), it(1).toLong))
        }
        val playTimeSeqt = mutable.ArrayBuffer[(String, Long)]()
        if (playTimeSeqs != null) {
          for (pts <- playTimeSeqs) {
            val pt = pts.split("\t")
            playTimeSeqt.append((pt(0), pt(1).toLong))
          }
        }

        val sortedImpressSeq = impressTimeSeqt.sortWith(_._2 < _._2)
        val sortedPlaySeq = playTimeSeqt.sortWith(_._2 < _._2)
        if (userId == 119585) {
          println("119585 sortedPlaySeq:")
          println(sortedImpressSeq)
          println(sortedPlaySeq)

        }

        val impressDs = getDiversityScore(sortedImpressSeq, videoVecMBroad.value)
        val palyDs = getDiversityScore(sortedPlaySeq, videoVecMBroad.value)

        val impressScope = impressDs.size / 10
        val playScope = palyDs.size / 10

        // val outKey = impressScope.toString + "\t" + playScope.toString
        val outKey = playScope.toString

        (outKey, (impressDs, palyDs))
      }).rdd.groupByKey.map({ case (key, values) =>
        val impressSums = mutable.ArrayBuffer[Double]()
        val impressCnts = mutable.ArrayBuffer[Int]()
        val playSums = mutable.ArrayBuffer[Double]()
        val playCnts = mutable.ArrayBuffer[Int]()
        var cnt = 0
        for (value <- values) {
          for (i <- 0 until value._1.size) {
            if (impressSums.size <= i) {
              impressSums.append(value._1(i))
              impressCnts.append(1)
            } else {
              impressSums(i) += value._1(i)
              impressCnts(i) += 1
            }
          }

          for (i <- 0 until value._2.size) {
            if (playSums.size <= i) {
              playSums.append(value._2(i))
              playCnts.append(1)
            } else {
              playSums(i) += value._2(i)
              playCnts(i) += 1
            }
          }
          cnt += 1
        }

        for (i <- 0 until impressSums.size) {
          impressSums(i) = impressSums(i) / impressCnts(i)
        }
        for (i <- 0 until playSums.size) {
          playSums(i) = playSums(i) / playCnts(i)
        }

        key + "\t" + cnt.toString + "\t" + impressSums.mkString(",") + "\t" + playSums.mkString(",")

      })

    userImpressPalyDist.repartition(1).saveAsTextFile(cmd.getOptionValue("output"))

  }

  def getDiversityScore(idtypeTimeSeq: Seq[(String, Long)], videoVecM: mutable.HashMap[String, DenseVector[Double]]) = {
    val distanceAvSeq = mutable.ArrayBuffer[Double]()
    distanceAvSeq.append(2)
    var distanceSum = 0.0
    val seqVectors = mutable.ArrayBuffer[DenseVector[Double]]()
    val iter = idtypeTimeSeq.iterator
    while (iter.hasNext && distanceAvSeq.length < 300) {
      val (idtype, ltime) = iter.next()
      val vector = videoVecM.getOrElse(idtype, null)
      for (svector <- seqVectors) {
        val simScore = sim(svector, vector)
        if (simScore > -100.0) {
          val distance = 1 + simScore
          distanceSum += distance
          var divide = distanceAvSeq.size * (distanceAvSeq.size - 1)
          if (divide <= 0) {
            divide = 1
          }
          distanceAvSeq.append(2 * distanceSum / divide)
        }
      }
      seqVectors.append(vector)
    }
    distanceAvSeq
  }
  def sim(svector: DenseVector[Double], vector: DenseVector[Double]) = {
    var simScore = -100.0
    if (svector != null && vector != null) {
      simScore = svector.dot(vector)
    }
    simScore
  }

  def getidtypeVecM(path: Path, idtypeVecM: mutable.HashMap[String, DenseVector[Double]]) = {
    //val idtypeVecM = mutable.HashMap[String, DenseVector[Double]]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    // val path = new Path(videoVecInput)
    if (hdfs.isDirectory(path) ) {
      for (subPath <- hdfs.listStatus(path)) {
        getidtypeVecMF(subPath.getPath, idtypeVecM)
      }
    }
  }

  def getidtypeVecMF(path: Path, idtypeVecM: mutable.HashMap[String, DenseVector[Double]]) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = reader.readLine()
    while (line != null) {
      // id:type \t 0.1,0.2,0.3...
      val idtypeVec = line.split("\t", 2)
      val vec = idtypeVec(1).split(",")
      val vecF = mutable.ArrayBuffer[Double]()
      for (v <- vec) {
        vecF.append(v.toDouble)
      }
      idtypeVecM.put(idtypeVec(0), DenseVector(vecF.toArray[Double]))
      line = reader.readLine()
    }
    reader.close()
  }


}
