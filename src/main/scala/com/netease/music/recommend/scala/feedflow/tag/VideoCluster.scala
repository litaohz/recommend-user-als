package com.netease.music.recommend.scala.feedflow.tag

import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import breeze.linalg._
import com.netease.music.recommend.scala.feedflow.tag.TagALS.getTagIdxM
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
/**
  *
  * 对聚类后视频，计算中心，进行索引
  *
  * Created by hzlvqiang on 2018/1/31.
  */
object VideoCluster {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    // options.addOption("video_vector", true, "input directory")
    options.addOption("video_kmeans", true, "input directory")
    options.addOption("video_tags", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    val idtypeTagsM = getVideoTagsM(cmd.getOptionValue("video_tags"))
    println("tagIdxM size:" + idtypeTagsM.size)

    val videoVectorData = spark.read.parquet(cmd.getOptionValue("video_kmeans"))

    val videoClusterData = videoVectorData.map(row => {
      val prediction = row.getAs[Int]("prediction")
      val vector = row.getAs[org.apache.spark.ml.linalg.Vector]("features")
      val idtype = row.getAs[String]("idtype")
      (prediction.toString, (idtype, vector.toArray))
    }).rdd.groupByKey.flatMap({case (key, values) =>
      // 求向量和
      var sumVector = mutable.ArrayBuffer[Double]()
      var cnt = 0
      for (value <- values) {
        cnt += 1
        if (sumVector.isEmpty) {
          sumVector.appendAll(value._2)
        } else {
          for (i <- 0 until value._2.length) {
            sumVector(i) += value._2(i)
          }
        }

      }

      // 求向量平均值，作为中心
      for (i <- 0 until sumVector.length) {
        sumVector(i) = sumVector(i) / cnt
      }
      println("av vector for:" + key)
      println(sumVector.toString())
      val avVector = DenseVector(sumVector.toArray[Double])
      // 计算距离中心最近的点，代表中心
      var maxSim = 0.0
      var centerIdtype = ""
      var centerVector = ""
      val tagCntM = mutable.HashMap[String, Int]()
      for (value <- values) {
        val sim = avVector.dot(DenseVector(value._2))
        val tags = idtypeTagsM.getOrElse(value._1, null)
        if (tags != null) {
          for (tag <- tags) {
            val cnt = tagCntM.getOrElse(tag, 0)
            tagCntM.put(tag, cnt + 1)
          }
        }
        if (sim > maxSim) {
          maxSim = sim
          centerIdtype = value._1
          centerVector = value._2.mkString(",")
        }
      }

      val sortedTagCnt = tagCntM.toArray[(String, Int)].sortWith(_._2 > _._2)
      val topnTags = mutable.ArrayBuffer[String]()
      var topn = 5
      if (sortedTagCnt.length < topn) {
        topn = sortedTagCnt.length
      }
      for (i <- 0 until topn) {
        topnTags.append(sortedTagCnt(i)._1)
      }

      val result = mutable.ArrayBuffer[String]()
      for (value <- values) {
        result.append(value._1 + "\t" + centerIdtype + "\t" + centerVector + "\t" + key + "\t" + cnt + "\t" + topnTags.mkString(","))
      }
      // idtype centeridtype centerVector cenerteId cnt
      result
    })

    videoClusterData.saveAsTextFile(cmd.getOptionValue("output"))

  }

  def getVideoTagsM(inputDir: String) = {
    val videoTagsM = mutable.HashMap[String, mutable.HashSet[String]]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir))) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        getVideoTagsMFromF(bufferedReader, videoTagsM)

      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getVideoTagsMFromF(bufferedReader, videoTagsM)
    }
    videoTagsM
  }

  def getVideoTagsMFromF(reader: BufferedReader, videoTagsM: mutable.HashMap[String, mutable.HashSet[String]]) = {
    var line = reader.readLine()
    while (line != null) {
      // id type  word_type,word_type,... title
      val ts = line.split("\t")
      val idtype = ts(0) + ":" + ts(1)
      val tags = ts(2).split(",")
      for (tag <- tags) {
        val wtype = tag.split("_")
        if (!wtype(1).equals("CC") && !wtype(1).equals("WD") && !wtype(1).equals("OT") && !wtype(1).equals("KW")) {
          val word = wtype(0)
          val tagset = videoTagsM.getOrElse(idtype, mutable.HashSet[String]())
          tagset.add(word)
          videoTagsM.put(idtype, tagset)
        }
      }
      line = reader.readLine()
    }
  }



}
