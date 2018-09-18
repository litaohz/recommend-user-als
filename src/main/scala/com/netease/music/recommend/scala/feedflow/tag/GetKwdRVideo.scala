package com.netease.music.recommend.scala.feedflow.tag

import com.netease.music.recommend.scala.feedflow.mainpage.GetMainpageRcmdResource.usefulTagtypeS
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable


/**
  * Created by hzzhangjunfei1 on 2018/3/16.
  */

// recalltype:
// 1: mCF
// 2: kwd
// 3: als
object GetKwdRVideo {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("downgrade", true, "input directory")
    options.addOption("videoTag", true, "input directory")
    options.addOption("mCF", true, "input directory")
//    options.addOption("alsCF", true, "input directory")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    val downgrade = cmd.getOptionValue("downgrade")
    val videoTag = cmd.getOptionValue("videoTag")
    val mCFInput = cmd.getOptionValue("mCF")
//    val alsCFInput = cmd.getOptionValue("alsCF")
    val output = cmd.getOptionValue("output")

    import spark.implicits._

    val vidDowngradeM = spark.read.parquet(downgrade)
      .rdd
      .map(row => (row.getAs[Long](0), row.getAs[Double](1)))
      .collect()
      .toMap[Long, Double]
    val vidDowngradeM_broad = spark.sparkContext.broadcast(vidDowngradeM)

    // 打标
    val kwdTable = spark.sparkContext.textFile(videoTag)
      .flatMap{line =>
        // 1000052  \t  video  \t  薛之谦_ET,演员_ET
        val info = line.split("\t")
        val videoId = info(0).toLong
        val vType = info(1)
        val kwds = mutable.ArrayBuffer[String]()
        if (info.length >= 3 && vType.equals("video")) {
          for (kwdType <- info(2).split(",")) {
            val kwdtype = kwdType.split("_")
            if (kwdtype.length >= 2) {
              val kwd = kwdtype(0)
              // 获取所有以"ET", "AT", "WK", "PT", "TT"为标记的tag相关信息
              if (usefulTagtypeS.contains(kwdtype(1))) {
                if (!kwds.contains(kwd))
                  kwds.append(kwd)
              }
            }
          }
        }
        kwds.map{kwd => (kwd, videoId)}
      }.toDF("kwd", "videoId")
      .groupBy("kwd")
      .agg(
        collect_set($"videoId").as("kwdVideoS")
      )
    // merged_cf_sim_kwd_video
    val mCFTable = spark.sparkContext.textFile(mCFInput)
      .filter{line =>
        val info = line.split("\t")
        info.length >= 2 && !info(1).equals("null")
      }
      .map{line =>
        val info = line.split("\t")
        val kwd = info(0).split("-")(0)
        val videos = info(1).split(",")
          .map { item =>
            val vInfo = item.split(":|-")
            val videoId = vInfo(0).toLong
            val vType = vInfo(1)
            val recalltypes = vInfo(3)
            var remain = false
            if (vType.equals("video") /*&&
              (recalltypes.contains("&") || usefulCFRecalltypeS.contains(recalltypes))  // 只保留有效的cf recalltypes*/
            )
              remain = true
            (videoId+":"+recalltypes, remain)
          }
          .filter(tup => tup._2)
          .map(tup => tup._1)
        (kwd, videos)
      }.toDF("kwd", "mCFVInfoS")
    /*// als
    val alsCFTable = spark.sparkContext.textFile(alsCFInput)
      .filter{line =>
        val info = line.split("\t")
        info.length >= 2 && info(0).endsWith("-kwd")
      }
      .map{line =>
        val info = line.split("\t")
        val kwd = info(0).split("-")(0)
        val videos = info(1).split(",")
          .map { item =>
            val vInfo = item.split(":|-")
            val videoId = vInfo(0).toLong
            val vType = vInfo(1)
            var remain = false
            if (vType.equals("video")) // 只保留有效的cf recalltypes
              remain = true
            (videoId, remain)
          }
          .filter(tup => tup._2)
          .map(tup => tup._1)
        (kwd, videos)
      }.toDF("kwd", "alsVideoS")*/

    val mergedTable = mCFTable
      .join(kwdTable, Seq("kwd"), "outer")
      //1.join(alsCFTable, Seq("kwd"), "outer")
      .map{line =>
        val kwd = line.getString(0)

        val mCFVInfoS = line.getAs[Seq[String]]("mCFVInfoS")
        val kwdVideoS = line.getAs[Seq[Long]]("kwdVideoS")
        //val alsVideoS = line.getAs[Seq[Long]]("alsVideoS")

        val vidDowngradeM = vidDowngradeM_broad.value
        val addedS = scala.collection.mutable.Set[Long]()
        val videosRet = new scala.collection.mutable.ArrayBuffer[String]()

        if (mCFVInfoS != null) {
          val cfRet = new scala.collection.mutable.ArrayBuffer[(Long, Double)]()
          mCFVInfoS.foreach { vidRecalltypes =>
            val vInfo = vidRecalltypes.split(":")
            val vid = vInfo(0).toLong
            val recallCnt = vInfo(1).split("&").length
            if (vidDowngradeM.contains(vid) && addedS.add(vid)) {
              val score = vidDowngradeM(vid)
              cfRet.append((vid , recallCnt.toDouble))
            }
          }
          if (cfRet.size > 0) {
            //val cfSize = cfRet.length
            //val endIndex = if (cfSize > 20) 20 else cfSize
            //videosRet.appendAll(cfRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:mcf").slice(0, endIndex))
            // 默认相似度排序
            videosRet.appendAll(cfRet.map(tup => tup._1 + ":video:mcf"))/*.slice(0, endIndex))*/
          }
        }
        if (kwdVideoS != null) {
          val kwdRet = new scala.collection.mutable.ArrayBuffer[(Long, Double)]()
          kwdVideoS.foreach { vid =>
            if (vidDowngradeM.contains(vid) && addedS.add(vid)) {
              val score = vidDowngradeM(vid)
              kwdRet.append((vid , score))
            }
          }
          if (kwdRet.size > 0) {
            /*val labelSize = kwdRet.length
            val endIndex = if (labelSize > 20) 20 else labelSize*/
            videosRet.appendAll(kwdRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:label"))/*.slice(0, endIndex))*/
          }
        }
//        if (alsVideoS != null) {
//          val alsRet = new scala.collection.mutable.ArrayBuffer[(Long, Double)]()
//          alsVideoS.foreach { vid =>
//            if (vidDowngradeM.contains(vid) && addedS.add(vid)) {
//              val score = vidDowngradeM(vid)
//              alsRet.append((vid, score))
//            }
//          }
//          if (alsRet.size > 0) {
//            val alsSize = alsRet.length
//            val endIndex = if (alsSize > 20) 20 else alsSize
//            //println("zhp:" + alsSize + "," + alsRet.length + "," + alsRet.size + "," + endIndex + "," + kwd)
//            //videosRet.appendAll(alsRet.sortWith(_._2 > _._2).map(tup => tup._1 + ":video:als").slice(0, endIndex))
//            // 默认相似度排序
//            videosRet.appendAll(alsRet.map(tup => tup._1 + ":video:als").slice(0, endIndex))
//          }
//        }
        var videoStr = "null"
        if (videosRet.length > 0)
          videoStr = videosRet.mkString(",")
        (kwd, videoStr)
      }.toDF("kwd", "videos")
      .filter($"videos"=!="null")
    //.cache
    mergedTable.write.option("sep", "\t").csv(output)

  }
}
