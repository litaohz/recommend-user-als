package com.netease.music.recommend.scala.feedflow.userbased

import java.io.{BufferedReader, InputStreamReader}
import java.util.Date

import com.netease.music.recommend.scala.feedflow.mainpage.GetMainpageRcmdFromResort.{isValid, parseIds}
import com.netease.music.recommend.scala.feedflow.tag.SimItemBySimtag.getVideoPredM
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by hzlvqiang on 2018/1/22.
  */
object UserVectorRcmd {

  def seq2Vec = udf((features:Seq[Float]) => Vectors.dense(features.map(_.toDouble).toArray[Double]))

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate
    val options = new Options
    options.addOption("user_vector_cluster", true, "user_vector_cluster directory")
    options.addOption("user_pref_list", true, "user_pref_list directory")
    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("user_rced_video_from_event", true, "user_rced_video_from_event")
    options.addOption("Music_VideoRcmdMeta", true, "video_input")
    options.addOption("video_feature", true, "video_feature")
    options.addOption("videoClickrate", true, "videoClickrate")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    println("读取视频特征数据...")
    val inputVideoFeatrue = cmd.getOptionValue("video_feature")
    println("inputVideoFeatrue:" + inputVideoFeatrue)
    val viter = spark.read.parquet(inputVideoFeatrue).toDF().select("videoId", "vType", "rawPrediction").collect()
    // println("vparray size:" + vparray.)
    val videoPredM = getVideoPredM(viter)
    //val videoPredM = getVideoPredM(viter)
    println("1742243 pred:" + videoPredM.get("1742243"))
    println("1746123 pred:" + videoPredM.get("1746123"))
    println("vieoPrefM size:" + videoPredM.size)
    val videoPredMBroad = sc.broadcast(videoPredM)

    println("加载有效视频数据")
    val validVids = mutable.HashSet[String]()
    getVideoInfoM(cmd.getOptionValue("Music_VideoRcmdMeta"), validVids)
    println("validVids size:" + validVids.size)
    if (validVids.size <= 10) {
      System.exit(0)
    }
    val validVidsBroad = sc.broadcast(validVids)

    println("加载曝光数据")
    val vidImpressM = mutable.HashMap[String, Int]()
    val idClickData = spark.read.parquet(cmd.getOptionValue("videoClickrate"))
      .filter($"vType" === "video")
      .filter($"clickrateType" === "flow")
      .select("videoId", "clickrate30days").collect()
    val idImpM = getVideoImpressM(idClickData, validVids)
    println("idImpM size:" + idImpM.size)
    println("1742243 logimp = " + idImpM.get("1742243"))
    println("1746123 logimp = " + idImpM.get("1746123"))
    val vidImpMBroad = sc.broadcast(idImpM)

    val userVectorData = spark.read.parquet(cmd.getOptionValue("user_vector_cluster")) //|  id|            features| userIdStr|prediction|

    println("加载用户偏好视频")
    val userPrefVideoData = spark.read.textFile(cmd.getOptionValue("user_pref_list"))
      .map(line => {
        // uid \t vid:-type:score,vid-type:score
        val uv = line.split("\t", 2)
        val uid = uv(0)
        val vs = uv(1)
        (uid, vs)
      }).toDF("userIdStr", "prefVideoWts")
    println("userPrefVideoData cnt:" + userPrefVideoData.count())

    println("加载用户已经推荐数据1...")
    val userRcedVideosData1 = spark.read.textFile(cmd.getOptionValue("user_rced_video")).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userId", "rcedVideos1")
    println("加载用户已经推荐数据2...")
    val userRcedVideosData2 = spark.read.textFile(cmd.getOptionValue("user_rced_video")).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userId", "rcedVideos2")

    val userRcedVideosData = userRcedVideosData1.join(userRcedVideosData2, Seq("userId"), "outer")
      .map(row => {
        val rcedVideos = mutable.HashSet[String]()
        val userId = row.getAs[String]("userId")
        val rcedVideos1 = row.getAs[String]("rcedVideos1")
        if (rcedVideos1 != null) {
          for (vid <- rcedVideos1.split(",")) {
            rcedVideos.add(vid)
          }
        }
        val rcedVideos2 = row.getAs[String]("rcedVideos2")
        if (rcedVideos2 != null) {
          for (vid <- rcedVideos2.split(",")) {
            rcedVideos.add(vid)
          }
        }
        (userId, rcedVideos.toArray[String])
      }).toDF("userIdStr", "rcedVideos")
    println("userRcedVideosData cnt:" + userRcedVideosData.count())

    val clusterCandsData = userVectorData.join(userPrefVideoData, Seq("userIdStr"), "left")
      .map(row => {
        val uid = row.getAs[String]("userIdStr")
        val clusterId = row.getAs[Int]("prediction")
        val prefVideoWts = row.getAs[String]("prefVideoWts")
        //val rcedVideos = row.getAs[String]("rcedVideos")
        (clusterId, (uid, prefVideoWts))
      }).rdd.groupByKey()
      .flatMap({case (key, values) =>
        val vidWtM = mutable.HashMap[String, Float]()
        val iter = values.iterator
        var uidSet = mutable.HashSet[String]()
        // TODO 处理10000以上情况
        while (iter.hasNext && uidSet.size < 10000) {
          val value = iter.next()
          val uid = value._1

          uidSet.add(uid)
          val prefVideoWts = value._2
          if (uid.equals("359792224")) {
            println("prefVideoWts for 359792224")
            println(prefVideoWts)
          }
          if (prefVideoWts != null) {
            for (prefVideoWt <- prefVideoWts.split(",")) {
              val vidtypeWt = prefVideoWt.split(":")
              var vid = vidtypeWt(0)
              if (vid.endsWith("-video")) {
                vid = vid.split("-")(0)
              }
              if (validVidsBroad.value.contains(vid)) {
                val wt = vidtypeWt(1).toFloat
                vidWtM.put(vid, vidWtM.getOrElse(vid, 0F) + wt)
              }
            }
          }
        }
        // TODO 除以整体偏好，消除热门
        val vidWtArray1 = vidWtM.toArray[(String, Float)].sortWith(_._2 > _._2)
        val vidWtM2 = mutable.HashMap[String, Float]()
        val vidWtM3 = mutable.HashMap[String, Float]()
        val cands1 = mutable.HashSet[String]()
        // TODO 阈值限定
        for (i <- 0 until vidWtArray1.length * 1/2) {
          if (vidWtArray1(i)._2 > 40) {
            val vid = vidWtArray1(i)._1
            cands1.add(vid)
            // pred * wt
            vidWtM2.put(vidWtArray1(i)._1, (Math.sqrt(vidWtArray1(i)._2) * videoPredMBroad.value.getOrElse(vid, 0.2)).toFloat)
            vidWtM3.put(vidWtArray1(i)._1, (Math.sqrt(vidWtArray1(i)._2) / vidImpMBroad.value.getOrElse(vid, 2.0)).toFloat)
          }
        }

        val vidWtArray2 = vidWtM2.toArray[(String, Float)].sortWith(_._2 > _._2)
        val cands2 = mutable.ArrayBuffer[String]()
        for (i <- 0 until vidWtArray2.length) {
          cands2.append(vidWtArray2(i)._1)
        }

        val vidWtArray3 = vidWtM3.toArray[(String, Float)].sortWith(_._2 > _._2)
        val cands3 = mutable.ArrayBuffer[String]()
        for (i <- 0 until vidWtArray3.length) {
          cands3.append(vidWtArray3(i)._1)
        }

        if (uidSet.contains("359792224")) {
          println("359792224 candidates1:")
          for (vw <- vidWtArray1) {
            println(vw)
          }
          //println("cands1 size:" + cands1.size)
          println("359792224 candidates2:")
          for (vw <- vidWtArray2) {
            println(vw)
          }

          println("359792224 candidates3:")
          for (vw <- vidWtArray3) {
            println(vw)
          }
        }

        val result = mutable.ArrayBuffer[(String, Array[String], Array[String], Array[String])]()
        for (uid <- uidSet) {
          // uidCandsM.put(uid, array)
          result.append((uid, cands1.toArray[String], cands2.toArray[String], cands3.toArray[String]))
        }
        result
      }).toDF("userIdStr", "candidatesVideos1", "candidatesVideos2", "candidatesVideos3")

    //println("clusterCandsData cnt:" + clusterCandsData.count())

    println ("根据中心类推荐...")
    val usreRcmdsData = clusterCandsData.join(userRcedVideosData, Seq("userIdStr"), "left")
        .flatMap(row => {
        val uid = row.getAs[String]("userIdStr")
        val candidatesVideos1 = row.getAs[Seq[String]]("candidatesVideos1")
        val rcedVideos = mutable.HashSet[String]()
        val rcedVseq = row.getAs[Seq[String]]("rcedVideos")
        if (rcedVseq != null) {
          for (rvid <- rcedVseq) {
            rcedVideos.add(rvid)
          }
        }
        val rcmds = mutable.ArrayBuffer[String]()
        // TODO 随机
        //val candPreds1 = mutable.ArrayBuffer[(String, Double)]()
        val vidIter1 = candidatesVideos1.iterator
        while (vidIter1.hasNext) {
          val vid = vidIter1.next()
          if (!rcedVideos.contains(vid)) {
            if (rcmds.size < 25) {
              rcmds.append(vid + ":video")
            }
          }
        }

        val res = mutable.ArrayBuffer[(Int, String)]()
        // 按投票
        if (rcmds.size > 0) {
          res.append((1, uid + "\t" + rcmds.mkString(",")))
        }

        // 按pred排序
        val vidIter2 = row.getAs[Seq[String]]("candidatesVideos2").iterator
        val rcmds2 = mutable.ArrayBuffer[String]()
        while (vidIter2.hasNext) {
          val vid = vidIter2.next()
          if (!rcedVideos.contains(vid)) {
            if (rcmds2.size < 25) {
              rcmds2.append(vid + ":video")
            }
          }
        }
        if (rcmds2.size > 0) {
          res.append((2, uid + "\t" + rcmds2.mkString(",")))
        }

        // 按分数
        val vidIter3 = row.getAs[Seq[String]]("candidatesVideos3").iterator
        val rcmds3 = mutable.ArrayBuffer[String]()
        while (vidIter3.hasNext) {
          val vid = vidIter3.next()
          if (!rcedVideos.contains(vid)) {
            if (rcmds3.size < 25) {
              rcmds3.append(vid + ":video")
            }
          }
        }
        if (rcmds3.size > 0) {
          res.append((3, uid + "\t" + rcmds3.mkString(",")))
        }
        res
        ////////

      }).toDF("t", "uidRcmd")

    // usreRcmdsData.show(10, false)
    println("输出推荐结果...")
    usreRcmdsData.filter($"t" === 1).select("uidRcmd").write.text(cmd.getOptionValue("output") + "/t1")
    usreRcmdsData.filter($"t" === 2).select("uidRcmd").write.text(cmd.getOptionValue("output") + "/t2")
    usreRcmdsData.filter($"t" === 3).select("uidRcmd").write.text(cmd.getOptionValue("output") + "/t3")

    // predResult.write.parquet(cmd.getOptionValue("output"))

  }

  def getVideoInfoM(inputDir: String, vailidVids: mutable.HashSet[String]) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getVideoInfoMFromFile(bufferedReader, vailidVids)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getVideoInfoMFromFile(bufferedReader, vailidVids)
      bufferedReader.close()
    }
  }
  /*
  def isValid(coverSize: String) = {
    var valid = true
    if (coverSize != null) {
      val wh = coverSize.split("\\*")
      if (wh.length >= 2) {
        val rate = wh(0).toDouble/wh(1).toDouble
        if (wh(0).toInt < 540 || wh(1).toInt < 300 ||  rate < 1.7 || rate > 1.8) {
          valid = false
        }
      }
    }
    valid
  }*/

  def getVideoInfoMFromFile(reader: BufferedReader, validVids: mutable.HashSet[String]) = {
    val curtime: Long = new Date().getTime
    var line = reader.readLine()
    while (line != null) {
      try {
        implicit val formats = DefaultFormats
        val json = parse(line)
        val videoId = (json \ "videoId").extractOrElse[String]("")
        //val songIds = parseIds((json \ "bgmIds").extractOrElse[String](""))
        //val artistIds = parseIds((json \ "artistIds").extractOrElse[String](""))
        //val category = (json \ "category").extractOrElse[String]("").replace(",", "-")
        // var title = (json \ "title").extractOrElse[String]("推荐视频")
        //title = title.replaceAll(",", "，").replaceAll(":", "：").replaceAll("\r", "").replaceAll("\n", "").replaceAll("《", "<").replaceAll("》", ">")
        // TODO 白名单， minFLow, 分辨率

        var validV = true
        if (validV == true) {
          // "extData":"{\"updateCover\":null,\"smallFlow\":false,\"expose\":true,\"coverSize\":\"1920*1080\",\"videoSize\":\"1280*720\"}
          val extDataStr = (json \ "extData").toString
          val status = (json \ "status").toString
          val expTime = (json \ "expireTime").extractOrElse[Long](0)
          val extData = parse(extDataStr.substring(8, extDataStr.length - 1)) // TODO 去掉JString(...)
          if ("1".equals(status) && extData != null && expTime > curtime) {
            // println("extData:" + extData)
            val smallFlow = (extData \ "smallFlow").extractOrElse[Boolean](false)
            // println((extData \ "smallFlow").extractOrElse[Boolean](false))
            if (smallFlow == true) { // 小流量
              println("filt by smallFlow:" + smallFlow)
              validV = false
            } else {
              val coverSize = (extData \ "coverSize").extractOrElse[String]("")
              //println("coverSize:" + coverSize)
              validV = isValid(coverSize)
              if (validV == true) {
                val videoSize = (extData \ "videoSize").extractOrElse[String]("")
                validV = isValid(videoSize)
                if (validV == false) {
                  println("filt by videoSize:" + videoSize)
                }
              } else {
                println("filt by coverSize:" + coverSize)
              }
            }
          }
        }
        if (validV == true) {
          validVids.add(videoId)
        } else {
          println("Invalid video:" + line)
        }
      } catch {
        case ex: Exception => println("exception" + ex + ", line=" + line)
      }
      line = reader.readLine()
    }
  }

  def getVideoImpressM(idClickData: Array[Row], validVids: mutable.HashSet[String]) = {
    val idImpM = mutable.HashMap[String, Double]()
    for (idRateImpPlay <- idClickData) {
      val vid = idRateImpPlay.getAs[Long]("videoId").toString
      if (validVids.contains(vid)) {
        val rateIP = idRateImpPlay.getAs[String]("clickrate30days")
        val imp = rateIP.split(":")(1).toInt
        idImpM.put(vid, Math.sqrt(imp + 10))
      }
    }
    idImpM
  }


}
