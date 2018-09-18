package com.netease.music.recommend.scala.feedflow.tag

import java.io._
import java.util

import com.netease.music.recommend.scala.feedflow.tag.SimItemByAls.getRcmd
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks._

/**
  * 根据用户偏好tag + 相似tag + tag-video 推荐video
  * Created by hzlvqiang on 2017/12/4.
  */
object SimItemByAlsWithMV {

  def getAidWtM = udf((idtypeWts: Seq[String]) => {
    val aidWtM = mutable.HashMap[String, Float]()
    for (idtypewtStr <- idtypeWts) {
      val idtypewt = idtypewtStr.split("\t")
      if (idtypewt(0).endsWith("-art")) {
        val aid = idtypewt(0).split("-")(0)
        if (aid.charAt(0).isDigit && aid.charAt(aid.size-1).isDigit) {
          val wt = Math.sqrt(aid.toFloat).toFloat
          aidWtM.put(aid, aidWtM.getOrElse(aid, 0F) + wt)
        } else {
          println("Ivalid:" + idtypewtStr )
        }
      }
    }
    map2string(aidWtM)
  })

  def comb = udf((idtype: String, rating: Double) => {
    idtype + "\t" + rating.toInt.toString
  })

  def getReason(srcprefs: mutable.HashSet[String], maxNum: Int) = {
    val iter = srcprefs.iterator
    val res = mutable.ArrayBuffer[String]()
    while (iter.hasNext && res.size < maxNum) {
      val src = iter.next()
      res.append(src)
    }
    res.mkString("&")
  }

  def map2string(prefArtIdM: mutable.HashMap[String, Float]) = {
    val res = mutable.ArrayBuffer[String]()
    for ((key, value) <- prefArtIdM) {
      res.append(key + ":" + value)
    }
    res.mkString(",")
  }
  def string2map(str: String) = {
    val idWtM = mutable.HashMap[String, Float]()
    if (str != null) {
      for (idwt <- str.split(",")) {
        val iw = idwt.split(":")
        if (iw.size >= 2) {
          idWtM.put(iw(0), iw(1).toFloat)
        }
      }
    }
    idWtM
  }

  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val options = new Options()

    options.addOption("userArtPref", true, "userArtPref")
    options.addOption("userSongPref", true, "userSongPref")
    options.addOption("userItemRating", true, "userItemRating")

    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("user_rced_video_from_event", true, "user_rced_video_from_event")

    options.addOption("sim_als", true, "sim_als")

    options.addOption("video_feature", true, "video_feature")

    options.addOption("ml_resort", true, "ml_resort")
    options.addOption("Music_MVSubscribe", true, "Music_MVSubscribe")

    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    println("1. 读取视频特征数据...")
    val inputVideoFeatrue = cmd.getOptionValue("video_feature")
    println("inputVideoFeatrue:" + inputVideoFeatrue)
    // println("vparray size:" + vparray.)
    val videoPredM = getVideoPredM(spark.read.parquet(inputVideoFeatrue).toDF().select("videoId", "vType", "rawPrediction").collect())
    //val videoPredM = getVideoPredM(viter)
    println("vieoPrefM size:" + videoPredM.size)
    val videoPredMBroad = sc.broadcast(videoPredM)


    println("2. 已推荐...")

    println("读取用户已经推荐数据1...")
    val inputUserRced = cmd.getOptionValue("user_rced_video")
    val userRcedVideosData = spark.read.textFile(inputUserRced).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userIdStr", "rcedVideos")

    println("读取用户已经推荐数据2...")
    val inputUserRcedFromEvent = cmd.getOptionValue("user_rced_video_from_event")
    val userRcedVideosFromEventData = spark.read.textFile(inputUserRcedFromEvent).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userIdStr", "rcedVideosFromEvent")

    val userRecedVideosData = userRcedVideosData.join(userRcedVideosFromEventData, Seq("userIdStr"), "outer")
      .map(row => {
        val userId = row.getAs[String]("userIdStr")
        val rcedSet = mutable.HashSet[String]()
        val rcedVideos = row.getAs[String]("rcedVideos")
        val rcedVideosFromEvent = row.getAs[String]("rcedVideosFromEvent")
        if (rcedVideos != null) {
          for (v <- rcedVideos.split(",")) {
            rcedSet.add(v)
          }
        }
        if (rcedVideosFromEvent != null) {
          for (v <- rcedVideosFromEvent.split(",")) {
            rcedSet.add(v)
          }
        }
        (userId, rcedSet.mkString(","))
      }).toDF("userIdStr", "rcedVideos")

    println("3. 加载用户召回数据...")
    val userResortData = spark.read.textFile(cmd.getOptionValue("ml_resort")).map(line => {
      val ts = line.split("\t", 2)
      (ts(0), ts(1).split(",").size)
    }).toDF("userIdStr", "recallNum")

    println("4. 偏好关联...")
    // val video4vdieoM = mutable.HashMap[String, List[String]]()
    val (songid2vdieoM, artid2videoM, mvid2videoM) = getSimsM(cmd.getOptionValue("sim_als"))
    println("songid2vdieoM size:" + songid2vdieoM.size)
    println("artid2videoM size:" + artid2videoM.size)
    println("mvid2videoM size:" + mvid2videoM.size)
    val songid2vdieoMBroad = sc.broadcast(songid2vdieoM)
    val artid2videoMBroad = sc.broadcast(artid2videoM)
    val mvid2videoMBroad = sc.broadcast(mvid2videoM)

    val userSongPrefData = spark.read.textFile(cmd.getOptionValue("userSongPref")).map(line => {
      // userId  \t
      // videoActiveUser  \t
      // songId : pref , songId : pref , ...  \t
      // userSubSongId , userSubSongId , ...  \t
      // songId : searchPref , songId : searchPref , ...  \t
      // localSongId , localSongId ,
      val ts = line.split("\t")
      val userId = ts(0)
      val prefSongWtM = mutable.HashMap[String, Float]()
      loadFirstPartWithWt(ts(2), prefSongWtM)
      loadFirstPartWithWt(ts(3), prefSongWtM)
      loadFirstPartWithWt(ts(4), prefSongWtM)
      loadFirstPartWithWt(ts(5), prefSongWtM)
      (userId, map2string(prefSongWtM))
    }).toDF("userIdStr", "songidWtM")

    val userArtPrefData = spark.read.textFile(cmd.getOptionValue("userArtPref")).map(line => {
      // userId  \t  activeVideoUser  \t  subArtistInfo  \t  recArtistInfo  \t  subSimilarArtistInfo  \t prefArtistFromVideoInfo
      // subArtistInfo			:	artistId : subTime , artistId : subTime , ...
      // recArtistInfo			:	artistId : score , artistId : score , ...
      // subSimilarArtistInfo		:	artistId : score , artistId : score , ...
      // prefArtistFromVideoInfo	:	artistId : pref : recallCnt , artistId : pref : recallCnt , ...
      val ts = line.split("\t")
      val userId = ts(0)
      val prefArtIdM = mutable.HashMap[String, Float]()
      loadFirstPartWithWt(ts(2), prefArtIdM)
      //loadFirstPart(ts(3), prefSongIds)
      //loadFirstPart(ts(4), prefSongIds)
      loadFirstPartWithWt(ts(5), prefArtIdM)
      (userId, map2string(prefArtIdM))
    }).toDF("userIdStr", "artidWtM")

    val userItemRatingData = spark.read.parquet(cmd.getOptionValue("userItemRating"))
      .withColumn("idtype_rating", comb($"idtype", $"rating"))
      .groupBy("userIdStr").agg(collect_list($"idtype_rating").as("idtype_rating_list"))
      .withColumn("artWtFromSong", getAidWtM($"idtype_rating_list"))

    println("订阅MV...")
    val userMVPref = spark.read.textFile(cmd.getOptionValue("Music_MVSubscribe"))
      .map(line => {
        // UserId,MVId,SubTime,type(0:MV, 1:Video)
        val ts = line.split("\t")
        var uid = ts(0).toLong
        if (uid < 0) {
          uid = -uid
        }
        var vmid = ts(1)
        val vtype = ts(3).toInt
        (uid.toString, vmid, vtype)
      }).toDF("userIdStr", "mvid", "vtype")
      .filter($"vtype" === 0) // vtpe==1时为video
      .groupBy("userIdStr").agg(collect_list($"mvid").as("mvid_list"))

    val rcmdData = userSongPrefData
      .join(userArtPrefData, Seq("userIdStr"), "left")
      .join(userMVPref, Seq("userIdStr"), "outer")
      .join(userItemRatingData, Seq("userIdStr"), "left")
      .join(userRecedVideosData, Seq("userIdStr"), "left")
      .join(userResortData, Seq("userIdStr"), "left")
      .map(row => {
        var recallNum = row.getAs[Int]("recallNum")
        if (recallNum == null) {
          recallNum = 0
        }
        //if (recallNum < 200) {
        val songid2VideoML = songid2vdieoMBroad.value
        val videoPredML = videoPredMBroad.value
        val userId = row.getAs[String]("userIdStr")
        // 已推荐
        val recedVids = mutable.HashSet[String]()
        val rcedVideos = row.getAs[String]("rcedVideos")
        // val rcedVideosFromEvent = row.getAs[String]("rcedVideosFromEvent")
        loadFirstPart(rcedVideos, recedVids)
        // loadFirstPart(rcedVideos, recedVids)
        if (userId.equals("359792224")) {
          println("reced for 359792224:" + recedVids.mkString(","))
        }
        // 根据偏好进行推荐
        val simresSrcprefsM = mutable.LinkedHashMap[String, (mutable.HashSet[String], Float)]()
        val simresSrcprefsMArt = mutable.LinkedHashMap[String, (mutable.HashSet[String], Float)]()
        val simresSrcprefsMMV = mutable.LinkedHashMap[String, (mutable.HashSet[String], Float)]()
        val songidWtM = string2map(row.getAs[String]("songidWtM"))
        val aidWtM = string2map(row.getAs[String]("artidWtM"))
        val aidWt2 = string2map(row.getAs[String]("artWtFromSong"))
        for ((aid, wt) <- aidWt2) {
          aidWtM.put(aid, aidWtM.getOrElse(aid, 0F) + wt)
        }

        if (simresSrcprefsM != null) {
          for ((prefsongid, wt) <- songidWtM) {
            val simres = songid2VideoML.getOrElse(prefsongid, null)
            if (simres != null) {
              for (simr <- simres) {
                if (!recedVids.contains(simr)) {
                  val (prefset, lstWt) = simresSrcprefsM.getOrElse(simr, (mutable.HashSet[String](), 0F))
                  prefset.add(prefsongid + "-song")
                  simresSrcprefsM.put(simr, (prefset, lstWt + wt))
                }
              }
            }
          }
        }

        if (aidWtM != null) {
          for ((prefaid, wt) <- aidWtM) {
            val simres = artid2videoMBroad.value.getOrElse(prefaid, null)
            if (simres != null) {
              for (simr <- simres) {
                if (!recedVids.contains(simr)) {
                  val (prefset, lstWt) = simresSrcprefsMArt.getOrElse(simr, (mutable.HashSet[String](), 0F))
                  prefset.add(prefaid + "-artist")
                  simresSrcprefsMArt.put(simr, (prefset, lstWt + wt))
                }
              }
            }
          }
        }

        // mv
        val mvidList = row.getAs[Seq[String]]("mvid_list")
        if (mvidList != null) {
          for (mvid <- mvidList) {
            val simres = mvid2videoMBroad.value.getOrElse(mvid, null)
            if (simres != null) {
              for (simr <- simres) {
                if (!recedVids.contains(simr)) {
                  val (prefset, lstWt) = simresSrcprefsMMV.getOrElse(simr, (mutable.HashSet[String](), 0F))
                  prefset.add(mvid + "-mv")
                  simresSrcprefsMMV.put(simr, (prefset, lstWt + 4))
                }
              }
            }
          }
        }

        val rcmdResult = mutable.ArrayBuffer[String]()
        val rcmdResultFromSong = getRcmd(simresSrcprefsM, videoPredML, userId, 20)
        val rcmdResultFromArt = getRcmd(simresSrcprefsMArt, videoPredML, userId, 20)
        val rcmdResultFromMV = getRcmd(simresSrcprefsMMV, videoPredML, userId, 20)

        rcmdResult.appendAll(rcmdResultFromSong)
        rcmdResult.appendAll(rcmdResultFromArt)
        rcmdResult.appendAll(rcmdResultFromMV)

        if (rcmdResult.size > 0) {
          if (recallNum < 200) {
            (1, userId + "\t" + rcmdResult.mkString(","))
          } else {
            (2, userId + "\t" + rcmdResult.mkString(","))
          }
        } else {
          (3, null)
        }

        /*
        } else {
          (3, null)
        }*/
      }).toDF("st", "data")

    rcmdData.filter($"st" === 1).select("data").repartition(80).write.text(cmd.getOptionValue("output") + "/mixAlsNa")
    rcmdData.filter($"st" === 1 || $"st" === 2).select("data").repartition(80).write.text(cmd.getOptionValue("output") + "/mixAls")
    // rcmdData.filter($"st" === 2).select("data").repartition(80).write.text(cmd.getOptionValue("output") + "/mixAls")

    // rcmdByVec.write.text(cmd.getOptionValue("output"))

  }

  def isDigit(tag: String) = {
    val regex="""^\d+$""".r
    if (regex.findFirstMatchIn(tag) == None) {
      false
    } else {
      true
    }
  }

  def splitKV(tagScoreStr: String, str: String) = {
    val idx = tagScoreStr.lastIndexOf(str)
    (tagScoreStr.substring(0, idx), tagScoreStr.substring(idx+1))
  }

  /*
        return {tag ->  [simtag:score, simtag:score] }
         */
  def getSimsM(inputDir: String) = {

    val songid2videosM = mutable.HashMap[String, ArrayBuffer[String]]()
    val artid2videosM = mutable.HashMap[String, ArrayBuffer[String]]()
    val mvid2videosM = mutable.HashMap[String, ArrayBuffer[String]]()

    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        getSimsMFile(bufferedReader, songid2videosM, artid2videosM, mvid2videosM)
        bufferedReader.close()
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getSimsMFile(bufferedReader, songid2videosM, artid2videosM, mvid2videosM)
      bufferedReader.close()
    }
    (songid2videosM, artid2videosM, mvid2videosM)

  }
  def getSimsMFile(reader: BufferedReader, songid2videosM: mutable.HashMap[String, ArrayBuffer[String]],
                   artid2videosM: mutable.HashMap[String, ArrayBuffer[String]], mvid2videosM: mutable.HashMap[String, ArrayBuffer[String]]) = {
    var string: String = reader.readLine()

    while (string != null) {
      // id-type \t id-video,id-mv
      val ts = string.split("\t", 2)
      val idtype = ts(0)
      val simres = mutable.ArrayBuffer[String]()
      if (idtype.endsWith("-art")) {
        artid2videosM.put(idtype.split("-")(0), simres)
      } else if (idtype.endsWith("-mv")) {
        mvid2videosM.put(idtype.split("-")(0), simres)
      } else if (!idtype.contains("-")) {
        songid2videosM.put(idtype.split("-")(0), simres)
      }
      val iter = ts(1).split(",").iterator
      var maxSimNum = 40
      while (iter.hasNext && simres.size < maxSimNum) {
        val idtypeWt = iter.next().split(":")
        val simidtype = idtypeWt(0)
        val wt = idtypeWt(1).toFloat
        if (wt < 0.3) {
          maxSimNum = 3
        }
        if (simidtype.endsWith("-video")) { // 视频默认省略后缀
          simres.append(simidtype.split("-")(0))
        } else {
          simres.append(simidtype)
        }
      }
      if (songid2videosM.size % 1000000 == 0) {
        println("songid2videosM size:" + songid2videosM.size + ", sid=" + idtype.split("-")(0) + "->" + simres.mkString(","))
      }
      if (artid2videosM.size % 1000000 == 0) {
        println("artid2videosM size:" + artid2videosM.size + ", aid=" + idtype.split("-")(0) + "->" + simres.mkString(","))
      }
      string = reader.readLine()
    }


  }

  def getVideoPredM(rows: Array[Row]) = {
    val idtypePredM = mutable.HashMap[String, Double]()
    for (row <- rows) {
      if (row.getAs[String]("vType").equals("video")) {
        val vid = row.getAs[Long]("videoId").toString
        val rawPred = row.getAs[Double]("rawPrediction")
        idtypePredM.put(vid, rawPred)
      }
    }
    idtypePredM
  }

  def getVideoPredM(viter: util.Iterator[Row]) = {
    val idtypePredM = mutable.HashMap[String, Double]()
    while (viter.hasNext) {
      val row = viter.next()
      if (row.getAs[String]("vType").equals("video")) {
        val vid = row.getAs[String]("videoId")
        val rawPred = row.getAs[Double]("rawPrediction")
        idtypePredM.put(vid, rawPred)
      }
    }
    idtypePredM
  }

  case class UidPreftag(userId: Long, ttype: String, prefTags: String)
  case class UidRcedVideos(userId: Long, rcedVideos: String)
  case class UidRcedVideosFromEvent(userId: Long, rcedVideosFromEvent: String)

  def getSet(str: String, videos: mutable.HashSet[String]) = {
    if (str != null) {
      for (video <- str.split(",")) {
        videos.add(video)
      }
    }
  }

  def vidReasonStr(videoReasonM: mutable.HashMap[String, String]): String = {
    var res = ""
    if (videoReasonM != null && !videoReasonM.isEmpty) {
      val sbuff = mutable.ArrayBuffer[String]()
      for ((vid, reason) <- videoReasonM) {
        // sbuff.append(vid + ":video:" + reason +"-tag")
        sbuff.append(vid + ":video") // 先忽略掉理由，下游排序不支持
      }
      res = sbuff.mkString(",")
    }
    res
  }


  def getTopTagVideos(tagVideosMLocal: mutable.HashMap[String, Array[String]], sortedCandTag: Seq[(String, Double)], rcedVideos: mutable.HashSet[String], num: Int) = {
    val videoReasonM = mutable.HashMap[String, String]()
    breakable {
      for ((candTag, score) <- sortedCandTag) {
        val videos = tagVideosMLocal.getOrElse(candTag, null)
        if (videos != null) {
          breakable {
            for (vid <- videos) {
              if (!rcedVideos.contains(vid)) {
                videoReasonM.put(vid, candTag)
                break()
              }
            }
          }
        }
        if (videoReasonM.size > num) {
          break()
        }
      }
    }
    videoReasonM
  }

  def getVideosRandomByChance(tagVideosMLocal: mutable.HashMap[String, Array[String]], sortedCandTag: Seq[(String, Double)], rcedVideos: mutable.HashSet[String], num: Int) = {
    // val videoReasonM = mutable.HashMap[String, String]()
    val videoReasonList = mutable.ArrayBuffer[(String, String)]()
    val random = new Random()
    var cnt = 0

    for ((candTag, score) <- sortedCandTag) { // 分数越大的越排后面
      cnt += 1
      if (videoReasonList.length >= num) {
        var rmIdx = random.nextInt(cnt + 1)
        if (rmIdx < videoReasonList.size || random.nextInt(sortedCandTag.length * 2) < cnt) { // 平均概率 || 越排后面，被选中概率越大
          if (rmIdx >= videoReasonList.size) {
            rmIdx = random.nextInt(cnt + 1)
            if (rmIdx < videoReasonList.size) {
              videoReasonList.remove(rmIdx)
            }
          }
        }
      }
      if (videoReasonList.size < num) {
        val videos = tagVideosMLocal.getOrElse(candTag, null)
        if (videos != null) {
          breakable {
            for (vid <- videos) {
              if (!rcedVideos.contains(vid)) {
                videoReasonList.append((vid, candTag))
                // videoReasonM.put(vid, candTag)
                break()
              }
            }
          }
        }
      }
    }
    val videoReasonM = mutable.HashMap[String, String]()
    for (vidtag <- videoReasonList) {
      videoReasonM.put(vidtag._1, vidtag._2)
    }
    videoReasonM
  }

  def loadFirstPart(str: String, set: mutable.HashSet[String]) = {
    // id:score,id:score
    if (str != null) {
      for (ids <- str.split(",")) {
        set.add(ids.split(":")(0))
      }
    }
  }

  def loadFirstPartWithWt(str: String, map: mutable.HashMap[String, Float]) = {
    // id:score,id:score
    if (str != null) {
      for (ids <- str.split(",")) {
        val id = ids.split(":")(0)
        map.put(id, map.getOrElse(id, 0F)+ 1)
      }
    }
  }

  def matchMaxSrcCntM(srcCntM: mutable.HashMap[String, Int], srcprefs:mutable.HashSet[String], max: Int): Boolean = {
    for (src <- srcprefs) {
      val cnt = srcCntM.getOrElse(src, 0)
      if (cnt >= max) {
        return false
      }
    }
    return true
  }

  def updateSrcCntM(srcCntM: mutable.HashMap[String, Int], srcprefs: mutable.HashSet[String]) = {
    for (src <- srcprefs) {
      srcCntM.put(src, srcCntM.getOrElse(src, 0) + 1)
    }
  }

}
