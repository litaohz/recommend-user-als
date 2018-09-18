package com.netease.music.recommend.scala.feedflow.search

import java.io._
import java.util

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
  * 根据搜索行为挖掘关联，根据歌曲偏好关联视频
  */
object RcmdVideoBySims {

  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val options = new Options()

    options.addOption("userSongPref", true, "userSongPref")
    options.addOption("user_rced_video", true, "user_rced_video")
    options.addOption("sims", true, "sims")

    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)



    println("# 已推荐 #")

    println("读取用户已经推荐数据1...")
    val inputUserRced = cmd.getOptionValue("user_rced_video")
    val userRcedVideosData = spark.read.textFile(inputUserRced).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t")
      (ts(0), ts(1))
    }).toDF("userIdStr", "rcedVideos")

    println("读取用户已经推荐数据2...")
    val userRecedVideosData = userRcedVideosData //.join(userRcedVideosFromEventData, Seq("userIdStr"), "outer")
      .map(row => {
        val userId = row.getAs[String]("userIdStr")
        val rcedSet = mutable.HashSet[String]()
        val rcedVideos = row.getAs[String]("rcedVideos")
        //val rcedVideosFromEvent = row.getAs[String]("rcedVideosFromEvent")
        if (rcedVideos != null) {
          for (v <- rcedVideos.split(",")) {
            rcedSet.add(v)
          }
        }
        (userId, rcedSet.mkString(","))
      }).toDF("userIdStr", "rcedVideos")

    println("3. 加载用户召回数据...")
    /*
    val userResortData = spark.read.textFile(cmd.getOptionValue("ml_resort")).map(line => {
      val ts = line.split("\t", 2)
      (ts(0), ts(1).split(",").size)
    }).toDF("userIdStr", "recallNum")
*/

    println("4. 偏好关联...")
    // val video4vdieoM = mutable.HashMap[String, List[String]]()
    val songid2vdieoM = getSimsM(cmd.getOptionValue("sims"))
    println("songid2vdieoM size:" + songid2vdieoM.size)
    //println("artid2videoM size:" + artid2videoM.size)
    val songid2vdieoMBroad = sc.broadcast(songid2vdieoM)

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


    val rcmdData = userSongPrefData//.join(userArtPrefData, Seq("userIdStr"), "left")
      .join(userRecedVideosData, Seq("userIdStr"), "left")
      .map(row => {
        val songid2VideoML = songid2vdieoMBroad.value
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
        val songidWtM = string2map(row.getAs[String]("songidWtM"))
      if (userId.equals("359792224")) {
        println("songidWtM for 359792224:" + songidWtM)
      }
        if (songidWtM != null) {
          for ((prefsongid, wt) <- songidWtM) {
            val simres = songid2VideoML.getOrElse(prefsongid, null)
            if (simres != null) {
              for (simr <- simres) {
                val idtypescore = simr.split(":")
                if (!recedVids.contains(idtypescore(0)) && idtypescore(1).equals("video")) {
                  val (prefset, lstWt) = simresSrcprefsM.getOrElse(idtypescore(0), (mutable.HashSet[String](), 0F))
                  prefset.add(prefsongid + "-song")
                  simresSrcprefsM.put(idtypescore(0), (prefset, lstWt + wt * idtypescore(2).toFloat))
                }
              }
            }
          }
        }
        if (userId.equals("359792224")){
          println("359792224 + simresSrcprefsM" + simresSrcprefsM.head)
        }
        val rcmdResultFromSong = getRcmd(simresSrcprefsM, userId, 40)
        if (rcmdResultFromSong.size > 0) {
            (1, userId + "\t" + rcmdResultFromSong.mkString(","))
        }
        else {
          (4, null)
        }
      }).toDF("st", "data")

    rcmdData.filter($"st" === 1).select("data").repartition(80).write.text(cmd.getOptionValue("output") + "/mixSearch")

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

    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        getSimsMFile(bufferedReader, songid2videosM)
        bufferedReader.close()
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getSimsMFile(bufferedReader, songid2videosM)
      bufferedReader.close()
    }
    songid2videosM

  }
  def getSimsMFile(reader: BufferedReader, songid2videosM: mutable.HashMap[String, ArrayBuffer[String]]) = {
    var string: String = reader.readLine()

    while (string != null) {
      // id-type \t ... \tid-video,id-mv
      val ts = string.split("\t", 3)
      if (ts.size >= 3) {
        val idtype = ts(0)
        val simres = mutable.ArrayBuffer[String]()

       if (idtype.endsWith("-song")) {
          songid2videosM.put(idtype.split("-")(0), simres)
        }
        val iter = ts(2).split(",").iterator
        var maxSimNum = 30
        while (iter.hasNext && simres.size < maxSimNum) {
          val idtypeWt = iter.next().split(":")
          val simidtype = idtypeWt(0) + ":" + idtypeWt(1)
          val wt = idtypeWt(2).toFloat
          if (wt < 0.4) {
            maxSimNum = 4
          }
          simres.append(simidtype + ":" + idtypeWt(2).substring(0, 3)) // 保留2位分数
        }
        if (songid2videosM.size % 1000000 == 0) {
          println("songid2videosM size:" + songid2videosM.size + ", sid=" + idtype.split("-")(0) + "->" + simres.mkString(","))
        }
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

  def getVideoScoreM(rows: Array[Row]) = {
    val idtypePredM = mutable.HashMap[String, Double]()
    for (row <- rows) {
      val play = row.getAs[Long]("play")
      val rawPred = row.getAs[Double]("score")
      if (play > 300 && rawPred > 0.08) {
        val vid = row.getAs[String]("idtype").split(":")(0)
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

  def getRcmd(simresSrcprefsM: mutable.LinkedHashMap[String, (mutable.HashSet[String], Float)],
              userId: String, rcmdNum: Int) = {
    val rcmdResult = mutable.ArrayBuffer[String]()
    val simScoreList = mutable.ArrayBuffer[(String, Double)]()
    if (simresSrcprefsM.size > 0) {
      for ((simres, (set, wt)) <- simresSrcprefsM) {
        val score =  wt // 偏好数量 + 视频预估分
        simScoreList.append((simres, score))
      }

      val sortedSimSrcprefs = simScoreList.sortWith(_._2 > _._2)

      if (userId.equals("359792224")) {
        println("359792224 mid result:" + sortedSimSrcprefs.mkString(",") )
        for ((simres, score) <- sortedSimSrcprefs) {
          println(simres + " -> " + score)
        }
      }

      val srcCntM = mutable.HashMap[String, Int]()
      val iter = sortedSimSrcprefs.iterator


      // var rcmdNum = 40
      // 限制每个tag召回数量
      var rcmdNum4EachTag = rcmdNum / (sortedSimSrcprefs.size + 1) + 2
      if (rcmdNum4EachTag > 5) {
        rcmdNum4EachTag = 5
      }

      while (iter.hasNext && rcmdResult.size < rcmdNum) {
        val (simres, score) = iter.next()
        val (srcprefs, wt) = simresSrcprefsM.get(simres).get
        if (matchMaxSrcCntM(srcCntM, srcprefs, rcmdNum4EachTag)) { // 输出数量小于3
          if (!simres.contains("-")) {
            val reasonStr = getReason(srcprefs, 3)
            if (!reasonStr.isEmpty) {
              rcmdResult.append(simres + ":video:" + reasonStr)
            }
          } else {
            val reasonStr = getReason(srcprefs, 3)
            rcmdResult.append(simres.replace("-", ":") + ":" + reasonStr)
          }
          updateSrcCntM(srcCntM, srcprefs)
        }
      }
    }
    rcmdResult
  }

}
