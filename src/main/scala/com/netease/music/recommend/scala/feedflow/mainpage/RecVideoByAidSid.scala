package com.netease.music.recommend.scala.feedflow.mainpage

import java.io._
import java.util.Date

import com.netease.music.recommend.scala.feedflow.utils.ABTest
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 从排序视频中获取分类
  * Created by hzhangjunfei on 2018/04/01.
  */
object RecVideoByAidSid {

  val NOW_TIME = System.currentTimeMillis()

  val KWD_REC_WINDOW = 7
  val SONGART_REC_WINDOW = 7

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options

    options.addOption("Music_VideoRcmdMeta", true, "video_input")
    options.addOption("videoPool", true, "videoPool")

    options.addOption("reced_videos", true, "reced_videos")
    options.addOption("stopwords", true, "stopwords")

    options.addOption("Music_Song_artists", true, "music_song_input")
    options.addOption("songProfile", true, "songProfile")
    options.addOption("artistProfile", true, "artistProfile")
    options.addOption("songRVideos", true, "songRVideos")
    options.addOption("artistRVideos", true, "artistRVideos")
    options.addOption("art_tags", true, "art_tags")
    options.addOption("art_sims", true, "art_sims")

    options.addOption("output", true, "output directory")
    options.addOption("partitionNum", true, "output partitionNum")

//    options.addOption("abtestConfig", true, "abtestConfig file")
//    options.addOption("abtestExpName", true, "abtest Exp Name")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

//    val configText = spark.read.textFile(cmd.getOptionValue("abtestConfig")).collect()
//    val abtestExpName = cmd.getOptionValue("abtestExpName")
//    for (cf <- configText) {
//      if (cf.contains(abtestExpName)) {
//        println(cf)
//      }
//    }

    val videoPoolInput = cmd.getOptionValue("videoPool")
    println("videoPoolInput:" + videoPoolInput)
    /*val musicVideoS = spark.read.parquet(videoPoolInput)
      .filter($"isMusicVideo"===1 && $"vType"==="video")
      //.filter($"groupIdAndNames".contains("12102:只推首页") && $"vType"==="video")
      .select("videoId")
      .map(row => row.getLong(0).toString)
      .collect()
      .toSet
    val musicVideoSBroad = sc.broadcast(musicVideoS)*/
    val videosInPoolS = spark.read.parquet(videoPoolInput)
      //.filter($"isMusicVideo"===1 && $"vType"==="video")
      .select("videoId")
      .map(row => row.getLong(0).toString)
      .collect()
      .toSet
    //val videosInPoolSBroad = sc.broadcast(videosInPoolS)

    val songRVideosInput = cmd.getOptionValue("songRVideos")
    val artistRVideosInput = cmd.getOptionValue("artistRVideos")
    val sidVideosM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val aidVideosM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val aidsidVideosM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val vidSidAidM = mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])]()
    getVideoInfoM(songRVideosInput, artistRVideosInput, videosInPoolS, sidVideosM, aidVideosM, vidSidAidM/*, musicVideoS*/)

    for ((vid, (sids, aids)) <- vidSidAidM) {
      if (aids.size > 0 && sids.size > 0) {
        for (aid <- aids ; sid <- sids) {
          val aidsid = aid + "-" + sid
          val videos = aidsidVideosM.getOrElse(aidsid, ArrayBuffer[String]())
          aidsidVideosM.put(aidsid, videos)
          if (!videos.contains(vid))
            videos.append(vid)
        }
      }
    }
    flushStringArrayBufferM(sidVideosM, cmd.getOptionValue("output") + "/sidVideos")
    flushStringArrayBufferM(aidVideosM, cmd.getOptionValue("output") + "/aidVideos")
    flushStringArrayBufferArrayBufferM(vidSidAidM, cmd.getOptionValue("output") + "/vidSidAid")
    flushStringArrayBufferM(aidsidVideosM, cmd.getOptionValue("output") + "/aidsidVideos")
    println("videosInPoolS size :" + videosInPoolS.size + ", first item:" + videosInPoolS.iterator.next())
    println("sidVideosM size :" + sidVideosM.size + ", first item:" + sidVideosM.iterator.next())
    println("aidVideosM size :" + aidVideosM.size + ", first item:" + aidVideosM.iterator.next())
    println("vidSidAidM size :" + vidSidAidM.size + ", first item:" + vidSidAidM.iterator.next())
    println("aidsidVideosM size :" + vidSidAidM.size + ", first item:" + aidsidVideosM.iterator.next())

    //val stopwords = getStopwords(cmd.getOptionValue("stopwords"))

    val aidVideosMBroad = sc.broadcast(aidVideosM)
    val sidVideosMBroad = sc.broadcast(sidVideosM)
    val aidsidVideosMBroad = sc.broadcast(aidsidVideosM)
    //val vidSidAidMBroad = sc.broadcast(vidSidAidM)


    // 只加载aid有视频的songid
    val songArtInput = cmd.getOptionValue("Music_Song_artists")
    val sidAidsM = mutable.HashMap[String, ArrayBuffer[String]]()
    val aidNameM = mutable.HashMap[String, String]()
    getSongArtInfoM(songArtInput, sidAidsM, aidNameM, aidVideosM, sidVideosM)
    println("sidAidsM size:" + sidAidsM.size)
    println("sid=186010(周杰伦轨迹), aids=" + sidAidsM.get("186010"))
    val sidAidsMBroad = sc.broadcast(sidAidsM)
    //val aidNameMBroad = sc.broadcast(aidNameM)

    println("Load 推荐过视频")
    val recedVideosPath = cmd.getOptionValue("reced_videos")
    println(recedVideosPath)
    val recedVideoData = spark.read.textFile(recedVideosPath).map(line => {
      // uid \t vid,vid
      val ts = line.split("\t", 2)
      if (ts(0).equals("135571358")) {
        println("RECED KEYWORD reced:" +  line)
      }
      (ts(0), ts(1))
    }).toDF("uid", "recedVideos")

    println("Load songProfile")
    val songProfile = spark.read.textFile(cmd.getOptionValue("songProfile"))
      .map(line => {
        // uid  \t  songId:recalltypes , songId:recalltypes , ...
        val us = line.split("\t", 2)
        (us(0), us(1))
      }).toDF("uid", "songProfile")
    println("Load artistProfile")
    val artistProfile = spark.read.textFile(cmd.getOptionValue("artistProfile"))
      .map(line => {
        // uid  \t  artistId:recalltypes , artistId:recalltypes , ...
        val us = line.split("\t", 2)
        (us(0), us(1))
      }).toDF("uid", "artistProfile")

    val rcmdData = songProfile
      .join(artistProfile, Seq("uid"), "outer")
      .join(recedVideoData, Seq("uid"), "left_outer")
      .flatMap(row => {
        val uid = row.getAs[String]("uid")
//        val musicABTest = new ABTest(configText)
//        val groupName = musicABTest.abtestGroup(uid.toLong, abtestExpName)
//        if (groupName.equals("t1") || groupName.equals("t2") || groupName.equals("t3") || uid.equals("135571358")) {

          val prefSids = mutable.ArrayBuffer[String]()
          val prefAids = mutable.ArrayBuffer[String]()
          val recedVids = mutable.HashSet[String]()
          val recedReasons = mutable.HashSet[String]()

          // (1) 加载偏好
          val songProfile = row.getAs[String]("songProfile")  // 艺人偏好
          if (songProfile != null)
            appendPrefids(songProfile, prefSids)

          val artistProfile = row.getAs[String]("artistProfile") // 艺人偏好
          if (artistProfile != null)
            appendPrefids(artistProfile, prefAids)

          // (2) 加载推荐过数据
          val recedVideos = row.getAs[String]("recedVideos")
          getRecedVideos(recedVideos, recedVids)
          if (uid.equals("135571358")) {
            println("for 135571358 prefSids:" + prefSids)
            println("for 135571358 prefAids:" + prefAids)
            println("for 135571358 recedVids:" + recedVids)
          }

          val rcmds4ArtCands = mutable.ArrayBuffer[String]() // 推荐候选集
          val rcmds4SongCands = mutable.ArrayBuffer[String]() // 推荐候选集
          val rcmds4ArtSongCands = mutable.ArrayBuffer[String]() // 推荐候选集
          val recThred = 30
          val recsPerSourceThred = 3
          for (sid <- prefSids) {
            val songReason = sid + "-song"
            if (sidVideosMBroad.value.contains(sid) && !recedReasons.contains(songReason)) {

              // 关联偏好 aid + sid
              if (sidAidsMBroad.value.contains(sid)/* && (groupName.equals("t1") || uid.equals("135571358"))*/) {
                val aids = sidAidsMBroad.value(sid)
                for (aid <- aids) {
                  val artReason = aid + "-artist"
                  if (prefAids.contains(aid) && !recedReasons.contains(artReason) && !recedReasons.contains(songReason)) {
                    val aidsid = aid + "-" + sid
                    if (aidsidVideosMBroad.value.contains(aidsid)) {
                      var recCountPerSource = 0
                      for (vid <- aidsidVideosMBroad.value(aidsid)) {
                        if (!recedVids.contains(vid) && recCountPerSource < recsPerSourceThred && rcmds4ArtSongCands.size < recThred) {
                          rcmds4ArtSongCands.append(vid + ":video:" + sid + "-song&" + aid + "-artist")

                          recCountPerSource += 1
                          recedVids.add(vid)
                          recedReasons.add(songReason)
                          recedReasons.add(artReason)
                        }
                      }
                    }
                  }
                }
              }

              // 关联偏好 sid
              var recCountPerSource = 0
              if (!recedReasons.contains(songReason)/* && (groupName.equals("t2") || uid.equals("135571358"))*/) {
                for (vid <- sidVideosMBroad.value(sid)) {
                  if (!recedVids.contains(vid) && recCountPerSource < recsPerSourceThred && rcmds4SongCands.size < recThred) {
                    rcmds4SongCands.append(vid + ":video:" + sid + "-song")

                    recCountPerSource += 1
                    recedVids.add(vid)
                    recedReasons.add(songReason)
                  }
                }
              }
            }
          }
          // 关联偏好 aid
          for (aid <- prefAids) {
            val artReason = aid + "-artist"
            if (aidVideosMBroad.value.contains(aid) && !recedReasons.contains(artReason)/* && (groupName.equals("t3") || uid.equals("135571358"))*/) {
              var recCountPerSource = 0
              for(vid <- aidVideosMBroad.value(aid)) {
                if (!recedVids.contains(vid) && recCountPerSource < recsPerSourceThred && rcmds4ArtCands.size < recThred) {
                  rcmds4ArtCands.append(vid + ":video:" + aid + "-artist")

                  recCountPerSource += 1
                  recedVids.add(vid)
                  recedReasons.add(artReason)
                }
              }
            }
          }
          if (uid.equals("135571358")) {
            println("rcmds4ArtCands 4 135571358:" + rcmds4ArtCands)
            println("rcmds4SongCands 4 135571358:" + rcmds4SongCands)
            println("rcmds4ArtSongCands 4 135571358:" + rcmds4ArtSongCands)
            println("recedReasons 4 135571358:" + recedReasons)
          }

          val result = mutable.ArrayBuffer[(String, String)]()
          if (!rcmds4ArtCands.isEmpty)
            result.append(("A", uid + "\t" + rcmds4ArtCands.mkString(",")))
          if (!rcmds4SongCands.isEmpty)
            result.append(("S", uid + "\t" + rcmds4SongCands.mkString(",")))
          if (!rcmds4ArtSongCands.isEmpty)
            result.append(("AS", uid + "\t" + rcmds4ArtSongCands.mkString(",")))

          result
//        } else {
//          mutable.ArrayBuffer[(String, String)]()
//        }
      })
      .filter(_ != null).filter(_ != "").filter(_ != None)
      .cache

    // 使用partitionBy的作用：为了改名字（使用part-*****这种类型的名字）
    val partitionNum = cmd.getOptionValue("partitionNum").toInt
    rcmdData.filter(_._1.equals("A")).map({case(stype,line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(partitionNum)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/newArt")
    rcmdData.filter(_._1.equals("S")).map({case(stype,line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(partitionNum)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/newSong")
    rcmdData.filter(_._1.equals("AS")).map({case(stype,line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(partitionNum)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/newArtSong")
  }

  def getRecedVideos(recedVideos: String, recedVids: mutable.HashSet[String]) = {
    if (recedVideos != null) {
      for (vid <- recedVideos.split(",")) {
        recedVids.add(vid)
      }
    }
  }

  def appendPrefids(prefs: String, prefids: ArrayBuffer[String]) = {
    if (prefs != null) {
      for (prefInfoStr <- prefs.split(",")) {
        val prefInfo = prefInfoStr.split(":")
        val prefid = prefInfo(0)
        if (!prefids.contains(prefid))
          prefids.append(prefid)
      }
    }
  }

  def getVideoInfoM(songRVideosInput: String, artistRVideosInput: String, videosInPoolS: collection.immutable.Set[String],
                    sidVideosM: mutable.HashMap[String, mutable.ArrayBuffer[String]], aidVideosM: mutable.HashMap[String, mutable.ArrayBuffer[String]],
                    vidSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])]/*,
                    musicVideoS: Set[String]*/) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    // song
    val songpath = new Path(songRVideosInput)
    if (hdfs.isDirectory(songpath)) {
      for (status <- hdfs.listStatus(songpath) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getVideoInfoMFromFile(bufferedReader, videosInPoolS, sidVideosM, vidSidAidM/*, musicVideoS*/)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(songpath)))
      getVideoInfoMFromFile(bufferedReader, videosInPoolS, sidVideosM, vidSidAidM/*, musicVideoS*/)
      bufferedReader.close()
    }

    // artist
    val artistpath = new Path(artistRVideosInput)
    if (hdfs.isDirectory(artistpath)) {
      for (status <- hdfs.listStatus(artistpath) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getVideoInfoMFromFile(bufferedReader, videosInPoolS, aidVideosM, vidSidAidM/*, musicVideoS*/)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(artistpath)))
      getVideoInfoMFromFile(bufferedReader, videosInPoolS, aidVideosM, vidSidAidM/*, musicVideoS*/)
      bufferedReader.close()
    }

  }

  def getVideoInfoMFromFile(reader: BufferedReader, videosInPoolS: collection.immutable.Set[String],
                            sourceRVideosM: mutable.HashMap[String, ArrayBuffer[String]],
                            vidSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])]/*,
                            musicVideoS: Set[String]*/) = {

    var line = reader.readLine()
    while (line != null) {
      // srcid-type  \t  vid-type:score:recalltypes , vid-type:score:recalltypes , ...
      val info = line.split("\t")
      if (info.length >= 2) {
        val idtype = info(0).split("-")
        val srcid = idtype(0)
        val srctype = idtype(1)
        val videos = mutable.ArrayBuffer[String]()
        for (videoInfoStr <- info(1).split(",")) {
          val videoInfo = videoInfoStr.split(":")
          val videoId = videoInfo(0).split("-")(0)

          var validV = true
          if (!videosInPoolS.contains(videoId))
            validV = false
          /*if (!musicVideoS.contains(videoId))
            validV = false*/

          if (validV == true) {
            videos.append(videoId)

            val sidAidPair = vidSidAidM.getOrElse(videoId, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
            vidSidAidM.put(videoId, sidAidPair)
            if (srctype.equals("song"))
              sidAidPair._1.append(srcid)
            else if (srctype.equals("artist"))
              sidAidPair._2.append(srcid)
          }
        }
        if (videos.size > 0)
          sourceRVideosM.put(srcid, videos)
      }
      line = reader.readLine()
    }
  }

  def getSongArtInfoM(inputDir: String, sidAidsM: mutable.HashMap[String, ArrayBuffer[String]], aidNameM: mutable.HashMap[String, String],
                      aidVideosM: mutable.HashMap[String, ArrayBuffer[String]], sidVideosM: mutable.HashMap[String, ArrayBuffer[String]]) = {

    // getSongArtInfoM(inputDir: String, sidAidM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String])= {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getSongArtInfoMFromFile(bufferedReader, sidAidsM, aidNameM, aidVideosM, sidVideosM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getSongArtInfoMFromFile(bufferedReader, sidAidsM, aidNameM,  aidVideosM, sidVideosM)
      bufferedReader.close()
    }
  }

  def getSongArtInfoMFromFile(reader: BufferedReader, sidAidsM: mutable.HashMap[String, ArrayBuffer[String]], aidNameM: mutable.HashMap[String, String],
                              aidVideosM: mutable.HashMap[String, ArrayBuffer[String]], sidVideosM: mutable.HashMap[String, ArrayBuffer[String]]) = {
    var line: String = reader.readLine()
    while (line != null) {
      // 59879	1873:阿宝	10846:张冬玲
      if (line != null) {
        val ts = line.split("\t", 2)
        var sid = ts(0)
        if (sid.startsWith("-"))
          sid = sid.substring(1)

        val aids = mutable.ArrayBuffer[String]()
        if (!sid.isEmpty) {
          if (ts.length >= 2) {
            for (aidnameStr <- ts(1).split("\t")) {
              val aidname = aidnameStr.split(":", 2)
              if (aidname.length >= 2) {
                val aid = aidname(0)
                if (!aid.isEmpty) {
                  aids.append(aid)
                  val aname = aidname(1)
                  if (!aidNameM.contains(aid) && !aname.isEmpty && aidVideosM.contains(aid)) { // video或者resource里面有这个艺人
                    aidNameM.put(aid, aname.replace(",", " "))
                  }
                }
              }
            }
          }
          if (aids.length > 0 && sidVideosM.contains(sid)) { // video或者resource里面有这首歌
            sidAidsM.put(sid, aids)
          }
        }
      }
      line = reader.readLine()
    }
  }

  def flushVideoTitle(vidTitleM: mutable.HashMap[String, String], outPath: String) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var outpath = new Path(outPath)
    var bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(outpath)))
    for ((vid, title) <- vidTitleM) {
      bwriter.write(vid + ":video\t" + title + "\n")
    }
    bwriter.close()
  }

  def flushStringStringM(vidTitleM: mutable.HashMap[String, String], outPath: String) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var outpath = new Path(outPath)
    var bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(outpath)))
    for ((key, value) <- vidTitleM) {
      bwriter.write(key + "\t" + value + "\n")
    }
    bwriter.close()
  }

  def flushStringArrayBufferM(vidTitleM: mutable.HashMap[String, ArrayBuffer[String]], outPath: String) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var outpath = new Path(outPath)
    var bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(outpath)))
    for ((key, values) <- vidTitleM) {
      bwriter.write(key + "\t" + values.mkString(",") + "\n")
    }
    bwriter.close()
  }

  def flushStringArrayBufferArrayBufferM(vidTitleM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String])], outPath: String) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var outpath = new Path(outPath)
    var bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(outpath)))
    for ((key, (values1, values2)) <- vidTitleM) {
      bwriter.write(key + "\t" + values1.mkString(",") + "\t" + values2.mkString(",") + "\n")
    }
    bwriter.close()
  }

  def flushStringArrayBufferArrayBufferArrayBufferM(vidTitleM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String], ArrayBuffer[String])], outPath: String) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var outpath = new Path(outPath)
    var bwriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(outpath)))
    for ((key, (values1, values2, values3)) <- vidTitleM) {
      bwriter.write(key + "\t" + values1.mkString(",") + "\t" + values2.mkString(",") + "\t" + values3.mkString(",") + "\n")
    }
    bwriter.close()
  }

  def getVideoDupM(input: String, vidPredM: mutable.HashMap[String, Float]) = {
    val vid2ShowvidM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(input)

    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = reader.readLine()
    while (line != null) {
      // id \t id \t id
      val ids = line.split("\t")
      var maxPredId = ids(0)
      var maxPred = vidPredM.getOrElse(maxPredId, 0F)
      for (i <- 1 until ids.length) {
        val curPred = vidPredM.getOrElse(ids(i), 0F)
        if (curPred > maxPred) {
          maxPred = curPred
          maxPredId = ids(i)
        }
      }
      val idset = mutable.HashSet[String]()
      for (id <- ids) {
        if (!id.equals(maxPredId)) {
          vid2ShowvidM.put(id, maxPredId)
        }
      }
      line = reader.readLine()
    }
    reader.close()
    vid2ShowvidM
  }

  def getRcmd(aid: String, keyword: String, sids: mutable.ArrayBuffer[String],
              /*recedSids: mutable.HashSet[String], */recedVids: mutable.HashSet[String],
              vidTitleM: mutable.HashMap[String, String],
              //vidSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
              aidsidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]],
              aidVideosM: mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]],
              rcmdsForArtCands: mutable.ArrayBuffer[String], maxResultNum:Int) {

    val resourcetype = "video"
    val sidIter = sids.iterator
    while (sidIter.hasNext && rcmdsForArtCands.length < maxResultNum) {
      val sid = sidIter.next
      //if (!recedSids.contains(sid)) { // 歌曲没有推荐过

      // aid+sid同时命中user偏好的videos
      val aidsid = aid + ":" + sid
      var aidsidrcmdCnt = 0
      val videos = aidsidVideosM.getOrElse(aidsid, null)
      if (videos != null) {
        for ((vid, pred) <- videos if aidsidrcmdCnt < 1) {    // 每个aidsid召回最多2个结果
          if (vidTitleM.contains(vid) && !recedVids.contains(vid)) {

            rcmdsForArtCands.append(vid + ":" + resourcetype + ":" + sid + "-song")
            recedVids.add(vid)
            aidsidrcmdCnt += 1
            //recedSids.add(sid)
          }
        }
      }
    }

    // aid命中user偏好的videos
    val videoPrds = aidVideosM.getOrElse(aid, null)
    if (videoPrds != null) {
      val vIter = videoPrds.iterator
      while (vIter.hasNext  && rcmdsForArtCands.length < maxResultNum) {
        val vidPrd = vIter.next
        val vid = vidPrd._1
        //val (sids, aids) = vidSidAidM.getOrElse(vid, (null, null))
        if (!recedVids.contains(vid) &&
          vidTitleM.contains(vid) /*&&
          !isSongReced(sids, recedSids)*/
        ) {

          rcmdsForArtCands.append(vid + ":" + resourcetype + ":" + aid + "-artist")
          recedVids.add(vid)
          //addRecedSong(sids, recedSids)
        }
      }
    }
  }
}
