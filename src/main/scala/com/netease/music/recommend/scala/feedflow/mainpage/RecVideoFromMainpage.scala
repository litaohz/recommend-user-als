package com.netease.music.recommend.scala.feedflow.mainpage

import java.io._
import java.util.Date

import com.netease.music.recommend.scala.feedflow.utils.ABTest
import com.netease.music.recommend.scala.feedflow.videoProfile.GetVideoHotScore.getExistsPath
import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 从排序视频中获取分类
  * Created by hzhangjunfei on 2018/04/01.
  */
object RecVideoFromMainpage {

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
    options.addOption("video_prd", true, "video_prd")

    options.addOption("reced_videos", true, "reced_videos")
    options.addOption("reced_keyword", true, "reced_keyword")

    //options.addOption("video_dup", true, "video_dup")
    options.addOption("stopwords", true, "stopwords")

    options.addOption("Music_Song_artists", true, "music_song_input")
    options.addOption("songPref", true, "songPref")
    options.addOption("art_tags", true, "art_tags")
    options.addOption("art_sims", true, "art_sims")
    options.addOption("user_sub_art", true, "user_sub_art")

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

    // 获取vid及其对应的rawPrediction
    val videoPredInput = cmd.getOptionValue("video_prd")
    println("Load video_prd:" + videoPredInput)
    val videoPredM = getVideoPredM(videoPredInput)
    println("videoPredM size:" + videoPredM.size)
    println("video=213295, prd=" + videoPredM.get("213295"))

    /*// 获取vid及其对应的maxRawPrediction vid
    val vid2TopvidM = getVideoDupM(cmd.getOptionValue("video_dup"), videoPredM) // 重复视频*/


    val videoPoolInput = cmd.getOptionValue("videoPool")
    println("videoPoolInput:" + videoPoolInput)
    val musicVideoS = spark.read.parquet(videoPoolInput)
      .filter($"isMusicVideo"===1 && $"vType"==="video")
      //.filter($"groupIdAndNames".contains("12102:只推首页") && $"vType"==="video")
      .select("videoId")
      .map(row => row.getLong(0).toString)
      .collect()
      .toSet
    val musicVideoSBroad = sc.broadcast(musicVideoS)

    val videoInput = cmd.getOptionValue("Music_VideoRcmdMeta")
    println("videoMetaInput:" + videoInput)
    val sidVideosM = mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]]()
    val aidsidVideosM = mutable.HashMap[String, ArrayBuffer[(String, Float)]]()
    val aidVideosM = mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]]()
    val vidSidAidM = mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])]()
    val vidTitleM = getVideoInfoM(videoInput, sidVideosM, aidVideosM, videoPredM, vidSidAidM, aidsidVideosM/*, musicVideoS*/)
    println("vidTitleM size before clear:" + vidTitleM.size)

    val stopwords = getStopwords(cmd.getOptionValue("stopwords"))
    removeInvalid(vidTitleM, /*vid2TopvidM, */stopwords, videoPredM)

    println("sidVideosM size:" + sidVideosM.size)
    println("aidVideosM size:" + aidVideosM.size)
    println("vidTitleM size:" + vidTitleM.size)

    // 输出 vid  \t  title
    flushVideoTitle(vidTitleM, cmd.getOptionValue("output") + "/vid_title")

    // aid:sid    vid
    val aidsidVideosMBroad = sc.broadcast(aidsidVideosM)
    // aid    (vid,rawPrediction),...
    val aidVideosMBroad = sc.broadcast(aidVideosM)
    // sid    (vid,rawPrediction),...
    val vidSidAidMBroad = sc.broadcast(vidSidAidM)
    // vid    title
    val vidTitleMBroad = sc.broadcast(vidTitleM)


    //////////// 只加载aid有视频的songid
    val songArtInput = cmd.getOptionValue("Music_Song_artists")
    println("songArtInput:" + songArtInput)
    val sidAidsM = mutable.HashMap[String, String]()
    val aidNameM = mutable.HashMap[String, String]()
    getSongArtInfoM(songArtInput, sidAidsM, aidNameM, aidVideosM, sidVideosM)
    println("sidAidsM size:" + sidAidsM.size)
    println("sid=186010(周杰伦轨迹), aids=" + sidAidsM.get("186010"))
    val sidAidsMBroad = sc.broadcast(sidAidsM)
    val aidNameMBroad = sc.broadcast(aidNameM)

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


    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val existsRecedKeywordDataPaths = getExistsPath(cmd.getOptionValue("reced_keyword"), fs)
    val recedKeywordData = spark.sparkContext.textFile(existsRecedKeywordDataPaths.mkString(",")).map(line => {
      // keyword: recommendimpress	mainpage	android	129232837	  1520170808000	388455	video	  2	          bySort	null	      -1	      simTagv3-1447101-video	-1	      电视剧
      // guess  : recommendimpress	mainpage	android	1292328832	1520147571000	1428679	video	  1	          hot	    null	      -1	      null	                  -1	      猜你喜欢
      // format : action            page      os      uid         logTime       vid     vType   position    alg     netstatus   sourceId  sourceInfo              groupId   groupName
      // num    : 1                 2         3       4           5             6       7        8          9       10          11        12                      13        14
      val info = line.split("\t")
      val actionUserId = info(3)
      val videoId = info(5)
      var vType = info(6)
      vType = vType.replaceAll("eventactivity", "evtTopic")
      val idtype = videoId + "-" + vType
      var logTime = 0L
      var lastRecedFromNow = 0
      if (info.length >= 5) {
        logTime = info(4).toLong
        lastRecedFromNow = getRecedDayFromNow(NOW_TIME, logTime)
        if (lastRecedFromNow < 0)
          lastRecedFromNow *= -1
      }
      var keyword = ""
      if (info.length >= 14) {
        keyword = info(13)
      }
      (actionUserId, (keyword, idtype + ":" + lastRecedFromNow))
    }).groupByKey.map({case (key, values) =>
      val keywords = mutable.HashSet[String]()
      val kwdImpressedVideos = mutable.HashSet[String]()
      val guessImpressedVideos = mutable.HashSet[String]()
      for (value <- values) {
        val kwd = value._1
        val idtypeDay = value._2
        val lastRecedFromNow = idtypeDay.split(":")(1).toInt
        if (!kwd.isEmpty) {
          if (!kwd.equals("猜你喜欢")) {
            if (lastRecedFromNow <= KWD_REC_WINDOW)
              keywords.add(kwd)
            if (!idtypeDay.isEmpty)
              kwdImpressedVideos.add(idtypeDay)
          } else {
            if (!idtypeDay.isEmpty)
              guessImpressedVideos.add(idtypeDay)
          }
        }
      }
      if (key.equals("135571358")) {
        println("reced info from mainpage log 4 135571358: \nrecedKeywords:" + keywords.mkString(",") + "\nkwdImpressedVideos:" + kwdImpressedVideos.mkString(",") + "\nguessImpressedVideos:" + guessImpressedVideos.mkString(","))
      }
      (key, keywords.mkString(","), kwdImpressedVideos.mkString(","), guessImpressedVideos.mkString(","))
    }).toDF("uid", "recedKeywords", "impressedKwdVideos", "impressedGuessVideos")

    println("Load songPref")
    val uidSongPref = spark.read.textFile(cmd.getOptionValue("songPref"))
      .map(line => {
        // uid.toString  \t  subSongs.mkString(",")  \t  searchSongs.mkString(",")  \t  localSongs.mkString(",")
        val us = line.split("\t", 2)
        (us(0), us(1))
      }).toDF("uid", "songPref")

    println("Load user_sub_art")
    val uidPrefArt = spark.read.textFile(cmd.getOptionValue("user_sub_art"))
      .map(line => {
        // ArtistId  \t  UserId  \t  SubTime
        val ts = line.split("\t")
        if (ts.length >= 3) {
          (ts(1), ts(0))
        } else {
          (null, null)
        }
      }).filter(_._1 != null).rdd.groupByKey.map({ case (key, values) =>
      val aidsBuff = mutable.ArrayBuffer[String]()
      for (value <- values) {
        aidsBuff.append(value)
      }
      if (key.equals("135571358")) {
        println("pref arts:" +  aidsBuff.mkString(","))
      }
      (key, aidsBuff.mkString(","))
    }).toDF("uid", "prefAids")

    //TODO
    // val sidAidTagM = mutable.HashMap[String, String]()
    // val sidAidMBroad = sc.broadcast(sidAidM)
    //val aidNameTagM = mutable.HashMap[String, String]()
    // val aidNameTagMBroad = sc.broadcast(aidNameTagM)

    val rcmdData = uidSongPref
      .join(uidPrefArt, Seq("uid"), "outer")
      .join(recedVideoData, Seq("uid"), "left_outer")
      .join(recedKeywordData, Seq("uid"), "left_outer")
      .flatMap(row => {
        val uid = row.getAs[String]("uid")
//        val musicABTest = new ABTest(configText)
//        val groupName = musicABTest.abtestGroup(uid.toLong, abtestExpName)
//        if (!groupName.equals("t2") || uid.equals("135571358")) {

          // (1) 加载喜欢的歌曲
          // 从song中推测出的用户喜欢的artist：subSong、localSong、searchSong、subArtist
          val aidSidsM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
          //songid:type:aid:aname,... \t search \t local
          val songPref = row.getAs[String]("songPref")
          if (songPref != null) {
            val ss = songPref.split("\t", 3)
            appendAidSidsM(ss(0), aidSidsM, sidAidsMBroad.value) // sub
            if (ss.length >= 2) {
              appendAidSidsM(ss(1), aidSidsM, sidAidsMBroad.value) // search
            }
            if (ss.length >= 3) {
              appendAidSidsM(ss(2), aidSidsM, sidAidsMBroad.value) // local
            }
          }

          val aidsPref = row.getAs[String]("prefAids") // 收藏艺人
          if (aidsPref != null) {
            appendAidsM(aidsPref, aidSidsM)
          }
          // 按照song数量由多到少排序
          val aidSidsSorted = resortM(aidSidsM)
          //println("aidSidsSorted:" + aidSidsSorted)

          // (2) 加载推荐过数据
          val impressedKwdVideos = row.getAs[String]("impressedKwdVideos")
          val impressedGuessVideos = row.getAs[String]("impressedGuessVideos")
          val recedVideos = row.getAs[String]("recedVideos")
          val recedKeywords = row.getAs[String]("recedKeywords")
          // uid \t 周杰伦:aid:1362297:video:mArt:猜你喜欢,...  1362297:video:mArt:猜你喜欢,
          val recedAidOrKeywords = mutable.HashSet[String]()
          //val recedResroucetypeAidOrKeywordsM = mutable.HashMap[String, Int]()
          val recedResources = mutable.HashSet[String]()
          val recedVids = mutable.HashSet[String]()
          val recedSids = mutable.HashSet[String]()
          // TODO 去重关键词短一点， sid时间长一点
          getRecedKeyVideosFromLog(recedKeywords, impressedKwdVideos, impressedGuessVideos,
            recedVideos, recedAidOrKeywords, recedResources, recedVids, recedSids,
            vidSidAidMBroad.value)
          if (uid.equals("135571358")) {
            println("for 135571358")
            println("recedAidOrKeywords:" + recedAidOrKeywords)
            println("recedResources:" + recedResources)
            println("recedSids:" + recedSids)
            println("recedVids:" + recedVids)
          }

          val rcmds4ArtSongCands = mutable.ArrayBuffer[String]() // 推荐候选集
          //val rcmds4SimArtCands = mutable.ArrayBuffer[String]() // 推荐候选集
          if (uid.equals("135571358")) {
            println("aid for uid:135571358")
            println(aidSidsSorted.map(tup => tup._1 + ":" + tup._2.mkString(",")).mkString("\n"))
          }
          val asIter = aidSidsSorted.iterator
          val aidCntThred4aidsid = 30
          /*if (groupName.equals("t1") || uid.equals("135571358"))
            */recedVids ++= musicVideoSBroad.value
          if (uid.equals("135571358")) {
            println("recedVids size for uid:135571358:" + recedVids.size + ", musicVideoSBroad.value.size:" + musicVideoSBroad.value.size)
            println(aidSidsSorted.map(tup => tup._1 + ":" + tup._2.mkString(",")).mkString("\n"))
          }
          //val aidCntThred4simaid = 30
          while (asIter.hasNext) {
            // for ((aid, sids) <- aidSidsM) {
            val (aid, sids) = asIter.next()
            val aname = aidNameMBroad.value.getOrElse(aid, null)
            if (aname != null && !aname.equals("周杰伦") && !recedAidOrKeywords.contains(aid) && !recedAidOrKeywords.contains(aname)) {

              if (uid.equals("135571358")) {
                println("aname:" + aname)
              }

              // 关联音乐偏好 aid + sid
              if (rcmds4ArtSongCands.size <= aidCntThred4aidsid) {
                getRcmd(aid, aname, sids,
                  recedVids,
                  vidTitleMBroad.value,
                  aidsidVideosMBroad.value,
                  aidVideosMBroad.value,
                  rcmds4ArtSongCands, rcmds4ArtSongCands.length + 1) // 每次最多1个召回结果
              }


              /*if (groupName.equals("t2") || uid.equals("135571358")) {
                // 获取相似艺人相关推荐
                val (keyword, simaids, maxScore) = getKwordSimaids(aid, aidSidsM, artSimsMBroad.value, artTagsMBroad.value, recedAidOrKeywords)
                if (uid.equals("135571358")) {
                  println("kwd simaids 4 135571358:")
                  println("keyword:" + keyword)
                  println("simaids:" + simaids)
                  println("maxScore:" + maxScore)
                }
                if (simaids != null && simaids.size > 1) {
                  for (said <- simaids) {
                    if (!recedAidOrKeywords.contains(aid) && !recedAidOrKeywords.contains(aname) && rcmds4SimArtCands.size<= aidCntThred4simaid) {
                      getRcmd(said, keyword, sids,
                        recedVids,
                        vidTitleMBroad.value,
                        aidsidVideosMBroad.value,
                        aidVideosMBroad.value,
                        rcmds4SimArtCands, rcmds4SimArtCands.length + 1) // 每次最多1个召回结果
                    }
                  }
                }
              }*/
            }
          }
          if (uid.equals("135571358")) {
            println("rcmds4ArtSongCands:" + rcmds4ArtSongCands)
          }

          val result = mutable.ArrayBuffer[(String, String)]()


          if (!rcmds4ArtSongCands.isEmpty) {
            result.append(("A", uid + "\t" + rcmds4ArtSongCands.mkString(",")))
          }
          /*if (!rcmds4SimArtCands.isEmpty) {
            result.append(("S", uid + "\t" + rcmds4SimArtCands.mkString(",")))
          }*/
          result
//        } else {
//          mutable.ArrayBuffer[(String, String)]()
//        }
      })
      .filter(_ != null).filter(_ != "").filter(_ != None)
      .cache

    // 使用partitionBy的作用：为了改名字（使用part-*****这种类型的名字）
    val partitionNum = cmd.getOptionValue("partitionNum").toInt
    rcmdData.filter(_._1.equals("A")).map({case(stype,line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(partitionNum)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/artSong")
    //rcmdData.filter(_._1.equals("S")).map({case(stype,line) => (line.toString.split("\t",2)(0).toLong, line)}).rdd.partitionBy(new HashPartitioner(partitionNum)).map(_._2).saveAsTextFile(cmd.getOptionValue("output") + "/simart")

  }

  def isSongReced(idRelativeSids: mutable.ArrayBuffer[String], recedSids:mutable.HashSet[String]):Boolean = {
    var isSongRecedVideo = false
    if (idRelativeSids != null) {
      for (sid <- idRelativeSids) {
        if (recedSids.contains(sid))
          isSongRecedVideo = true
      }
    }
    isSongRecedVideo
  }

  def addRecedSong(idRelativeSids: mutable.ArrayBuffer[String], recedSids:mutable.HashSet[String]) = {

    if (idRelativeSids != null) {
      for (sid <- idRelativeSids) {
        recedSids.add(sid)
      }
    }
  }

  def addAidOrKwd(resourcetype:String, entities:Seq[String], recedResroucetypeAidOrKeywordsM: mutable.HashMap[String, Int]) = {

    for (entity <- entities) {
      val resourcetypeAidOrKwd = resourcetype + "-" + entity
      val cnt = recedResroucetypeAidOrKeywordsM.getOrElse(resourcetypeAidOrKwd, 0)
      recedResroucetypeAidOrKeywordsM.put(resourcetypeAidOrKwd, cnt + 1)
    }
  }

  def getRecedKeyVideos(recedKeywords: String, recedKeyVideos: String, recedGuessVideos: String,
                        recedVideos: String,
                        recedAidOrKeywords: mutable.HashSet[String],
                        recedResources: mutable.HashSet[String],  recedVids: mutable.HashSet[String],
                        recedSids: mutable.HashSet[String], vidSidAidM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String])],
                        ridSidAidM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String])]) = {
    // /周杰伦:1362297:video:mArt,...  1362297:video:mArt&-1362297-v,
    //val uvs = recedKeyVideos.split("\t")
    if (recedKeywords != null && !recedKeywords.isEmpty) {
      for (kw <- recedKeywords.split(",")) {
        recedAidOrKeywords.add(kw)
      }
    }
    if (recedKeyVideos != null) {
      for (keyVideostr <- recedKeyVideos.split(",")) {
        val keyVideos = keyVideostr.split(":")
        // 已推荐kw
        recedAidOrKeywords.add(keyVideos(0))
        if (keyVideos.length >= 3) {
          if (keyVideos(2).equals("video")) {
            // kw已推荐videos
            recedVids.add(keyVideos(1))
            // 获取vid关联的sids、aids
            val (sids, aids) = vidSidAidM.getOrElse(keyVideos(1), (null, null))
            if (sids != null) {
              for (sid <- sids) {
                recedSids.add(sid)
              }
            }
            if (aids != null) {
              for (aid <- aids) {
                if (!aid.isEmpty) {
                  recedAidOrKeywords.add(aid)
                }
              }
            }
          } else {
            val ridType = keyVideos(1) + "-" + keyVideos(2)
            recedResources.add(ridType)
            val (sids, aids) = ridSidAidM.getOrElse(ridType, (null, null))
            if (sids != null) {
              for (sid <- sids) {
                recedSids.add(sid)
              }
            }
            if (aids != null) {
              for (aid <- aids) {
                if (!aid.isEmpty) {
                  recedAidOrKeywords.add(aid)
                }
              }
            }
          }
        }
      }
    }
    if (recedGuessVideos != null) {
      for (vs <- recedGuessVideos.split(",")) {
        val idtypereason = vs.split(":")
        if (idtypereason.size >= 2) { // 猜你喜欢, 不考虑关键词
          // 1362297:video:mArt
          if (idtypereason(1).equals("video")) {
            recedVids.add(idtypereason(0))
          } else {
            val ridType = idtypereason(0) + "-" + idtypereason(1)
            recedResources.add(ridType)
          }
        }
      }
    }
    if (recedVideos != null) {
      for (vid <- recedVideos.split(",")) {
        recedVids.add(vid)
      }
    }
  }

  def getRecedKeyVideosFromLog(recedKeywords: String, impressedKwdVideos: String, impressedGuessVideos: String,
                               recedVideos: String, recedAidOrKeywords: mutable.HashSet[String], recedResources: mutable.HashSet[String],  recedVids: mutable.HashSet[String], recedSids: mutable.HashSet[String],
                               vidSidAidM: mutable.HashMap[String, (ArrayBuffer[String], ArrayBuffer[String])]) = {

    try {
      if (recedKeywords != null && !recedKeywords.isEmpty) {
        for (kw <- recedKeywords.split(",")) {
          recedAidOrKeywords.add(kw)
        }
      }
      if (impressedKwdVideos != null) {
        for (idTypestr <- impressedKwdVideos.split(",")) {
          // idtype : lastRecedFromNow , idtype : lastRecedFromNow , ...
          val idTypeDay = idTypestr.split("-|:")
          if (idTypeDay.length >= 3) {
            if (idTypeDay(1).equals("video")) {
              // kw已曝光videos
              recedVids.add(idTypeDay(0))
              if (idTypeDay(2).toInt <= SONGART_REC_WINDOW) {
                // 获取vid关联的sids、aids
                val (sids, aids) = vidSidAidM.getOrElse(idTypeDay(0), (null, null))
                if (sids != null) {
                  for (sid <- sids) {
                    recedSids.add(sid)
                  }
                }
                if (aids != null) {
                  for (aid <- aids) {
                    if (!aid.isEmpty) {
                      recedAidOrKeywords.add(aid)
                    }
                  }
                }
              }
            }
          }
        }
      }
      if (impressedGuessVideos != null) {
        for (idTypestr <- impressedGuessVideos.split(",")) {
          // idtype : lastRecedFromNow , idtype : lastRecedFromNow , ...
          val idTypeDay = idTypestr.split("-|:")
          if (idTypeDay.size >= 2) { // 猜你喜欢, 不考虑关键词
            if (idTypeDay(1).equals("video")) {
              recedVids.add(idTypeDay(0))
            } else {
              val ridType = idTypeDay(0) + "-" + idTypeDay(1)
              recedResources.add(ridType)
            }
          }
        }
      }
      if (recedVideos != null) {
        for (vid <- recedVideos.split(",")) {
          recedVids.add(vid)
        }
      }
    } catch {
      case ex:Exception => println("error in getRecedKeyVideosFromLog:" + ex.getMessage + "\nrecedKeywords:" + recedKeywords + "\nimpressedKwdVideos:" + impressedKwdVideos + "\nimpressedGuessVideos:" + impressedGuessVideos)
    }
  }


  def resortM(aidSidsM: mutable.HashMap[String, ArrayBuffer[String]]) = {
    //val sortedAidSids = mutable.ArrayBuffer[(String, ArrayBuffer[String])]()
    val aidSidsArray = aidSidsM.toArray[(String, mutable.ArrayBuffer[String])]
    val sortedAidSids = aidSidsArray.sortWith(_._2.length > _._2.length)
    sortedAidSids
  }

  def appendAidSidsM(str: String, aidSidsM: mutable.HashMap[String, ArrayBuffer[String]], sidAidsM: mutable.HashMap[String, String]) = {
    if (str != null && !str.isEmpty) {
      // sid:...,sid
      for (sidx <- str.split(",")) {
        if (!sidx.isEmpty) {
          val sid = sidx.split(":")(0)
          val aidstr = sidAidsM.getOrElse(sid, null)
          if (aidstr != null) {
            val aids = aidstr.split(",")
            for (aid <- aids) {
              val sids = aidSidsM.getOrElse(aid, mutable.ArrayBuffer[String]())
              if (!sids.contains(sid)) {
                sids.append(sid)
                aidSidsM.put(aid, sids)
              }
            }
          }
        }
      }
    }
  }

  def appendAidsM(aidstr: String, aidSidsM: mutable.HashMap[String, ArrayBuffer[String]]) = {
    if (aidstr != null) {
      for (aid <- aidstr.split(",")) {
        val sids = aidSidsM.getOrElse(aid, mutable.ArrayBuffer[String]())
        if (!sids.contains("0")) {
          sids.append("0") // 以0替代搜藏艺人
          aidSidsM.put(aid, sids)
        }
      }
    }
  }

  def getVideoPredM(keywordsInput: String) = {
    val videoPredM = mutable.HashMap[String, Float]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(keywordsInput)
    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = reader.readLine()
    while (line != null) {
      //video pred
      val ts = line.split("\t")
      if (ts.length >= 2) {
        videoPredM.put(ts(0), ts(1).toFloat)
      }
      line = reader.readLine()
    }
    reader.close()
    videoPredM
  }



  def getVideoInfoM(inputDir: String, sidVideosM: mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]],
                    aidVideosM: mutable.HashMap[String, mutable.ArrayBuffer[(String, Float)]],
                    videoPredM: mutable.HashMap[String, Float],
                    vidSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                    aidsidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]]/*,
                    musicVideoS: Set[String]*/) = {
    val vidTitleM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getVideoInfoMFromFile(bufferedReader, vidTitleM, sidVideosM, aidVideosM, videoPredM, vidSidAidM, aidsidVideosM/*, musicVideoS*/)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getVideoInfoMFromFile(bufferedReader, vidTitleM, sidVideosM, aidVideosM, videoPredM, vidSidAidM, aidsidVideosM/*, musicVideoS*/)
      bufferedReader.close()
    }
    vidTitleM
  }

  def isValid(coverSize: String) = {
    var valid = false
    if (coverSize != null) {
      val wh = coverSize.split("\\*")
      if (wh.length >= 2) {
        val rate = wh(0).toDouble/wh(1).toDouble
        if (wh(0).toInt < 540 || wh(1).toInt < 300 ||  rate < 1.7 || rate > 1.8) {
          valid = false
        } else {
          valid = true
        }
      }
    }
    valid
  }

  def getVideoInfoMFromFile(reader: BufferedReader, vidTitleM: mutable.HashMap[String, String],
                            sidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]], aidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]],
                            videoPredM: mutable.HashMap[String, Float],
                            vidSidAidM: mutable.HashMap[String, (mutable.ArrayBuffer[String], mutable.ArrayBuffer[String])],
                            aidsidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]]/*,
                            musicVideoS: Set[String]*/) = {
    // var line: String = null
    val curtime: Long = new Date().getTime
    var line = reader.readLine()
    while (line != null) {
      //      try {
      implicit val formats = DefaultFormats
      val json = parse(line)
      val videoId = (json \ "videoId").extractOrElse[String]("")
      val songIds = parseIds((json \ "bgmIds").extractOrElse[String](""))
      val artistIds = parseIds((json \ "artistIds").extractOrElse[String](""))
      val auditTagIds = parseIds((json \ "auditTagIds").extractOrElse[String](""))
      val category = (json \ "category").extractOrElse[String]("").replace(",", "_")
      var title = (json \ "title").extractOrElse[String]("推荐视频")
      title = title.replaceAll(",", "，").replaceAll(":", "：").replaceAll("\r", "").replaceAll("\n", "").replaceAll("《", "<").replaceAll("》", ">")
      // TODO 白名单， minFLow, 分辨率
      println("vid:" + videoId);
      var validV = true
      /*if (!musicVideoS.contains(videoId))
        validV = false*/

      if (auditTagIds.contains("37028") || artistIds.contains("6452")) // 周杰伦相关tag过滤
        validV = false

      if (validV == true && (json \ "extData") != null) {
        // "extData":"{\"updateCover\":null,\"smallFlow\":false,\"expose\":true,\"coverSize\":\"1920*1080\",\"videoSize\":\"1280*720\"}
        val extDataStr = (json \ "extData").extractOrElse[String](null)
        val status = (json \ "status").extractOrElse[Int](0)
        val expTime = (json \ "expireTime").extractOrElse[Long](0)
        //println("extDataStr:" + extDataStr)

        //val extData = parse(extDataStr.substring(8, extDataStr.length - 1)) // TODO 去掉JString(...)
        if (extDataStr != null) {
          val extData = parse(extDataStr)
          //println("extData:" + extData)
          // 过滤掉cover、videoSize不符合要求的视频
          //println("status:" + status)
          //println("curtime:" + curtime)
          if (1 == status && extData != null && expTime > curtime) {
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
                if (validV == true) {
                  val coverWatermark = (extData \ "coverWatermark").extractOrElse[Boolean](true)  // 默认值为true，没有检测过的都默认有水印
                  validV = !coverWatermark
                  if (validV == false) {
                    println("filt by coverWatermark:" + coverWatermark)
                  }
                } else {
                  println("filt by videoSize:" + videoSize)
                }
              } else {
                println("filt by coverSize:" + coverSize)
              }
            }
          } else {
            validV = false
            //println("extData:" + extData)
            //println("curtime:" + curtime)
            println("status, extData, time = " + (1==status) + "," + (extData != null) + "," + (expTime > curtime))

          }
        } else {
          validV = false
        }
      }

      if (validV == true) {
        vidTitleM.put(videoId, title)
        /* var fsid = ""
         var faid = ""*/
        if (songIds != null) {
          for (sid <- songIds) {
            if (!sid.isEmpty) {
              val videoPrds = sidVideosM.getOrElse(sid, mutable.ArrayBuffer[(String, Float)]())
              sidVideosM.put(sid, videoPrds)
              videoPrds.append((videoId, videoPredM.getOrElse(videoId, 0F)))
              val sidAidPair = vidSidAidM.getOrElse(videoId, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
              vidSidAidM.put(videoId, sidAidPair)
              sidAidPair._1.append(sid)
              //fsid = sid
            }
          }
        }

        if (artistIds != null) {
          for (aid <- artistIds) {
            if (!aid.isEmpty) {
              var videosPrds = aidVideosM.getOrElse(aid, null)
              if (videosPrds == null) {
                videosPrds = mutable.ArrayBuffer[(String, Float)]()
                aidVideosM.put(aid, videosPrds)
              }
              videosPrds.append((videoId, videoPredM.getOrElse(videoId, 0F)))
              val sidAidPair = vidSidAidM.getOrElse(videoId, (mutable.ArrayBuffer[String](), mutable.ArrayBuffer[String]()))
              vidSidAidM.put(videoId, sidAidPair)
              sidAidPair._2.append(aid)
              //faid = aid
            }
          }
        }
        /*if (!fsid.isEmpty && !faid.isEmpty) {
          aidsidVidM.put(faid + ":" + fsid, videoId)
        }*/
        if (songIds != null && artistIds != null) {
          for (sid <- songIds; aid <- artistIds) {
            val aidsid = aid + ":" + sid
            var videosPrds = aidsidVideosM.getOrElse(aidsid, null)
            if (videosPrds == null) {
              videosPrds = mutable.ArrayBuffer[(String, Float)]()
              aidsidVideosM.put(aidsid, videosPrds)
            }
            videosPrds.append((videoId, videoPredM.getOrElse(videoId, 0F)))

            //aidsidVideosM.put(aid + ":" + sid, videoId)
          }
        }
      } else {
        println("Invalid video:" + line)
      }

      line = reader.readLine()
    }

    // 排序
    for (sid <- sidVideosM.keys.toArray[String]) {
      val vidPrds = sidVideosM.getOrElse(sid, null)
      sidVideosM.put(sid, vidPrds.sortWith(_._2 > _._2))
    }
    for (aid <- aidVideosM.keys.toArray[String]) {
      val vidPrds = aidVideosM.getOrElse(aid, null)
      aidVideosM.put(aid, vidPrds.sortWith(_._2 > _._2))
    }
    for (aidsid <- aidsidVideosM.keys.toArray[String]) {
      val vidPrds = aidsidVideosM.getOrElse(aidsid, null)
      aidsidVideosM.put(aidsid, vidPrds.sortWith(_._2 > _._2))
    }
  }

  def getSongArtInfoM(inputDir: String, sidAidsM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String],
                      aidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]], sidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]]) = {

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

  def getSongArtInfoMFromFile(reader: BufferedReader, sidAidM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String],
                              aidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]], sidVideosM: mutable.HashMap[String, ArrayBuffer[(String, Float)]]) = {
    var line: String = reader.readLine()
    while (line != null) {
      // 59879	1873:阿宝	10846:张冬玲
      if (line != null) {
        val ts = line.split("\t", 2)
        var sid = ts(0)
        if (sid.startsWith("-")) {
          sid = sid.substring(1)
        }
        /*
        var sidLong = ts(0).toLong
        if (sidLong < 0) sidLong *= -1
        val sid = sidLong.toString
        */
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
            sidAidM.put(sid, aids.mkString(","))
          }
        }
      }
      line = reader.readLine()
    }
  }

  def parseIds(str: String) = {
    val res = mutable.ArrayBuffer[String]()
    if (!str.isEmpty && !str.equalsIgnoreCase("null")) {
      res.appendAll(str.substring(1, str.length-1).split(","))
    }
    res
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

  def trim(title: String, stopwords: mutable.HashSet[Char]) = {
    val sb = new mutable.StringBuilder()
    for (c <- title.toCharArray) {
      if (!stopwords.contains(c)) {
        sb.append(c)
      }
    }
    sb.toString
  }

  def removeInvalid(vidTitleM: mutable.HashMap[String, String], stopwords: mutable.HashSet[Char], vidPredM: mutable.HashMap[String, Float]) = {
    val rmSet = mutable.HashSet[String]()
    val trimTitlesM = mutable.HashMap[String, String]()
    for ((vid, title) <- vidTitleM) {
      val trimTitle = trim(title, stopwords)
      if (!trimTitlesM.contains(trimTitle)) {
        trimTitlesM.put(trimTitle, vid)
      } else {
        println("dup title:" + vid + " -vs- " + trimTitlesM(trimTitle) + "->" + trimTitle)
        val prd1 = vidPredM.getOrElse(vid, 0F)
        val prd2 = vidPredM.getOrElse(trimTitlesM(trimTitle), 0F)
        if (prd1 <= prd2) {
          rmSet.add(vid)
          println("rm:" + vid)
        } else {
          rmSet.add(trimTitlesM(trimTitle))
          trimTitlesM.put(trimTitle, vid)
          println("rm:" + trimTitlesM(trimTitle))
        }
      }
    }
    println("rmSet size:" + rmSet.size)
    for (rmid <- rmSet) {
      vidTitleM.remove(rmid)
    }
  }


  def getStopwords(input: String) = {
    val stopwords = new mutable.HashSet[Char]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(input)

    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = reader.readLine()
    while (line != null) {
      // word
      if (!line.isEmpty) {
        stopwords.add(line.charAt(0))
      }
      line = reader.readLine()
    }
    reader.close()
    stopwords
  }


  def getArtTagsM(artTagsInput: String) = {
    val uidTagsM = new mutable.HashMap[String, Seq[String]]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(artTagsInput)

    var reader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    var line: String = reader.readLine()
    while (line != null) {
      // aid \t name \t tag,tag
      if (!line.isEmpty) {
        val ts = line.split("\t", 3)
        if (ts.length >= 3) {
          uidTagsM.put(ts(0), ts(2).split(","))
        }
      }
      line = reader.readLine()
    }
    reader.close()
    uidTagsM
  }

  def getArtSimsM(pathInput: Path, artSimsM:  mutable.HashMap[String, mutable.LinkedHashSet[String]]) = {

    val hdfs: FileSystem = FileSystem.get(new Configuration())
    // val path = new Path(artSimsInput)
    if (hdfs.isDirectory(pathInput)) {
      for (status <- hdfs.listStatus(pathInput) ) {
        val fpath = status.getPath
        getArtSimsMF(fpath, artSimsM)
      }
    } else {
      getArtSimsMF(pathInput, artSimsM)
      //uidSimsM
    }

  }

  def getArtSimsMF(fpath: Path, artSimsM: mutable.HashMap[String, mutable.LinkedHashSet[String]]) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    var reader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
    var line: String = reader.readLine()
    while (line != null) {
      // aid \t aid:score,aid:score
      if (!line.isEmpty) {
        val ts = line.split("\t", 2)
        if (ts.length >= 2) {
          val idScores = ts(1).split(",")
          val simaids = mutable.LinkedHashSet[String]()
          for (ids <- idScores) {
            val idScore = ids.split(":")
            simaids.add(idScore(0))
          }
          artSimsM.put(ts(0), simaids)
        }
      }
      line = reader.readLine()
    }
    reader.close()
  }

  def getKwordSimaids(aid: String
                      ,aidSidsM: mutable.HashMap[String, ArrayBuffer[String]]
                      ,artSimsM: mutable.HashMap[String, mutable.LinkedHashSet[String]]
                      ,artTagsM: mutable.HashMap[String, Seq[String]]
                      //,kwdTagM: Map[String, String]
                      ,recedAidOrKeyword: mutable.HashSet[String]) = {
    val res = mutable.ArrayBuffer[String]()
    res.append(aid)
    //println("aid:" + aid)
    val tags = artTagsM.getOrElse(aid, null)
    //println("tags:" + tags)
    val tagSaidScoreM = mutable.HashMap[String, mutable.LinkedHashMap[String, Int]]()
    if (tags != null) {
      val sims = artSimsM.getOrElse(aid, null)
      //println("sims:" + sims)
      if (sims != null) {
        for (said <- sims) {
          val stags = artTagsM.getOrElse(said, null)
          //println("stags:" + stags)
          if (stags != null) {
            //for (stag <- stags) {
            var stag = stags(0) // 优先用mv tag
            if (tags.contains(stag)) { // 有相同tag
              //println("Match:" + stag)
              val saidScoreM = tagSaidScoreM.getOrElse(stag, mutable.LinkedHashMap[String, Int]()) // 保持有先后顺序
              tagSaidScoreM.put(stag, saidScoreM)
              saidScoreM.put(said, saidScoreM.getOrElse(said, 0) + 1)
              if (aidSidsM.contains(said)) {  // 命中根据songid推测的用户artist偏好
                saidScoreM.put(said, saidScoreM.getOrElse(said, 0) + 1)
              }
            }
            //}
          }
        }
      }
      var maxTag = ""
      var maxSaidNum = 0;
      for ((tag, saidScoreM) <- tagSaidScoreM) { // 选择行为最多，相似度最高的
        if (saidScoreM.size > maxSaidNum) {
          maxTag = tag
          maxSaidNum = saidScoreM.size
        }
      }
      var maxScore = 1
      if (!maxTag.isEmpty) {
        for ((said, score) <- tagSaidScoreM(maxTag).toArray[(String, Int)].sortWith(_._2 > _._2)) {
          //println(said + "->" + score)
          res.append(said)
          if (score > maxScore) {
            maxScore = score
          }
        }

      }
      //println("res:" + res)
      /*if (kwdTagM.contains(maxTag))
        maxTag = kwdTagM(maxTag)*/
      (maxTag, res, maxScore)
    } else {
      ("", null, 1)
    }
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
