package com.netease.music.recommend.scala.feedflow.tag

import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{collect_list, _}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 合并偏好, song + mv + art
  */
object RcmdMvByMusicPref {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options()

    options.addOption("user_song_from_playlist", true, "user_song_from_playlist")
    options.addOption("searchProfile_days", true, "searchProfile_days")
    options.addOption("user_local_song", true, "user_local_song")

    options.addOption("Music_Song_artists", true, "music_song_input")
    options.addOption("Music_MVSubscribe", true, "Music_MVSubscribe")

    //options.addOption("Music_Follow", true, "Music_Follow")
    options.addOption("Music_UserArtistSubscribe", true, "Music_UserArtistSubscribe")
    //options.addOption("Music_RegisteredArtist", true, "Music_RegisteredArtist")
    options.addOption("Music_MVMeta_allfield", true, "Music_MVMeta_allfield")

    options.addOption("userVideoPref", true, "userVideoPref")

    options.addOption("musicSimMvsInput", true, "musicSimMvsInput")

    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val mvInput = cmd.getOptionValue("Music_MVMeta_allfield")
    val mvIdAidM = getinfoRromMV(mvInput)
    val mvIdAidMBroad = sc.broadcast(mvIdAidM)
    println("mvIdAidMBroad size:" + mvIdAidMBroad.value.size)

    val userVideoPrefs = cmd.getOptionValues("userVideoPref")
    val userMVprefData = spark.read.parquet(userVideoPrefs:_*)
      .filter($"vType" === "mv")
      .select("videoId", "actionUserId")
      .groupBy("actionUserId").agg(collect_list($"videoId").as("rced_mvid_list"))
      .withColumn("uid", toLong($"actionUserId"))
      .select("uid", "rced_mvid_list")
    println("userMVprefData sample...")
    userMVprefData.show(2, false)

    val subArtistData = spark.read.textFile(cmd.getOptionValue("Music_UserArtistSubscribe"))
      .map(line => {
        // ArtistId,UserId,SubTime
        val ts = line.split("\t")
        val aid = ts(0)
        val userId = ts(1)
        (userId.toLong, aid)
      }).toDF("uid", "aid")

    // 选取有足够粉丝的大号
    /*
    val validFriendData = subArtistData
      .groupBy($"aid")
      .agg(count($"uid").as("userCnt"))
      // .filter($"userCnt" > 200)
      .select("aid", "userCnt")
    val allFriendFansCntM = getM(validFriendData.collect())
    val allFriendFansCntMBroad = sc.broadcast(allFriendFansCntM)
    println("allFriendFansCntM size:" + allFriendFansCntM.size)
    */
    val userPrefAidsData = subArtistData.groupBy("uid")
      .agg(collect_list($"aid").as("validAids"))
      //.withColumn("validAids", getValidAids(allFriendFansCntMBroad.value)($"friends"))
     // .filter($"validAids".isNotNull)
    println("userPrefAidsData count:" + userPrefAidsData.count())
    userPrefAidsData.show(2, false)

    // 目前用历史数据
    val userSongFromPlaylistData = spark.read.textFile(cmd.getOptionValue("user_song_from_playlist"))
      .map(line => {
        // uid soingid.songid,...
        val ts = line.split("\t")
        val samples = mutable.ArrayBuffer[String]()
        val random = new Random()
        var uid = ts(0).toLong
        for (sid <- ts(1).split(",")) {
          if (samples.length < 1000) {
            samples.append(sid)
          } else {
            //println("songids > 500:" + uid.toString)
            samples.remove(random.nextInt(900), 100) // 随机删除100
          }
        }
        if (uid < 0) {
          uid = -uid
        }
        (uid, samples)
      }).toDF("uid", "songidsPlaylist").repartition(1000)
    //println("userSongFromPlaylistData cnt:" + userSongFromPlaylistData.count())
    //userSongFromPlaylistData.show(10, false)

    println("Load user search song data...")
    val userSearchSongData = spark.read.textFile(cmd.getOptionValue("searchProfile_days"))
      .map(line => {
        val uidsongids = mutable.ArrayBuffer[Tuple2[Long, Long]]()
        var uid = 0L
        // 100001799	371362:4.0,400689263:4.0,512359367:1.0
        val ts = line.split("\t")
        if (ts.length >= 2) {
          uid = ts(0).toLong
          if (uid < 0) {
            uid = -uid
          }
        }
        val random = new Random()
        val samples = mutable.ArrayBuffer[String]()
        for (idwt <- ts(1).split(",")) {
          if (samples.length < 1000) {
            samples.append(idwt.split(":")(0))
          } else {
            //println("songids > 500:" + uid.toString)
            samples.remove(random.nextInt(900), 100) // 随机删除100
          }
        }
        (uid, samples)
      }).toDF("uid", "songidsSearchList")

    val userLocalSongData = spark.read.textFile(cmd.getOptionValue("user_local_song"))
      .map(line => {
        // uid soingid type
        val ts = line.split("\t")
        var uid = 0L
        var songid = 0L
        if (ts.length >= 3) {
          uid = ts(0).toLong
          songid = ts(1).toLong
          if (uid < 0) {
            uid = -uid
          }
          if (songid < 0) {
            songid = -songid
          }
        }
        (uid, songid.toString)
      }).toDF("uid", "songidsLocal")
      .groupBy($"uid").agg(
      collect_list("songidsLocal").as("songidsLocalList")
    ).select("uid", "songidsLocalList")

    println("歌曲艺人数据...")
    val songArtInput = cmd.getOptionValue("Music_Song_artists")
    val sidAidsM = getSongArtInfoM(songArtInput)
    println("sidAidsM size:" + sidAidsM.size)
    println(sidAidsM.head)
    val sidAidsMBroad = sc.broadcast(sidAidsM)

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
        (uid, vmid, vtype)
      }).toDF("uid", "mvid", "vtype")
      .filter($"vtype" === 0) // mv
      .groupBy("uid").agg(collect_list($"mvid").as("mvid_list"))

    val (songid2vdieoM, artid2videoM, mvid2videoM) = getSimsM(cmd.getOptionValue("musicSimMvsInput"))
    println("songid2vdieoM size:" + songid2vdieoM.size)
    println("artid2videoM size:" + artid2videoM.size)
    println("mvid2videoM size:" + mvid2videoM.size)
    val songid2vdieoMBroad = sc.broadcast(songid2vdieoM)
    println(songid2vdieoM.head)
    val artid2videoMBroad = sc.broadcast(artid2videoM)
    val mvid2videoMBroad = sc.broadcast(mvid2videoM)

    val rcmdData = userMVPref
      .join(userSongFromPlaylistData, Seq("uid"), "outer") // 只保留有MV偏好用户
      .join(userSearchSongData, Seq("uid"), "outer")
      .join(userLocalSongData, Seq("uid"), "outer")
      .join(userPrefAidsData, Seq("uid"), "outer")
      .join(userMVprefData, Seq("uid"), "left")
      .flatMap(row => {
        val uid = row.getAs[Long]("uid")
        val songidsLocalList = row.getAs[mutable.WrappedArray[String]]("songidsLocalList")
        val songidsSearchList = row.getAs[mutable.WrappedArray[String]]("songidsSearchList")
        val songidsPlaylist = row.getAs[mutable.WrappedArray[String]]("songidsPlaylist")
        val mvidlist = row.getAs[Seq[String]]("mvid_list")
        val prefAidList = row.getAs[Seq[String]]("validAids")
        val songWtM = mutable.HashMap[String, Double]()
        val artWtM = mutable.HashMap[String, Double]()
        val mvWtM = mutable.HashMap[String, Double]()

        // 已推荐
        val rcedMvSet = mutable.HashSet[String]()
        val rcedMvList = row.getAs[Seq[Long]]("rced_mvid_list")
        if (rcedMvList != null) {
          for (mvid <- rcedMvList) {
            rcedMvSet.add(mvid.toString)
          }
        }
        if (mvidlist != null) {
          for (mvid <- mvidlist) {
            rcedMvSet.add(mvid)
          }
        }
        if (uid == 359792224) {
          println(rcedMvSet)
        }

        appendAll(songWtM, songidsLocalList, 2.0)
        if (uid == 359792224) {
          println("for 359792224 arts(local):" )
          println(songWtM)
        }
        appendAll(songWtM, songidsSearchList, 2.0)
        if (uid == 359792224) {
          println("for 359792224 arts(append search):" )
          println(songWtM)
          println(songidsSearchList)
        }
        appendAll(songWtM, songidsPlaylist, 1.0)
        if (uid == 359792224) {
          println("for 359792224 arts(append playlist):" )
          println(songWtM)
          println(songidsSearchList)
        }
        appendArt(artWtM, songWtM, sidAidsMBroad.value, 1)
        if (uid == 359792224) {
          println("for 359792224 arts(1):" )
          println(artWtM)
        }
        appendArt2(artWtM, prefAidList, 3)
        if (uid == 359792224) {
          println("for 359792224 arts(2):" )
          println(prefAidList)
          println(artWtM)
        }
        appendArt3(artWtM, mvidlist, mvIdAidMBroad.value, 1)
        if (uid == 359792224) {
          println("for 359792224 arts(3):" )
          println(artWtM)
        }
        filtLowArt(artWtM, 3) // 删除置信度低的艺人
        if (uid == 359792224) {
          println("for 359792224 arts(filt):" )
          println(artWtM)
        }

        appendAll(mvWtM, mvidlist, 2)

        if (uid == 359792224) {
          println("for 359792224 mv:" )
          println(mvWtM)
        }

        val rcmdBySong = rcmdByMusicPref(songWtM, songid2vdieoMBroad.value, 20, 2, rcedMvSet, "-song")
        val rcmdByArt = rcmdByMusicPref(artWtM, artid2videoMBroad.value, 20, 3, rcedMvSet, "-aritst")
        val rcmdByMv = rcmdByMusicPref(mvWtM, mvid2videoMBroad.value, 20, 3, rcedMvSet, "-mv")

        val res = mutable.ArrayBuffer[(String, Int)]()
        if (rcmdBySong != null) {
          res.append((uid + "\t" + rcmdBySong, 1))
        }
        if (rcmdByArt != null) {
          res.append((uid + "\t" + rcmdByArt, 2))
        }
        if (rcmdByMv != null) {
          res.append((uid + "\t" + rcmdByMv, 3))
        }
        res
        //(uid, songWtM, artWtM, mvWtM)
      }).toDF("rcmd_data", "type")
    println("rcmd data sample...")
    rcmdData.show(2, false)
    rcmdData.filter($"type" === 1).select("rcmd_data").repartition(50).write.text(cmd.getOptionValue("output") + "/s2mv")
    rcmdData.filter($"type" === 2).select("rcmd_data").repartition(50).write.text(cmd.getOptionValue("output") + "/a2mv")
    rcmdData.filter($"type" === 3).select("rcmd_data").repartition(50).write.text(cmd.getOptionValue("output") + "/m2mv")

  }

  def getEventVideoM(lines: Array[String]) = {
    val eventVideoM = mutable.HashMap[String, String]()
    for (line <- lines) {
      val ts = line.split("\01", -1)
      val event = ts(1)
      val video = ts(3)
      eventVideoM.put(event, video)
    }
    eventVideoM
  }

  def appendAll(itemMap: mutable.HashMap[String, Double], prefItems: Seq[String], pref: Double) = {
    if (prefItems != null && prefItems != None) {
      for (item <- prefItems) {
        if (item != null) {
          itemMap.put(item, itemMap.getOrElse(item, 0.0) + pref)
        }
      }
    }
  }

  def joinIdType = udf((vid: String, vtype: String) => {
    vid + "-" + vtype
  })

  def toLong = udf((idstr: String) => {
    idstr.toLong
  })

  def getItemIdfWtM(rows: Array[Row], allUserCnt: Long) = {
    val itemWtM = mutable.HashMap[String, Double]()
    for (row <- rows) {
      itemWtM.put(row.getAs[String]("idtype"), Math.log( allUserCnt / (row.getAs[Long]("userCnt") + 1.0)) )
    }
    itemWtM
  }

  def updateWtM(uItemWtM: scala.collection.immutable.Map[String, Double], itemWtM: mutable.HashMap[String, Double]) = {
    val wtM = mutable.HashMap[String, Double]()
    for ((key, oWt) <- uItemWtM) {
      val wt = itemWtM.getOrElse(key, -1.0)
      if (wt != -1.0) {
        wtM.put(key, oWt * wt)
      }
    }
    wtM
  }

  def getSongArtInfoM(inputDir: String) = {
    val sidAidsM = mutable.HashMap[String, String]()
    // getSongArtInfoM(inputDir: String, sidAidM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String])= {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        println("load fpath:" + fpath)
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getSongArtInfoMFromFile(bufferedReader, sidAidsM)
          bufferedReader.close()
        }
        println("current sidAidsM size:" + sidAidsM.size)
      }
    } else {
      println("load path:" + path)
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getSongArtInfoMFromFile(bufferedReader, sidAidsM)
      bufferedReader.close()
    }
    sidAidsM
  }

  def getSongArtInfoMFromFile(reader: BufferedReader, sidAidM: mutable.HashMap[String, String]) = {
    var line: String = reader.readLine()
    while (line != null) {
      // 59879	1873:阿宝	10846:张冬玲
      if (line != null) {
        val ts = line.split("\t", 2)
        var sid = ts(0)
        if (sid.startsWith("-")) {
          sid = sid.substring(1)
        }
        if (!sid.isEmpty) {
          val aids = mutable.ArrayBuffer[String]()
          if (ts.length >= 2) {
            for (aidnameStr <- ts(1).split("\t")) {
              val aidname = aidnameStr.split(":", 2)
              if (aidname.length >= 2) {
                val aid = aidname(0)
                if (!aid.isEmpty) {
                  aids.append(aid)
                }
              }
            }
          }
          if (aids.length > 0) { // video或者resource里面有这首歌
            sidAidM.put(sid, aids.mkString(","))
          }
        }
      }
      line = reader.readLine()
    }
  }

  def getVidArtsM(rows: Array[Row]) = {
    val vidAidsM = mutable.HashMap[String, String]()
    for (row <- rows) {
      val vid = row.getAs[Long]("videoId").toString
      val aids = row.getAs[String]("artistIds")
      vidAidsM.put(vid + "-video", aids)
    }
    vidAidsM
  }
  def getIds = udf((ids: String) => {
    if (ids == null || ids.length <= 2 || ids.equalsIgnoreCase("null"))
      "0"
    else
      ids.substring(1, ids.length-1)
  })

  def appendArt(artWtM: mutable.HashMap[String, Double], itemWtM: mutable.HashMap[String, Double], itemArtsM: mutable.HashMap[String, String], i: Int) = {
    for (item <- itemWtM.keys) {
      val arts = itemArtsM.getOrElse(item, null)
      if (arts != null) {
        for (art <- arts.split(",")) {
          val arttype = art
          val lstWt = artWtM.getOrElse(arttype, 0.0)
          artWtM.put(arttype, lstWt + i)
        }
      }
    }
  }

  def appendArt2(artWtM: mutable.HashMap[String, Double], aidList: Seq[String], i: Int) = {
    if (aidList != null) {
      for (aid <- aidList) {
        val arttype = aid
        val lstWt = artWtM.getOrElse(arttype, 0.0)
        artWtM.put(arttype, lstWt + i)
      }
    }
  }
  def appendArt3(artWtM: mutable.HashMap[String, Double], mvidTypeList: Seq[String], mvidAidsM:mutable.HashMap[String, mutable.ArrayBuffer[String]],  i: Int) = {
    if (mvidTypeList != null) {
      for (mvidType <- mvidTypeList) {
        val mvid = mvidType.split("-")(0)
        val aids = mvidAidsM.getOrElse(mvid, mutable.ArrayBuffer[String]())
        for (aid <- aids) {
          val arttype = aid
          val lstWt = artWtM.getOrElse(arttype, 0.0)
          artWtM.put(arttype, lstWt + i)
        }
      }
    }
  }

  def getM(validFriendData: Array[Row]) = {
    val idCntM = mutable.HashMap[String, Int]()
    for( row <- validFriendData) {
      val friendId = row.getAs[String]("aid")
      val cnt = row.getAs[Long]("userCnt")
      idCntM.put(friendId, cnt.toInt)
    }
    idCntM
  }

  def getValidAids(allFriendFansCntM: mutable.HashMap[String, Int]) = udf((aids:Seq[String]) => {

    val result = mutable.ArrayBuffer[String]()
    for (aid <- aids) {
      if (allFriendFansCntM.contains(aid)) {
        result.append(aid)
      }
    }
    if (result.isEmpty) {
      null
    } else {
      result
    }
  })

  // def getmvArtsM(inputDir: String, aidNameM: mutable.HashMap[String, String])= {
  def getinfoRromMV(inputDir: String) = {
    val mvidAidM = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getmvArtsMFromFile(bufferedReader, mvidAidM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getmvArtsMFromFile(bufferedReader, mvidAidM)
      bufferedReader.close()
    }
    mvidAidM
  }

  def getmvArtsMFromFile(reader: BufferedReader, mvidAidsM: mutable.HashMap[String, mutable.ArrayBuffer[String]]) = {
    var line: String = reader.readLine()

    while (line != null) {
      // ID,Name,Artists,Tags,MVDesc,Valid,AuthId,Status,ArType,MVStype,Subtitle,Caption,Area,Type,SubType,Neteaseonly,Upban,Plays,Weekplays,Dayplays,Monthplays,Mottos,Oneword,Appword,Stars,Duration,Resolution,FileSize,Score,PubTime,PublishTime,Online,ModifyTime,ModifyUser,TopWeeks,SrcFrom,SrcUplod,AppTitle,Subscribe,transName,aliaName,alias,fee
      // println("LINE:" + line)
      if (line != null) {
        val ts = line.split("\01")
        if (ts != null && ts.length >= 3) {
          val mvid = ts(0)
          val artids = parseArts(ts(2))
          mvidAidsM.put(mvid, artids)
        }
      }
      line = reader.readLine()
    }
  }

  def parseArts(str: String): ArrayBuffer[String] = {
    val res = mutable.ArrayBuffer[String]()
    var artstr = str.trim
    if (!artstr.isEmpty && !artstr.equals("NULL")) {
      val arts = artstr.split("\t")
      for (art <- arts) {
        val artid = art.split(":")(0).trim
        if (!artid.equals("0")) {
          res.append(artid)
        }
      }
    }
    res
  }

  def filtLowArt(artWtM: mutable.HashMap[String, Double], i: Int) = {
    val rmSet = mutable.ArrayBuffer[String]()
    for ((aid, wt) <- artWtM) {
      if (wt < i) {
        rmSet.append(aid)
      }
    }
    for (rmid <- rmSet) {
      artWtM.remove(rmid)
    }
  }

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
        } else if (wt < 0.5) {
          maxSimNum = 5;
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

  def rcmdByMusicPref(prefWtM: mutable.HashMap[String, Double], resSimsM: mutable.HashMap[String, ArrayBuffer[String]], recallNum: Int, maxRecallPerRes: Int, recedMvSet: mutable.HashSet[String], suffix: String) = {
    val rcmdReasonsM = mutable.HashMap[String, mutable.HashSet[String]]()
    val random = new Random()
    for ( (res, wt) <- prefWtM.toArray[(String, Double)].sortWith(_._2 > _._2) if rcmdReasonsM.size < recallNum) {
      if (random.nextInt(2) == 0) { // 1/2随机
        val sims = resSimsM.getOrElse(res, null)
        if (sims != null) {
          val lstSize = rcmdReasonsM.size
          for (sim <- sims if rcmdReasonsM.size < (lstSize+maxRecallPerRes)) {
            val idtype = sim.split("-")
            if (!recedMvSet.contains(idtype(0))) {
              val newIdtype = idtype(0) + ":" + idtype(1)
              val reasons = rcmdReasonsM.getOrElse(newIdtype, mutable.HashSet[String]())
              reasons.add(res + suffix)
              rcmdReasonsM.put(newIdtype, reasons)
            }
          }
        }
      }
    }

    val rcmds = new StringBuilder()
    for ((idtype, reasons) <- rcmdReasonsM) {
      rcmds.append(idtype).append(":").append(reasons.mkString("&")).append(",")
    }
    if (rcmds.size > 0) {
      rcmds.substring(0, rcmds.length-1)
    } else {
      null
    }
  }


}