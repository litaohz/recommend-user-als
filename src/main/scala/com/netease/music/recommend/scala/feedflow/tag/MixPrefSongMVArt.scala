package com.netease.music.recommend.scala.feedflow.tag

import java.io.{BufferedReader, InputStreamReader}

import com.netease.music.recommend.scala.feedflow.tag.MixPrefJoinArtIdf.{appendAll, joinIdType, toLong}
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
object MixPrefSongMVArt {

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
          val arttype = art + "-art"
          val lstWt = artWtM.getOrElse(arttype, 0.0)
          artWtM.put(arttype, lstWt + i)
        }
      }
    }
  }

  def appendArt2(artWtM: mutable.HashMap[String, Double], aidList: Seq[String], i: Int) = {
    if (aidList != null) {
      for (aid <- aidList) {
        val arttype = aid + "-art"
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
          val arttype = aid + "-art"
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
    options.addOption("user_pref_video", true, "user_pref_video")

    //options.addOption("Music_Follow", true, "Music_Follow")
    options.addOption("Music_UserArtistSubscribe", true, "Music_UserArtistSubscribe")
    //options.addOption("Music_RegisteredArtist", true, "Music_RegisteredArtist")
    options.addOption("Music_MVMeta_allfield", true, "Music_MVMeta_allfield")

    options.addOption("output", true, "output")

    import spark.implicits._
    val parser = new PosixParser()
    val cmd = parser.parse(options, args)

    val userVidPref = spark.read.parquet(cmd.getOptionValue("user_pref_video"))
      .filter($"videoPref" > 7 || $"subscribeCnt"> 0 || $"shareClickCnt" > 0 || $"zanCnt" > 0)
      .withColumn("vidtype", joinIdType($"videoId", $"vType"))
      .withColumn("uid", toLong($"actionUserId"))
      .groupBy($"uid").agg(
      collect_list("vidtype").as("vidtypeList")
    ).select("uid", "vidtypeList")

    val mvInput = cmd.getOptionValue("Music_MVMeta_allfield")
    val mvIdAidM = getinfoRromMV(mvInput)
    val mvIdAidMBroad = sc.broadcast(mvIdAidM)
    println("mvIdAidMBroad size:" + mvIdAidMBroad.value.size)
    /*
    // 云音乐福利, 网易UFO丁磊, 云音乐小秘书, 云音乐曲库, 云音乐视频酱
    val filteredUsers = mutable.HashSet[String]("2082221", "48353", "9003", "2608402", "1305093793", "1")
    println("filteredUsers size:" + filteredUsers.size)
    val filteredUsersBroad = sc.broadcast(filteredUsers)

    val followDataPath = cmd.getOptionValue("Music_Follow")
    println("followDataPath:" + followDataPath)
    val followData = spark.read.textFile(followDataPath).map(line => {
      // ID,UserId,FriendId,CreateTime,Mutual
      val ts = line.split("\t")
      val userId = ts(1).toLong
      val friendId = ts(2)
      if (filteredUsersBroad.value.contains(friendId) /*|| !pubuidsBroad.value.contains(friendId)*/) {
        (userId, "0")
      } else {
        (userId, friendId)
      }
    }).filter(!_._2.equals("0"))
      .toDF("uid", "fuid")
    */

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
        if (vtype == 0) {
          vmid = vmid + "-mv"
        } else if (vtype == 1) {
          vmid = vmid + "-video"
        }
        (uid, vmid, vtype)
      }).toDF("uid", "mvid", "vtype")
      .filter($"vtype" === 0)
      .groupBy("uid").agg(collect_list($"mvid").as("mvid_list"))

    val prefData = userMVPref
      .join(userVidPref, Seq("uid"))
      .join(userSongFromPlaylistData, Seq("uid"), "left") // 只保留有MV偏好用户
      .join(userSearchSongData, Seq("uid"), "left")
      .join(userLocalSongData, Seq("uid"), "left")
      .join(userPrefAidsData, Seq("uid"), "left")
      .map(row => {
        val uid = row.getAs[Long]("uid")
        val songidsLocalList = row.getAs[mutable.WrappedArray[String]]("songidsLocalList")
        val songidsSearchList = row.getAs[mutable.WrappedArray[String]]("songidsSearchList")
        val songidsPlaylist = row.getAs[mutable.WrappedArray[String]]("songidsPlaylist")
        val mvidlist = row.getAs[Seq[String]]("mvid_list")
        val prefAidList = row.getAs[Seq[String]]("validAids")
        val vidtypeList = row.getAs[mutable.WrappedArray[String]]("vidtypeList")

        val songWtM = mutable.HashMap[String, Double]()
        val artWtM = mutable.HashMap[String, Double]()
        val mvWtM = mutable.HashMap[String, Double]()

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
        appendAll(mvWtM, vidtypeList, 2.0)

        if (uid == 359792224) {
          println("for 359792224 mv:" )
          println(mvWtM)
        }
        (uid, songWtM, artWtM, mvWtM, mvWtM.size)
      }).toDF("uid", "songWtM", "artWtM", "mvWtM", "mvSize")
      .filter($"mvSize" >= 2)
      .repartition(1000)

    //prefData.write.parquet(cmd.getOptionValue("output") + "/prefData")

    val allUserCnt = prefData.count()
    println("allUserCnt:" + allUserCnt)

    val itemCntData = prefData.flatMap(row => {
      val itemCnts = mutable.ArrayBuffer[(String, Int)]()

      val songWtM = row.getAs[scala.collection.immutable.Map[String, Double]]("songWtM")
      if (songWtM != null) {
        for (song <- songWtM.keys) {
          itemCnts.append((song, 1))
        }
      }
      val artWtM = row.getAs[scala.collection.immutable.Map[String, Double]]("artWtM")
      if (artWtM != null) {
        for (art <- artWtM.keys) {
          itemCnts.append((art, 1))
        }
      }
      val mvWtM = row.getAs[scala.collection.immutable.Map[String, Double]]("mvWtM")
      if (mvWtM != null) {
        for (mvid <- mvWtM.keys) {
          itemCnts.append((mvid, 1))
        }
      }
      itemCnts
    }).toDF("idtype", "ux")
      .groupBy($"idtype")
      .agg(count($"ux").as("userCnt"))
      .filter($"userCnt" > 100) // TODO
      .repartition(200)

    itemCntData.write.parquet(cmd.getOptionValue("output") + "/itemCntX")

    if (itemCntData == null) {
      println("Debug error ....")
    }
    println("求item权重...")
    val itemWtM = getItemIdfWtM(itemCntData.collect(), allUserCnt)
    println("itemWtM size:" + itemWtM.size)
    val itemWtMBroad = sc.broadcast(itemWtM)

    val joinData = prefData.flatMap(row => {
      val uid = row.getAs[Long]("uid")

      val songImWtM = row.getAs[scala.collection.immutable.Map[String, Double]]("songWtM")
      val songWtM = updateWtM(songImWtM, itemWtMBroad.value)
      val artImWtM = row.getAs[scala.collection.immutable.Map[String, Double]]("artWtM")
      val artWtM = updateWtM(artImWtM, itemWtMBroad.value)
      val mvImWtM = row.getAs[scala.collection.immutable.Map[String, Double]]("mvWtM")
      val mvWtM = updateWtM(mvImWtM, itemWtMBroad.value)

      val sortedSongWtList = songWtM.toArray[(String, Double)].sortWith(_._2 > _._2)
      val sortedArtWtList = artWtM.toArray[(String, Double)].sortWith(_._2 > _._2)
      val sortedMvWtList = mvWtM.toArray[(String, Double)].sortWith(_._2 > _._2)

      val result = mutable.ArrayBuffer[(String, String, Double)]()

      val uidStr = uid.toString

      var targetNumForSong = 200
      if (200 > sortedSongWtList.size * 2 / 3) {
        targetNumForSong = result.size + sortedSongWtList.size * 2 / 3 + 1
      }
      val sortedSongIter = sortedSongWtList.iterator
      while (sortedSongIter.hasNext && result.size < targetNumForSong) {
        val (item, wt) = sortedSongIter.next()
        result.append((uidStr, item, wt))
      }

      var targetNumForMV = result.size + 200
      if (200 > sortedMvWtList.size * 2 / 3) {
        targetNumForMV = result.size + sortedMvWtList.size * 2 / 3 + 1
      }
      val sortedMvIter = sortedMvWtList.iterator
      while (sortedMvIter.hasNext && result.size < targetNumForMV) {
        val (item, wt) = sortedMvIter.next()
        result.append((uidStr, item, wt))
      }

      var targetNumForArt = result.size + 50
      if (50 > sortedArtWtList.size * 2 / 3) {
        targetNumForArt = result.size + sortedArtWtList.size * 2 / 3 + 1
      }
      val sortedArtIter = sortedArtWtList.iterator
      while (sortedArtIter.hasNext && result.size < targetNumForArt) {
        val (item, wt) = sortedArtIter.next()
        result.append((uidStr, item, wt))
      }

      if (uid == 359792224L) {
        println("uid=359792224L")
        for ((uidStr, item, wt) <- result) {
          println(uidStr + ", " + item + ", " + wt)
        }
      } else if (uid == 477399796L) {
        println("uid=477399796L")
        for ((uidStr, item, wt) <- result) {
          println(uidStr + ", " + item + ", " + wt)
        }
      }
      result

    }).toDF("userIdStr", "idtype", "rating")

    val userIdMapping = joinData.select($"userIdStr").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("userIdStr"), line._2.toInt)
    }).toDF("userIdStr", "userId").cache()
    //userIdMapping.write.parquet(cmd.getOptionValue("output") + "/userIdMapping")

    val itemMapping = joinData.select($"idtype").rdd.distinct().zipWithUniqueId().map(line =>{
      (line._1.getAs[String]("idtype"), line._2.toInt)
    }).toDF("idtype", "itemId").cache()
    //itemMapping.write.parquet(cmd.getOptionValue("output") + "/itemMapping")

    val userItemRating = joinData
      .join(userIdMapping, Seq("userIdStr"), "left")
      .join(itemMapping, Seq("idtype"), "left")
       // .select("userId", "itemId", "rating")
      .filter($"itemId".isNotNull && $"userId".isNotNull && $"rating".isNotNull)

    userItemRating.repartition(1000).write.parquet(cmd.getOptionValue("output") + "/data")



  }

}