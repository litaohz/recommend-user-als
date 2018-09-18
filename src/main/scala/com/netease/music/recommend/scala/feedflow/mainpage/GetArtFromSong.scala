package com.netease.music.recommend.scala.feedflow.mainpage

import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

/**
  * 根据用户收藏、搜索、本地歌曲偏好， 获取用户对艺人偏好
  * Created by hzlvqiang on 2017/12/20.
  */
object GetArtFromSong {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf);
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("user_sub_songs_type", true, "sub_song_input")
    options.addOption("searchProfile_days", true, "search_input")
    options.addOption("user_local_song", true, "local_song_input")

    options.addOption("Music_Song_artists", true, "song_artist")
    options.addOption("songtag", true, "songtag")

    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    val songArtInput = cmd.getOptionValue("Music_Song_artists")
    println("songArtInput:" + songArtInput)
    val sidAidsM = mutable.HashMap[String, String]()
    val aidNameM = mutable.HashMap[String, String]()
    getSongArtInfoM(songArtInput, sidAidsM, aidNameM)
    println("sid=59879, aids=" + sidAidsM.get("59879"))
    println("aid=10846, name=" + aidNameM.get("10846"))

    println("sidAidM size:" + sidAidsM.size)
    println("aidNameM size:" + aidNameM.size)

    println("load songtag...")
    val songTagInput = cmd.getOptionValue("songtag")
    appendSongTag(songTagInput, sidAidsM)

    val sidAidsMBroad = sc.broadcast(sidAidsM)
    val aidNameMBroad = sc.broadcast(aidNameM)

    val userSubSongData = spark.read.textFile(cmd.getOptionValue("user_sub_songs_type"))
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
      }).rdd.groupByKey().map({ case (key, values) =>
      val random = new Random()
      val songids = mutable.ArrayBuffer[String]()
      for (value <- values) {
        if (songids.length < 500) {
          songids.append(value)
        } else {
          println("songids > 500:" + key.toString)
          songids.remove(random.nextInt(400), 100)// 随机删除100
        }
      }
      (key, songids)
    }).toDF("uid", "songids1")



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
          /*
          for (idscore <- ts(1).split(",")) {
            var songid = idscore.split(":")(0).toLong
            if (songid < 0) {
              songid = -songid
            }
            uidsongids.append((uid, songid))
          }*/
        }
        val random = new Random()
        val samples = mutable.ArrayBuffer[String]()
        for (idwt <- ts(1).split(",")) {
          if (samples.length < 500) {
            samples.append(idwt.split(":")(0))
          } else {
            println("songids > 500:" + uid.toString)
            samples.remove(random.nextInt(400), 100) // 随机删除100
          }
        }

        (uid, samples)
      }).toDF("uid", "songids2")


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
      }).rdd.groupByKey().map({ case (key, values) =>
        val random = new Random()
        val songids = mutable.ArrayBuffer[String]()
        for (value <- values) {
          if (songids.length < 500) {
            songids.append(value)
          } else {
            println("songids > 500:" + key.toString)
            songids.remove(random.nextInt(400), 100)// 随机删除100
          }
        }
      (key, songids)
      }).toDF("uid", "songids3")


    println("Join uid-songid 数据...")
    val userSongArtData = userSubSongData.join(userSearchSongData, Seq("uid"), "outer").join(userLocalSongData, Seq("uid"), "outer")
      .map(row => {
        val res = mutable.ArrayBuffer[Tuple2[Long, Long]]()
        val uid = row.getAs[Long]("uid")
        var subSongs = row.getAs[mutable.Seq[String]]("songids1")
        if (subSongs == null) {subSongs = mutable.ArrayBuffer[String]()}
        for (i <- 0 until subSongs.length) {
          val aidsTag = sidAidsMBroad.value.getOrElse(subSongs(i), "").split(":") // aid,aid:tag
          var stag = ""
          if (aidsTag.length >= 2) {
            stag = aidsTag(1)
          }
          val aidNames = mutable.ArrayBuffer[String]()
          for (aid <- aidsTag(0).split(",")) {
            val aname = aidNameMBroad.value.getOrElse(aid, "")
            aidNames.append(aid + ":" + aname)
          }
          // songid:songtag:aid:aname&&aid:aname
          subSongs(i) = subSongs(i) + ":" + stag + ":" + aidNames.mkString("&&")
        }
        var searchSongs = row.getAs[mutable.Seq[String]]("songids2")
        if (searchSongs == null) {searchSongs =  mutable.ArrayBuffer[String]()}
        // id,id
        for (i <- 0 until searchSongs.length) {
          val aidsTag = sidAidsMBroad.value.getOrElse(searchSongs(i), "").split(":") // aid,aid:tag
          var stag = ""
          if (aidsTag.length >= 2) {
            stag = aidsTag(1)
          }
          val aidNames = mutable.ArrayBuffer[String]()
          for (aid <- aidsTag(0).split(",")) {
            val aname = aidNameMBroad.value.getOrElse(aid, "")
            aidNames.append(aid + ":" + aname)
          }
          // songid:songtag:aid:aname&&aid:aname
          searchSongs(i) = searchSongs(i) + ":" + stag + ":" + aidNames.mkString("&&")
        }

        var localSongs = row.getAs[mutable.Seq[String]]("songids3")
        if (localSongs == null) {localSongs =  mutable.ArrayBuffer[String]()}
        // sid,sid
        for (i <- 0 until localSongs.length) {
          val aidsTag = sidAidsMBroad.value.getOrElse(localSongs(i), "").split(":") // aid,aid:tag
          var stag = ""
          if (aidsTag.length >= 2) {
            stag = aidsTag(1)
          }
          val aidNames = mutable.ArrayBuffer[String]()
          for (aid <- aidsTag(0).split(",")) {
            val aname = aidNameMBroad.value.getOrElse(aid, "")
            aidNames.append(aid + ":" + aname)
          }
          // songid:songtag:aid:aname&&aid:aname
          localSongs(i) = localSongs(i) + ":" + stag + ":" + aidNames.mkString("&&")
        }

        uid.toString + "\t" + subSongs.mkString(",") + "\t" + searchSongs.mkString(",") + "\t" + localSongs.mkString(",")

      })
    println("userSubSongData showcase")
    userSongArtData.show(10, true)
    userSongArtData.write.text(cmd.getOptionValue("output"))
  }


  def getSongArtInfoM(inputDir: String, sidAidM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String])= {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getSongArtInfoMFromFile(bufferedReader, sidAidM, aidNameM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getSongArtInfoMFromFile(bufferedReader, sidAidM, aidNameM)
      bufferedReader.close()
    }
  }

  def getSongArtInfoMFromFile(reader: BufferedReader, sidAidM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String]) = {
    var line: String = reader.readLine()
    while (line != null) {
      // 59879	1873:阿宝	10846:张冬玲
      if (line != null) {
        val ts = line.split("\t", 2)
        val sid = ts(0)
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
                  if (!aidNameM.contains(aid) && !aname.isEmpty) {
                    aidNameM.put(aid, aname)
                  }
                }
              }
            }
          }
          if (aids.length > 0) {
            sidAidM.put(sid, aids.mkString(","))
          }
        }
      }
      line = reader.readLine()
    }
  }

  def appendSongTag(inputDir: String, sidAidsM: mutable.HashMap[String, String]) = {
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          appendSongTagFromFile(bufferedReader, sidAidsM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      appendSongTagFromFile(bufferedReader, sidAidsM)
      bufferedReader.close()
    }
  }

  def appendSongTagFromFile(reader: BufferedReader, sidAidsM: mutable.HashMap[String, String]) = {
    var line: String = reader.readLine()
    while (line != null) {
      //101243	民谣_7	1118	3
      val ts = line.split("\t")
      val songid = ts(0)
      val styleLang = ts(1).split("_")
      var langStyle = styleLang(0)
      if (styleLang(1).equals("7")) {
        langStyle = "中文" + langStyle
      } else if (styleLang(1).equals("8")) {
        langStyle = "日语" + langStyle
      } else if (styleLang(1).equals("16")) {
        langStyle = "韩语" + langStyle
      } else if (styleLang(1).equals(96)) {
        langStyle = "欧美" + langStyle
      }
      val aids = sidAidsM.getOrElse(songid, "")
      sidAidsM.put(songid, aids + ":" + langStyle)
      line = reader.readLine()
    }
  }

}
