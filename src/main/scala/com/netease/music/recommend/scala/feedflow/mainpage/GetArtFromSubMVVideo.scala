package com.netease.music.recommend.scala.feedflow.mainpage

import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 根据用户搜藏的mv，video获取用户对艺人偏好
  * Created by hzlvqiang on 2017/12/20.
  */
object GetArtFromSubMVVideo {



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val sc = new SparkContext(conf);
    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate

    val options = new Options
    options.addOption("Music_MVMeta_allfield", true, "mv_input")
    options.addOption("Music_VideoRcmdMeta", true, "video_input")
    options.addOption("Music_MVSubscribe_ALL", true, "sub_mv_video_input")
    options.addOption("Music_Song_artists", true, "Music_Song_artists")
    options.addOption("output", true, "output directory")

    val parser = new PosixParser
    val cmd = parser.parse(options, args)

    import spark.implicits._

    println("从songartist读取artName")
    val songArtInput = cmd.getOptionValue("Music_Song_artists")
    println("songArtInput:" + songArtInput)
    val aidNameM = getAidNameM(songArtInput)
    println("aid=6452, name=" + aidNameM.get("6452"))

    println("读取MV META...")
    val mvInput = cmd.getOptionValue("Music_MVMeta_allfield")
    println("mvInput:" + mvInput)
    val mvidArtsM = getmvArtsM(mvInput, aidNameM)
    println("mvid=20, arts=" + mvidArtsM.getOrElse("20", null))
    println("mvidArtsM size:" + mvidArtsM.size)
    val mvidArtsMBroad = sc.broadcast(mvidArtsM)

    println("读取VIDEO META...")
    val videoInput = cmd.getOptionValue("Music_VideoRcmdMeta")
    println("videoInput:" + videoInput)
    val vidArtsM = getVideoArtsM(videoInput, aidNameM)
    println("vid=9018, arts=" + vidArtsM.getOrElse("9018", null))
    println("video size:" + vidArtsM.size)
    val vidArtsMBroad = sc.broadcast(vidArtsM)

    println("根据收藏mv, video获取偏好艺人...")
    val subData = spark.read.textFile(cmd.getOptionValue("Music_MVSubscribe_ALL"))
        .rdd.map(line => {
        var key = 0L
        var value = mutable.ArrayBuffer[String]()
        // UserId,id,SubTime,type
        val ts = line.split("\t")
        if (ts.length >= 4) {
          val uid = ts(0)
          val id = ts(1)
          val vType = ts(3)
          if (vType.equals("0")) { // MV
            val artType = mvidArtsMBroad.value.getOrElse(id, null)
            if (artType != null) {
              //(uid, (arrayToString(artType._1), arrayToString(artType._2), ""))
              key = uid.toLong
              value.append(id + "-mv:" + artType) // arttype:artid:artanme
              value.append("")
            }
          } else if (vType.equals("1")) { // VIDEO
            val artCat = vidArtsMBroad.value.getOrElse(id, null)
            if (artCat != null) {
              //(uid, (arrayToString(artCat._1), "", artCat._2))
              key = uid.toLong
              //value = (arrayToString(artCat._1), "", artCat._2)
              value.append("")
              value.append(id + "-video:"  + artCat)// cat:artid:artanme
            }
          }
        } else {
          (0, null)
        }
        if (key < 0) {
          key = -key
        }
        (key, value)
      }).filter(_._1 != 0).groupByKey().map({case (key, values) =>
        val mvInfos = mutable.ArrayBuffer[String]()
        val videoInfos = mutable.ArrayBuffer[String]()
        for (value: mutable.ArrayBuffer[String] <- values) {
          if (!value(0).isEmpty) {
            mvInfos.append(value(0))
          } else if(!value(1).isEmpty) {
            videoInfos.append(value(1))
          }
        }

        key + "\t" + mvInfos.mkString(",") + "\t" + videoInfos.mkString(",")
    }).toDS()
    val output = cmd.getOptionValue("output")
    println("OUTPUT:" + output)
    subData.write.text(output)
    // subData.write.text(output)

  }

  def map2String(keyValueM: mutable.HashMap[String, Int]): String = {
    val res = new mutable.StringBuilder()
    for ((k, v) <- keyValueM) {
      res.append(k).append(":").append(v).append(",")
    }
    if (res.length > 0) {
      res.substring(0, res.length-1)
    } else {
      ""
    }
  }


  def arrayToString(array: mutable.ArrayBuffer[String]) = {
    val res = mutable.StringBuilder
    if (array != null && !array.isEmpty) {
      array.mkString(",")
    } else {
      ""
    }
  }

  def getAidNameM(inputDir: String) = {
    val aidNameM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(path) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        getArtNameMFromFile(bufferedReader, aidNameM)
        bufferedReader.close()
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getArtNameMFromFile(bufferedReader, aidNameM)
      bufferedReader.close()
    }
    aidNameM
  }
  def getArtNameMFromFile(bufferedReader: BufferedReader, aidNameM: mutable.HashMap[String, String]) = {
    var line: String = bufferedReader.readLine()
    while (line != null) {
      // -525196726	74494:Rumer	0:Paul Pesco
      val ts = line.split("\t")
      for (i <- 1 until ts.length) {
        val aidName = ts(i).split(":", 2)
        if (aidName.length >= 2) {
          aidNameM.put(aidName(0), aidName(1))
        }
      }
      line = bufferedReader.readLine()
    }
  }

  def getVideoArtsM(inputDir: String, aidNameM: mutable.HashMap[String, String]) = {
    val videoArtsM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(path) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        getvideoArtsMFromFile(bufferedReader, videoArtsM, aidNameM)
        bufferedReader.close()
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getvideoArtsMFromFile(bufferedReader, videoArtsM, aidNameM)
      bufferedReader.close()
    }
    videoArtsM
  }

  def getvideoArtsMFromFile(reader: BufferedReader, videoArtsM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String]) = {
    var line: String = null
    try {
      while ((line = reader.readLine()) != null) {
        // json format
        implicit val formats = DefaultFormats
        val json = parse(line)
        val videoId = (json \ "videoId").extractOrElse[String]("")
        //println("videoId:" + videoId)
        val artistIds = parseIds((json \ "artistIds").extractOrElse[String](""))
        //println("artsIds:" + artistIds)
        val category = (json \ "category").extractOrElse[String]("").replace(",", "-")
        var artStr = ""
        if (artistIds != null) {
          for (i <- 0 until artistIds.length) {
            val aname = aidNameM.getOrElse(artistIds(i), "")
            artistIds(i) = artistIds(i) + ":" + aname
          }
          artStr = artistIds.mkString("&&")
        }
        videoArtsM.put(videoId, category + ":" + artStr)
      }
    }catch {
      case ex: Exception => println("io exception" + ex)
    }
  }

  def parseIds(str: String) = {
    val res = mutable.ArrayBuffer[String]()
    if (!str.isEmpty && !str.equalsIgnoreCase("null")) {
      res.appendAll(str.substring(1, str.length-1).split(","))
    }
    res
  }

  def getmvArtsM(inputDir: String, aidNameM: mutable.HashMap[String, String])= {
    val mvArtsM = new mutable.HashMap[String, String]()
    val hdfs: FileSystem = FileSystem.get(new Configuration())
    val path = new Path(inputDir)
    if (hdfs.isDirectory(path)) {
      for (status <- hdfs.listStatus(new Path(inputDir)) ) {
        val fpath = status.getPath
        var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(fpath)))
        if (bufferedReader != null) {
          getmvArtsMFromFile(bufferedReader, mvArtsM, aidNameM)
          bufferedReader.close()
        }
      }
    } else {
      var bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(path)))
      getmvArtsMFromFile(bufferedReader, mvArtsM, aidNameM)
      bufferedReader.close()
    }
    mvArtsM
  }

  def getmvArtsMFromFile(reader: BufferedReader, mvArtsM: mutable.HashMap[String, String], aidNameM: mutable.HashMap[String, String]) = {
    var line: String = reader.readLine()

    while (line != null) {
      // ID,Name,Artists,Tags,MVDesc,Valid,AuthId,Status,ArType,MVStype,Subtitle,Caption,Area,Type,SubType,Neteaseonly,Upban,Plays,Weekplays,Dayplays,Monthplays,Mottos,Oneword,Appword,Stars,Duration,Resolution,FileSize,Score,PubTime,PublishTime,Online,ModifyTime,ModifyUser,TopWeeks,SrcFrom,SrcUplod,AppTitle,Subscribe,transName,aliaName,alias,fee
     // println("LINE:" + line)
      if (line != null) {
        val ts = line.split("\01")
        if (ts != null && ts.length >= 3) {
          val mvid = ts(0)
          //println("mvid:" + mvid)
          val artids = parseArts(ts(2))
          //println("artids:" + ts(2))
          val arttags = mutable.ArrayBuffer[String]()
          if (ts.length >= 13) {
            val area = parseTags(ts(12))
            val mvtype = parseTags(ts(9))
            val artype = parseTags(ts(8))
            arttags.appendAll(comb(area, mvtype, artype))
            // println("area:" + ts(12))
          }
          var arttagStr = ""
          if (!arttags.isEmpty) {
            arttagStr = arttags.mkString("&&")
          }
          val anameBuf = new StringBuilder()
          for (aid <- artids) {
            val aname = aidNameM.getOrElse(aid, "")
            anameBuf.append(aid).append(":").append(aname).append(",")
          }
          var aidNameStr = ""
          if (anameBuf.size > 0) {
            aidNameStr = anameBuf.substring(0, anameBuf.size - 1)
          }
          mvArtsM.put(mvid, arttagStr + ":" + aidNameStr)
        }
      }
      line = reader.readLine()
    }
  }

  def comb(areas: Array[String], mvtypes: Array[String], artypes: Array[String]) = {
    val res = mutable.ArrayBuffer[String]()
    for (area <- areas) {
      for (mvtype <- mvtypes) {
        for (artype <- artypes) {
          val ama = area + mvtype + artype
          if (!ama.isEmpty) {
            res.append(ama)
          }
        }
      }
    }
    res
  }

  def parseTags(strin: String) = {
    val str = strin.trim
    str.split(";")
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
}
